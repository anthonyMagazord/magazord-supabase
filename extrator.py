"""
extrator.py — Magazord Digital Commerce (Supabase)
====================================================
Estratégia em duas fases para evitar timeout:

  FASE 1 — Salva todos os leads no Supabase (~30 min)
  FASE 2 — Atualiza campos personalizados em lotes (~3-4h)

Modos:
  python extrator.py --full      # fase 1 + fase 2
  python extrator.py --leads     # só fase 1 (leads)
  python extrator.py --custom    # só fase 2 (campos personalizados)
  python extrator.py             # delta (horário)

Secrets:
  EXACT_TOKEN   — token da API do Exact Sales
  SUPABASE_URL  — URL de conexão do Supabase
"""

import os, ast, sys, time, math
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────
#  CONFIGURAÇÕES
# ─────────────────────────────────────────────
EXACT_TOKEN   = os.environ.get("EXACT_TOKEN", "")
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
EXACT_BASE    = "https://api.exactspotter.com/v3"
ARQUIVO_REGIOES = "Regiões de atuação - Página1.csv"

PAGE_SIZE   = 500
DELAY_REQ   = 0.2
MAX_RETRIES = 3
DELTA_HORAS = 3
CHUNK_DB    = 500   # registros por insert

MAPA_FUNIS = {
    "6007": "Funil Principal", "10317": "Parcerias Hunter",
    "19653": "Summit", "16730": "Parcerias Farmer", "10268": "CI",
}

# Mapeamento por campo id (coluna 'id' da API = nome do campo personalizado)
CAMPOS_CUSTOM = {
    "_persona":                    "modelo_negocio_crm",
    "_faturamentomediomensal":     "faturamento_mensal",
    "_porte":                      "porte_empresa",
    "_sitedolead":                 "plataforma_ecommerce",
    "_utm_source":                 "utm_source",
    "_utm_medium":                 "utm_medium",
    "_utm_term":                   "utm_term",
    "_utm_content":                "utm_content",
    "_utm_campaign":               "utm_campaign",
    "_mobydick":                   "moby_dick_estrategico",
    "_evento":                     "eventos_participou",
    "_atualizadoporia":            "atualizado_por_ia",
    "_situacaoparamigracao":       "situacao_migracao",
}

UF_MAP = {
    "Acre": "AC", "Alagoas": "AL", "Amapá": "AP", "Amazonas": "AM",
    "Bahia": "BA", "Ceará": "CE", "Distrito Federal": "DF",
    "Espírito Santo": "ES", "Goiás": "GO", "Maranhão": "MA",
    "Mato Grosso": "MT", "Mato Grosso do Sul": "MS", "Minas Gerais": "MG",
    "Pará": "PA", "Paraíba": "PB", "Paraná": "PR", "Pernambuco": "PE",
    "Piauí": "PI", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS", "Rondônia": "RO", "Roraima": "RR",
    "Santa Catarina": "SC", "São Paulo": "SP", "Sergipe": "SE",
    "Tocantins": "TO",
}

B2B_MERCADOS  = ["Materiais de construção","Iluminação","Eletrônicos",
                  "Eletrodomésticos","Instrumentos Musicais","Alimentos","Bebidas"]
D2C_MERCADOS  = ["Moda","Moda Infantil","Calçados","Bolsas","Acessórios de moda",
                  "Cosméticos","Produtos de beleza","Suplementos Alimentares"]
HOME_MERCADOS = ["Artigos para casa","Cama, Mesa e Banho","Decoração",
                  "Móveis","Petshop","Brinquedos e Colecionáveis"]


# ─────────────────────────────────────────────
#  BANCO
# ─────────────────────────────────────────────
def get_engine():
    url = SUPABASE_URL.replace(":5432/", ":5432/") \
        if "pooler.supabase.com" in SUPABASE_URL \
        else SUPABASE_URL
    return create_engine(url, pool_pre_ping=True, connect_args={"sslmode": "require"})


def criar_tabela(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS leads (
                id                  TEXT PRIMARY KEY,
                lead                TEXT,
                cnpj                TEXT,
                website             TEXT,
                data_cadastro       TIMESTAMP,
                cidade              TEXT,
                estado              TEXT,
                uf                  TEXT,
                regiao              TEXT,
                etapa               TEXT,
                funil_nome          TEXT,
                origem              TEXT,
                mercado             TEXT,
                usuario             TEXT,
                modelo_negocio      TEXT,
                modelo_negocio_crm      TEXT,
                faturamento_mensal      TEXT,
                porte_empresa           TEXT,
                plataforma_ecommerce    TEXT,
                utm_source              TEXT,
                utm_medium              TEXT,
                utm_term                TEXT,
                utm_content             TEXT,
                utm_campaign            TEXT,
                moby_dick_estrategico   TEXT,
                eventos_participou      TEXT,
                atualizado_por_ia       TEXT,
                situacao_migracao       TEXT,
                atualizado_em           TIMESTAMP DEFAULT NOW()
            )
        """))
    print("   ✅ Tabela 'leads' verificada/criada")


def upsert_db(engine, df: pd.DataFrame):
    if df.empty:
        return

    colunas = df.columns.tolist()
    col_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in colunas if c != "id"])
    sql = text(f"""
        INSERT INTO leads ({", ".join(colunas)})
        VALUES ({", ".join([f":{c}" for c in colunas])})
        ON CONFLICT (id) DO UPDATE SET {col_set}, atualizado_em = NOW()
    """)

    total, inseridos = len(df), 0
    with engine.begin() as conn:
        for i in range(0, total, CHUNK_DB):
            chunk = df.iloc[i:i + CHUNK_DB]
            records = [
                {k: (None if (str(type(v).__name__) == "NaTType" or
                              (isinstance(v, float) and math.isnan(v))) else v)
                 for k, v in row.items()}
                for row in chunk.to_dict("records")
            ]
            conn.execute(sql, records)
            inseridos += len(chunk)
            print(f"   💾 {inseridos:,}/{total:,}...", end="\r")
    print(f"   ✅ {total:,} registros salvos          ")


# ─────────────────────────────────────────────
#  HELPERS — HTTP
# ─────────────────────────────────────────────
def _headers():
    return {"token_exact": EXACT_TOKEN, "Content-Type": "application/json"}


def _get_paginado(endpoint: str, filtro: str = "") -> list:
    url, registros, skip = f"{EXACT_BASE}/{endpoint}", [], 0
    while True:
        params = {"$top": PAGE_SIZE, "$skip": skip}
        if filtro:
            params["$filter"] = filtro
        # Remover parâmetros vazios
        params = {k: v for k, v in params.items() if v is not None and v != ""}
        for t in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=_headers(), params=params, timeout=180)
                if resp.status_code == 200:
                    data   = resp.json()
                    pagina = (data if isinstance(data, list)
                              else data.get("value") or data.get("items") or data.get("data") or [])
                    registros.extend(pagina)
                    if len(pagina) < PAGE_SIZE:
                        return registros
                    skip += PAGE_SIZE
                    time.sleep(DELAY_REQ)
                    break
                elif resp.status_code == 429:
                    print("  ⏳ Rate limit — aguardando 15s...")
                    time.sleep(15)
                else:
                    print(f"  ⚠️  HTTP {resp.status_code} (tentativa {t})")
                    time.sleep(2)
            except requests.exceptions.RequestException as e:
                print(f"  ❌ Erro de rede: {e} (tentativa {t})")
                time.sleep(3)
        else:
            print(f"  ⚠️  Parando paginação após falhas")
            return registros


# ─────────────────────────────────────────────
#  HELPERS — TRANSFORMAÇÃO
# ─────────────────────────────────────────────
def clean_str(x):
    v = str(x).strip() if x is not None and not (isinstance(x, float) and pd.isna(x)) else None
    return None if v in ("", "None", "nan", "[]", "['']") else v

def parse_dict(x, key):
    try:
        d = x if isinstance(x, dict) else ast.literal_eval(str(x))
        return d.get(key)
    except: return None

def pessoa_nome(x):
    try:
        d = x if isinstance(x, dict) else ast.literal_eval(str(x))
        return f"{d.get('name', '')} {d.get('lastName','')}".strip() or None
    except: return None

def modelo_inferido(mercado):
    m = str(mercado).strip()
    if m in B2B_MERCADOS:  return "B2B"
    if m in D2C_MERCADOS:  return "D2C"
    if m in HOME_MERCADOS: return "Home & Deco"
    return "Outros"


# ─────────────────────────────────────────────
#  TRANSFORMAR LEADS
# ─────────────────────────────────────────────
def transformar_leads(df_leads: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["id"]          = df_leads.get("id",       pd.Series(dtype=str)).apply(clean_str)
    out["lead"]        = df_leads.get("lead",      pd.Series(dtype=str)).apply(clean_str)
    out["cnpj"]        = df_leads.get("cnpj",      pd.Series(dtype=str)).apply(clean_str)
    out["website"]     = df_leads.get("website",   pd.Series(dtype=str)).apply(clean_str)
    _datas = pd.to_datetime(df_leads.get("registerDate"), errors="coerce", utc=True)
    if hasattr(_datas, "dt"):
        out["data_cadastro"] = _datas.dt.tz_localize(None)
    else:
        out["data_cadastro"] = None
    out["cidade"]  = df_leads.get("city",  pd.Series(dtype=str)).apply(clean_str).str.strip().str.title()
    out["estado"]  = df_leads.get("state", pd.Series(dtype=str)).apply(clean_str).str.strip().str.title()
    out["uf"]      = out["estado"].apply(lambda x: UF_MAP.get(str(x).strip(), "N/A") if x else "N/A")

    if os.path.exists(ARQUIVO_REGIOES):
        df_r = pd.read_csv(ARQUIVO_REGIOES)
        df_r["Cidade"] = df_r["Cidade"].str.strip().str.title()
        mapa = df_r.set_index("Cidade")["Região"].to_dict()
        out["regiao"] = out["cidade"].map(mapa).fillna(out["estado"])
    else:
        out["regiao"] = out["estado"].fillna("N/A")

    out["etapa"]      = df_leads.get("stage",    pd.Series(dtype=str)).apply(clean_str).fillna("Sem Etapa")
    out["funil_nome"] = df_leads.get("funnelId", pd.Series(dtype=str)).apply(clean_str).map(MAPA_FUNIS).fillna("Outros")
    out["origem"]     = df_leads.get("source",   pd.Series(dtype=str)).apply(lambda x: parse_dict(x, "value")).apply(clean_str).fillna("N/A")
    out["mercado"]    = df_leads.get("industry", pd.Series(dtype=str)).apply(lambda x: parse_dict(x, "value")).apply(clean_str).fillna("N/A")
    out["usuario"]    = df_leads.get("sdr",      pd.Series(dtype=str)).apply(pessoa_nome).apply(clean_str).fillna("N/A")
    out["modelo_negocio"] = out["mercado"].apply(modelo_inferido)

    # Colunas custom vazias (serão preenchidas na fase 2)
    for col in [
        "modelo_negocio_crm", "faturamento_mensal", "porte_empresa",
        "plataforma_ecommerce", "utm_source", "utm_medium", "utm_term",
        "utm_content", "utm_campaign", "moby_dick_estrategico",
        "eventos_participou", "atualizado_por_ia", "situacao_migracao",
    ]:
        out[col] = None

    return out[out["cidade"].notna() | out["estado"].notna()].copy()


# ─────────────────────────────────────────────
#  FASE 1 — LEADS
# ─────────────────────────────────────────────
def fase_leads(engine):
    print("\n📡 FASE 1 — Buscando todos os leads...")
    leads = _get_paginado("leads")
    print(f"   ✅ {len(leads):,} leads recebidos")

    df = transformar_leads(pd.DataFrame(leads))
    print(f"   🔄 {len(df):,} leads transformados")
    print(f"   💾 Salvando no Supabase...")
    upsert_db(engine, df)


# ─────────────────────────────────────────────
#  FASE 2 — CAMPOS PERSONALIZADOS
# ─────────────────────────────────────────────
def _processar_e_salvar(engine, registros: list):
    """Processa lista de registros de campos custom e salva no banco."""
    if not registros:
        return

    df_raw = pd.DataFrame(registros)
    if "leadId" not in df_raw.columns or "id" not in df_raw.columns:
        return

    def extrair(row):
        opts = row.get("options")
        if opts and isinstance(opts, list):
            return " | ".join(filter(None, [str(o) for o in opts if o])) or None
        v = row.get("value")
        return clean_str(v)

    df_raw["_lead_id"] = df_raw["leadId"].astype(str)
    df_raw["_valor"]   = df_raw.apply(extrair, axis=1)
    df_raw["_col"]     = df_raw["id"].map(CAMPOS_CUSTOM)

    df_f = df_raw[df_raw["_col"].notna()].copy()
    if df_f.empty:
        return

    pivot = (
        df_f.groupby(["_lead_id", "_col"])["_valor"]
        .apply(lambda v: " | ".join(filter(None, v.dropna().astype(str).unique())))
        .reset_index()
        .pivot(index="_lead_id", columns="_col", values="_valor")
        .reset_index()
    )
    pivot.columns.name = None

    with engine.begin() as conn:
        for _, row in pivot.iterrows():
            sets   = []
            params = {"lead_id": row["_lead_id"]}
            for col in CAMPOS_CUSTOM.values():
                if col in pivot.columns:
                    sets.append(f"{col} = :{col}")
                    params[col] = row.get(col)
            if sets:
                conn.execute(
                    text(f"UPDATE leads SET {', '.join(sets)}, atualizado_em = NOW() WHERE id = :lead_id"),
                    params
                )


def fase_custom(engine):
    print("\n📡 FASE 2 — Buscando campos personalizados por lotes de leads...")

    # Pegar todos os IDs do banco
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id FROM leads ORDER BY id"))
        ids_banco = [row[0] for row in result]

    total_ids = len(ids_banco)
    print(f"   📂 {total_ids:,} leads para processar")

    # Busca campo por campo — muito mais eficiente!
    # Cada campo retorna todos os leads que o têm preenchido
    print(f"   Buscando campo por campo ({len(CAMPOS_CUSTOM)} campos)...")

    for campo_key, campo_col in CAMPOS_CUSTOM.items():
        print(f"   📋 Buscando '{campo_key}' → '{campo_col}'...")
        registros_campo = _get_paginado(
            "LeadsCustomFields",
            filtro=f"id eq '{campo_key}'"
        )
        print(f"      ✅ {len(registros_campo):,} registros")
        if registros_campo:
            _processar_e_salvar(engine, registros_campo)

    print(f"   ✅ Todos os campos personalizados processados!")
    return



    df_raw = pd.DataFrame(todos_registros)
    print(f"   Colunas retornadas: {df_raw.columns.tolist()}")

    # API retorna: id, value, leadId, options, registerDate, updateDate, type
    # 'type' = campo key (_porte, _persona, etc)
    # 'leadId' = ID do lead
    # 'value' = valor texto
    # 'options' = lista de opções selecionadas (EscolhaUnica/Multipla)

    if "leadId" not in df_raw.columns or "id" not in df_raw.columns:
        print(f"   ❌ Colunas esperadas não encontradas. Colunas: {df_raw.columns.tolist()}")
        return

    def extrair(row):
        opts = row.get("options")
        if opts and isinstance(opts, list):
            return " | ".join(filter(None, [str(o) for o in opts if o])) or None
        v = row.get("value")
        return clean_str(v)

    df_raw["_lead_id"] = df_raw["leadId"].astype(str)
    df_raw["_valor"]   = df_raw.apply(extrair, axis=1)
    df_raw["_col"]     = df_raw["id"].map(CAMPOS_CUSTOM)

    df_f = df_raw[df_raw["_col"].notna()].copy()
    print(f"   📊 {len(df_f):,} registros de campos relevantes encontrados")

    if df_f.empty:
        print("   ⚠️  Nenhum dos campos mapeados foi encontrado")
        print(f"   Types encontrados: {df_raw['type'].dropna().unique()[:20].tolist()}")
        return

    pivot = (
        df_f.groupby(["_lead_id","_col"])["_valor"]
        .apply(lambda v: " | ".join(filter(None, v.dropna().astype(str).unique())))
        .reset_index()
        .pivot(index="_lead_id", columns="_col", values="_valor")
        .reset_index()
    )
    pivot.columns.name = None
    print(f"   ✅ {len(pivot):,} leads com campos personalizados")

    # Salvar no banco em lotes
    print(f"   💾 Atualizando banco de dados...")
    atualizados = 0
    lote = 500
    with engine.begin() as conn:
        for i in range(0, len(pivot), lote):
            chunk = pivot.iloc[i:i+lote]
            for _, row in chunk.iterrows():
                sets   = []
                params = {"lead_id": row["_lead_id"]}
                for col in CAMPOS_CUSTOM.values():
                    if col in pivot.columns:
                        sets.append(f"{col} = :{col}")
                        params[col] = row.get(col)
                if sets:
                    conn.execute(
                        text(f"UPDATE leads SET {', '.join(sets)}, atualizado_em = NOW() WHERE id = :lead_id"),
                        params
                    )
            atualizados += len(chunk)
            print(f"   💾 {atualizados:,}/{len(pivot):,} leads atualizados...", end="\r")

    print(f"\n   ✅ Campos personalizados salvos: {atualizados:,} leads")


# ─────────────────────────────────────────────
#  MODO DELTA
# ─────────────────────────────────────────────
def modo_delta(engine):
    print("\n🟡 MODO DELTA — Atualizações recentes\n")

    desde     = datetime.now(timezone.utc) - timedelta(hours=DELTA_HORAS)
    desde_str = desde.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"   🕐 Buscando desde {desde_str}")

    leads_delta = _get_paginado("leads", filtro=f"updateDate ge {desde_str}")
    print(f"   ✅ {len(leads_delta):,} leads atualizados")

    if not leads_delta:
        print("   ℹ️  Nenhuma atualização")
        return

    df = transformar_leads(pd.DataFrame(leads_delta))
    upsert_db(engine, df)

    # Campos personalizados dos leads atualizados
    ids_delta = [str(r.get("id","")) for r in leads_delta if r.get("id")]
    print(f"\n📡 Buscando campos custom de {len(ids_delta):,} leads...")

    for i in range(0, len(ids_delta), 100):
        ids_str  = ",".join(ids_delta[i:i+100])
        registros = _get_paginado("leadsCustomFields", filtro=f"leadId in ({ids_str})")

        if registros:
            df_raw = pd.DataFrame(registros)
            col_lead  = next((c for c in df_raw.columns if c.lower() in ("leadid","lead_id")), None)
            col_field = next((c for c in df_raw.columns if c.lower() in ("fieldid","field_id")), None)
            col_value = next((c for c in df_raw.columns if c.lower() in ("value","fieldvalue")), None)
            col_opts  = next((c for c in df_raw.columns if c.lower() in ("options","fieldoptions")), None)

            if "leadId" in df_raw.columns and "id" in df_raw.columns:
                def extrair(row):
                    opts = row.get("options")
                    if opts and isinstance(opts, list):
                        return " | ".join(filter(None, [str(o) for o in opts if o])) or None
                    return clean_str(row.get("value"))

                df_raw["_lead_id"] = df_raw["leadId"].astype(str)
                df_raw["_valor"]   = df_raw.apply(extrair, axis=1)
                df_raw["_col"]     = df_raw["id"].map(CAMPOS_CUSTOM)

                df_f = df_raw[df_raw["_col"].notna()].copy()
                if not df_f.empty:
                    pivot = (
                        df_f.groupby(["_lead_id","_col"])["_valor"]
                        .apply(lambda v: " | ".join(filter(None, v.dropna().astype(str).unique())))
                        .reset_index()
                        .pivot(index="_lead_id", columns="_col", values="_valor")
                        .reset_index()
                    )
                    pivot.columns.name = None
                    with engine.begin() as conn:
                        for _, row in pivot.iterrows():
                            sets   = []
                            params = {"lead_id": row["_lead_id"]}
                            for col in CAMPOS_CUSTOM.values():
                                if col in pivot.columns:
                                    sets.append(f"{col} = :{col}")
                                    params[col] = row.get(col)
                            if sets:
                                conn.execute(
                                    text(f"UPDATE leads SET {', '.join(sets)}, atualizado_em = NOW() WHERE id = :lead_id"),
                                    params
                                )
        time.sleep(DELAY_REQ)

    print("   ✅ Delta concluído")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Extrator Magazord — Exact Sales → Supabase")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)

    if not EXACT_TOKEN:
        print("❌ EXACT_TOKEN não definido!"); raise SystemExit(1)
    if not SUPABASE_URL:
        print("❌ SUPABASE_URL não definido!"); raise SystemExit(1)

    engine = get_engine()
    criar_tabela(engine)

    args = sys.argv[1:]

    if "--full" in args:
        print("\n   Modo: FULL (fase 1 + fase 2)")
        fase_leads(engine)
        fase_custom(engine)
    elif "--leads" in args:
        print("\n   Modo: LEADS (só fase 1)")
        fase_leads(engine)
    elif "--custom" in args:
        print("\n   Modo: CUSTOM (só fase 2)")
        fase_custom(engine)
    else:
        print("\n   Modo: DELTA")
        modo_delta(engine)

    print(f"\n{'=' * 60}")
    print(f"✅ Concluído! {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")


if __name__ == "__main__":
    main()

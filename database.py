import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json

DATABASE_URL = os.getenv("DATABASE_URL") or \
    "postgresql://mfreturns_db_user:dTAlnHMqeFfhfLIWDUYCTj6mxXaqYqO3@dpg-d44qjq3ipnbc73apqqug-a/mfreturns_db"

conn = psycopg2.connect(DATABASE_URL, sslmode="require")
# conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor(cursor_factory=RealDictCursor)

# --------------------------------------------------------------------
# Initialize core tables
# --------------------------------------------------------------------
def init_db():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fund_returns (
        scheme_code TEXT PRIMARY KEY,
        scheme_name TEXT,
        type TEXT,
        plan TEXT,
        option TEXT,
        updated_at TIMESTAMP DEFAULT NOW(),
        return_1m FLOAT,
        return_3m FLOAT,
        return_6m FLOAT,
        return_1y FLOAT,
        return_3y FLOAT,
        return_5y FLOAT,
        return_7y FLOAT,
        return_10y FLOAT,
        results_json JSONB
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fund_metadata (
        scheme_code TEXT PRIMARY KEY,
        scheme_name TEXT,
        amc TEXT,
        category TEXT,
        subcategory TEXT,
        plan TEXT,
        option TEXT,
        type TEXT
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS filter_cache (
        type TEXT PRIMARY KEY,
        amcs JSONB,
        categories JSONB,
        subcategories JSONB,
        plans JSONB,
        options JSONB,
        total INT,
        mutual_funds INT,
        etfs INT,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """)
    conn.commit()

def ensure_results_json_column():
    cursor.execute("""ALTER TABLE fund_returns ADD COLUMN IF NOT EXISTS results_json JSONB;""")
    conn.commit()

# --------------------------------------------------------------------
# Upsert Metadata (schemes CSV)
# --------------------------------------------------------------------
def upsert_metadata(records):
    for s in records:
        cursor.execute("""
        INSERT INTO fund_metadata (scheme_code, scheme_name, amc, category, subcategory, plan, option, type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (scheme_code)
        DO UPDATE SET
          scheme_name=EXCLUDED.scheme_name,
          amc=EXCLUDED.amc,
          category=EXCLUDED.category,
          subcategory=EXCLUDED.subcategory,
          plan=EXCLUDED.plan,
          option=EXCLUDED.option,
          type=EXCLUDED.type;
        """, (
            s.get("scheme_code"), s.get("scheme_name"), s.get("amc"),
            s.get("category"), s.get("subcategory"), s.get("plan"),
            s.get("option"), s.get("type")
        ))
    conn.commit()

def get_schemes_from_db(filters=None):
    base = "SELECT * FROM fund_metadata WHERE 1=1"
    params = []
    if filters:
        for k, v in filters.items():
            if v:
                base += f" AND LOWER({k}) LIKE %s"
                params.append(f"%{v.lower()}%")
    cursor.execute(base, params)
    return cursor.fetchall()

# --------------------------------------------------------------------
# Filter Cache Helpers
# --------------------------------------------------------------------
def upsert_filter_cache(type_, data):
    cursor.execute("""
    INSERT INTO filter_cache (type, amcs, categories, subcategories, plans, options,
                              total, mutual_funds, etfs, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
    ON CONFLICT (type)
    DO UPDATE SET
        amcs=EXCLUDED.amcs,
        categories=EXCLUDED.categories,
        subcategories=EXCLUDED.subcategories,
        plans=EXCLUDED.plans,
        options=EXCLUDED.options,
        total=EXCLUDED.total,
        mutual_funds=EXCLUDED.mutual_funds,
        etfs=EXCLUDED.etfs,
        updated_at=NOW();
    """, (
        type_,
        Json(data.get("amcs")), Json(data.get("categories")),
        Json(data.get("subcategories")), Json(data.get("plans")),
        Json(data.get("options")), data.get("total"),
        data.get("mutual_funds"), data.get("etfs")
    ))
    conn.commit()

def get_filter_cache(type_):
    cursor.execute("SELECT * FROM filter_cache WHERE type=%s", (type_,))
    return cursor.fetchone()

# --------------------------------------------------------------------
# Return Caching Helpers
# --------------------------------------------------------------------
def upsert_fund_results_json(scheme_code, scheme_name, results_obj, meta=None):
    meta = meta or {}
    cursor.execute("""
    INSERT INTO fund_returns (scheme_code, scheme_name, type, plan, option, updated_at, results_json)
    VALUES (%s,%s,%s,%s,%s,NOW(),%s)
    ON CONFLICT (scheme_code)
    DO UPDATE SET
        scheme_name=EXCLUDED.scheme_name,
        type=EXCLUDED.type,
        plan=EXCLUDED.plan,
        option=EXCLUDED.option,
        updated_at=NOW(),
        results_json=EXCLUDED.results_json;
    """, (
        scheme_code, scheme_name,
        meta.get("type"), meta.get("plan"), meta.get("option"),
        Json(results_obj)
    ))
    conn.commit()

def get_precomputed_return_json(code):
    cursor.execute("SELECT * FROM fund_returns WHERE scheme_code=%s", (code,))
    return cursor.fetchone()

def get_all_cached_returns(limit=200):
    cursor.execute("""
        SELECT scheme_code, scheme_name, results_json, updated_at
        FROM fund_returns
        ORDER BY updated_at DESC
        LIMIT %s
    """, (limit,))
    return cursor.fetchall()

def safe_upsert(DB, *args, **kwargs):
    try:
        DB.upsert_fund_results_json(*args, **kwargs)
    except psycopg2.OperationalError:
        print("🔁 [DB] Connection dropped, reconnecting...")
        DB.connect()  # or re-init your connection object
        DB.upsert_fund_results_json(*args, **kwargs)
    except Exception as e:
        print(f"💾 [DB] upsert failed: {e}")

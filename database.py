import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json

DATABASE_URL = os.getenv("DATABASE_URL") or \
    "postgresql://mfreturns_db_user:dTAlnHMqeFfhfLIWDUYCTj6mxXaqYqO3@dpg-d44qjq3ipnbc73apqqug-a/mfreturns_db"

class Database:
    def __init__(self):
        self.DATABASE_URL = DATABASE_URL
        self.connect()

    def connect(self):
        """Establish DB connection"""
        try:
            self.conn = psycopg2.connect(self.DATABASE_URL, sslmode="require")
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            print("[DB] Connection established")
        except Exception as e:
            print("[DB] Connection failed:", e)
            self.conn = None
            self.cursor = None

    def ensure_connection_alive(self):
        """Reconnect automatically if connection is dropped"""
        try:
            self.cursor.execute("SELECT 1;")
        except Exception:
            print("🔁 [DB] Connection dropped — reconnecting...")
            self.connect()

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            print("[DB] Connection closed cleanly at shutdown.")
        except Exception as e:
            print("[DB] Error closing connection:", e)

    # ----------------------------------------------------------------
    # TABLE CREATION
    # ----------------------------------------------------------------
    def init_db(self):
        self.cursor.execute("""
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
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS fund_returns (
            scheme_code TEXT PRIMARY KEY,
            scheme_name TEXT,
            type TEXT,
            plan TEXT,
            option TEXT,
            updated_at TIMESTAMP DEFAULT NOW(),
            results_json JSONB
        );
        """)
        self.cursor.execute("""
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
        self.conn.commit()
        print("[DB] Tables initialized")

    # ----------------------------------------------------------------
    # METADATA UPSERT
    # ----------------------------------------------------------------
    def upsert_metadata(self, records):
        """Bulk upsert scheme metadata"""
        if not records:
            print("[DB] No metadata to upsert")
            return
        for s in records:
            self.cursor.execute("""
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
                s.get("scheme_code"),
                s.get("scheme_name"),
                s.get("amc"),
                s.get("category"),
                s.get("subcategory"),
                s.get("plan"),
                s.get("option"),
                s.get("type")
            ))
        self.conn.commit()
        print(f"[DB] Metadata upserted — total: {len(records)} schemes")

    # ----------------------------------------------------------------
    # FILTER HELPERS (for /api/schemes and /api/stats)
    # ----------------------------------------------------------------
    def get_schemes_from_db(self, filters=None):
        """Return filtered scheme list from DB."""
        base = "SELECT * FROM fund_metadata WHERE 1=1"
        params = []

        if filters:
            for k, v in filters.items():
                if v and isinstance(v, list):
                    # handle multiple filter values (IN clause)
                    base += f" AND LOWER({k}) = ANY(%s)"
                    params.append([x.lower() for x in v])
                elif v:
                    base += f" AND LOWER({k}) LIKE %s"
                    params.append(f"%{v.lower()}%")

        self.cursor.execute(base, params)
        return self.cursor.fetchall()

    def get_filter_cache(self, type_):
        self.cursor.execute("SELECT * FROM filter_cache WHERE type=%s", (type_,))
        return self.cursor.fetchone()

    def upsert_filter_cache(self, type_, data):
        """Cache dropdowns and totals."""
        self.cursor.execute("""
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
            Json(data.get("amcs")),
            Json(data.get("categories")),
            Json(data.get("subcategories")),
            Json(data.get("plans")),
            Json(data.get("options")),
            data.get("total"),
            data.get("mutual_funds"),
            data.get("etfs")
        ))
        self.conn.commit()
    # ----------------------------------------------------------------
    # Metadata count helper
    # ----------------------------------------------------------------
    def count_metadata(self):
        try:
            self.cursor.execute("SELECT COUNT(*) FROM fund_metadata;")
            row = self.cursor.fetchone()
            return row["count"] if isinstance(row, dict) else row[0]
        except Exception as e:
            print("[DB] count_metadata failed:", e)
            return 0


# ----------------------------------------------------------------
# Initialize DB Singleton
# ----------------------------------------------------------------
DB = Database()

def init_db():
    DB.init_db()

def ensure_connection_alive():
    DB.ensure_connection_alive()

def upsert_metadata(records):
    DB.upsert_metadata(records)

def get_schemes_from_db(filters=None):
    return DB.get_schemes_from_db(filters)

def get_filter_cache(type_):
    return DB.get_filter_cache(type_)

def upsert_filter_cache(type_, data):
    DB.upsert_filter_cache(type_, data)

def count_metadata():
    return DB.count_metadata()

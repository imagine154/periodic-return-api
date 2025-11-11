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

        def like_any_clause(column, values):
            return f" AND LOWER({column}) LIKE ANY(%s)", [f"%{x.lower()}%" for x in values]

        if filters:
            # Type: equality match (Mutual Fund / ETF)
            tval = filters.get("type")
            if tval:
                base += " AND LOWER(type) = %s"
                params.append(tval.lower())

            # For other string filters, use LIKE ANY for partial, case-insensitive matches
            for k in ["amc", "category", "subcategory", "plan", "option"]:
                v = filters.get(k)
                if v:
                    if isinstance(v, list):
                        clause, arr = like_any_clause(k, v)
                        base += clause
                        params.append(arr)
                    else:
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

    def get_top_performers(self, investment_type, categories, sort_by, plan, option):
        """Fetch top 2 performing funds for each category."""
        self.ensure_connection_alive()

        plan_clause = "AND fm.plan = %s" if plan else ""
        query = f"""
            WITH ranked_funds AS (
                SELECT
                    fr.scheme_code,
                    fm.scheme_name,
                    fm.category,
                    NULLIF(fr.results_json ->> %s, '')::float AS return_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY fm.category
                        ORDER BY NULLIF(fr.results_json ->> %s, '')::float DESC NULLS LAST
                    ) AS rn
                FROM fund_returns fr
                JOIN fund_metadata fm ON fr.scheme_code = fm.scheme_code
                WHERE
                    fm.type = %s
                    AND fm.category = ANY(%s)
                    {plan_clause}
                    AND fm.option = %s
            )
            SELECT
                scheme_code,
                scheme_name,
                category,
                ROUND(return_value::numeric, 2) AS "return"
            FROM ranked_funds
            WHERE rn <= 2
              AND return_value IS NOT NULL
            ORDER BY category, rn;
        """

        params = [sort_by, sort_by, investment_type, categories]
        if plan:
            params.append(plan)
        params.append(option)

        self.cursor.execute(query, tuple(params))
        return self.cursor.fetchall()









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

    def get_precomputed_return_json(self, scheme_code):
        """Fetch precomputed returns JSON or legacy columns from DB."""
        self.ensure_connection_alive()
        self.cursor.execute("""
                            SELECT scheme_code, scheme_name, results_json, updated_at,
                                   return_1m, return_3m, return_6m, return_1y,
                                   return_3y, return_5y, return_7y, return_10y
                            FROM fund_returns
                            WHERE scheme_code = %s
                                LIMIT 1;
                            """, (scheme_code,))
        return self.cursor.fetchone()


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

def get_precomputed_return_json(scheme_code):
    return DB.get_precomputed_return_json(scheme_code)

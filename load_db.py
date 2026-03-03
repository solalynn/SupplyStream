"""load_db.py — Load processed CSVs into SQLite supply_chain.db
Creates all 13 tables with FK relationships as defined in schema_v2.

Usage:
  python load_db.py
  python load_db.py --db path/to/custom.db
  python load_db.py --drop  # drop and recreate all tables
"""
import sqlite3, argparse, sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent / "etl"))
from config import PROCESSED, DB_PATH

# ── DDL Statements ────────────────────────────────────────────────────────────
DDL = [
    # ── Dimension 1: supply_planning_master (no FK deps)
    """CREATE TABLE IF NOT EXISTS supply_planning_master (
        plant_id            INTEGER PRIMARY KEY,
        plant_code          TEXT NOT NULL UNIQUE,
        plant_name          TEXT,
        plant_short         TEXT,
        plant_type          TEXT CHECK(plant_type IN ('OWN','CO-PACK','COBO')),
        location_id         INTEGER,
        total_lines         INTEGER DEFAULT 0,
        std_lead_time_days  INTEGER DEFAULT 2,
        is_active           INTEGER DEFAULT 1
    )""",

    # ── Dimension 2: location_master (FK → supply_planning_master)
    """CREATE TABLE IF NOT EXISTS location_master (
        location_id         INTEGER PRIMARY KEY,
        location_code       TEXT NOT NULL UNIQUE,
        location_name       TEXT,
        location_type       TEXT CHECK(location_type IN ('WH','DC','STORE','PORT')),
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        region              TEXT,
        capacity_pallets    INTEGER DEFAULT 0,
        capacity_cases      INTEGER DEFAULT 0
    )""",

    # ── Dimension 3: sku_master (FK → supply_planning_master)
    """CREATE TABLE IF NOT EXISTS sku_master (
        sku_id              INTEGER PRIMARY KEY,
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        material_code       TEXT NOT NULL,
        description_eng     TEXT,
        description_tha     TEXT,
        brand               TEXT,
        pack_type           TEXT CHECK(pack_type IN ('PET','CAN','BIB','CO2')),
        flavor              TEXT,
        prod_hierarchy      TEXT,
        hier_l1 TEXT, hier_l2 TEXT, hier_l3 TEXT, hier_l4 TEXT,
        hier_l5 TEXT, hier_l6 TEXT, hier_l7 TEXT,
        net_content_ml      INTEGER DEFAULT 0,
        unit_per_case       INTEGER DEFAULT 0,
        case_dim_mm         TEXT,
        case_gross_wt_kg    REAL,
        tray_type           TEXT,
        cases_per_pallet    INTEGER DEFAULT 0,
        pallet_height_m     REAL,
        shelf_life_days     INTEGER DEFAULT 0,
        std_rc_8oz          REAL,
        dual_plant_flag     INTEGER DEFAULT 0,
        is_active           INTEGER DEFAULT 1
    )""",

    # ── Dimension 4: production_line (FK → supply_planning_master)
    """CREATE TABLE IF NOT EXISTS production_line (
        line_id             INTEGER PRIMARY KEY,
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        line_code           TEXT NOT NULL,
        line_name           TEXT,
        line_type           TEXT CHECK(line_type IN ('PET','CAN','BIB','CO2')),
        line_series         TEXT,
        bpm_capacity        INTEGER DEFAULT 0,
        ne_target_pct       REAL DEFAULT 0.90,
        sku_count           INTEGER DEFAULT 0,
        moq_cases           REAL DEFAULT 0,
        is_active           INTEGER DEFAULT 1
    )""",

    # ── Dimension 5: unit_conversion (FK → sku_master)
    """CREATE TABLE IF NOT EXISTS unit_conversion (
        conv_id             INTEGER PRIMARY KEY,
        sku_id              INTEGER REFERENCES sku_master(sku_id),
        material_code       TEXT,
        transcode           TEXT,
        pack_size_desc      TEXT,
        content_ml          REAL DEFAULT 0,
        bottles_per_case    INTEGER DEFAULT 0,
        cases_per_pallet    INTEGER DEFAULT 0,
        factor_8oz          REAL,
        factor_rc_to_cv     REAL,
        type_desc           TEXT
    )""",

    # ── Fact 1: inventory_ohlc (FK → sku_master, location_master, supply_planning_master)
    """CREATE TABLE IF NOT EXISTS inventory_ohlc (
        ohlc_id             INTEGER PRIMARY KEY,
        sku_id              INTEGER REFERENCES sku_master(sku_id),
        location_id         INTEGER REFERENCES location_master(location_id),
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        period_date         TEXT NOT NULL,
        period_type         TEXT DEFAULT 'WEEK',
        week_label          TEXT,
        stock_open          REAL DEFAULT 0,
        stock_high          REAL DEFAULT 0,
        stock_low           REAL DEFAULT 0,
        stock_close         REAL DEFAULT 0,
        unrestricted_stock  REAL,
        quality_stock       REAL,
        block_stock         REAL,
        in_transit_stock    REAL,
        units_produced      REAL DEFAULT 0,
        units_consumed      REAL DEFAULT 0,
        stockout_flag       INTEGER DEFAULT 0,
        unit_type           TEXT DEFAULT 'RC'
    )""",

    # ── Fact 2: demand_signal (FK → sku_master, location_master)
    """CREATE TABLE IF NOT EXISTS demand_signal (
        signal_id           INTEGER PRIMARY KEY,
        sku_id              INTEGER REFERENCES sku_master(sku_id),
        location_id         INTEGER REFERENCES location_master(location_id),
        period_date         TEXT NOT NULL,
        period_type         TEXT DEFAULT 'WEEK',
        demand_type         TEXT CHECK(demand_type IN ('IBP','PRE_FCST','POST_FCST','ACTUAL','BILLING','PLAN')),
        week_label          TEXT,
        volume_rc           REAL DEFAULT 0,
        volume_8oz          REAL,
        volume_cv           REAL,
        forecast_upper      REAL,
        forecast_lower      REAL,
        forecast_model      TEXT,
        mape                REAL
    )""",

    # ── Fact 3: production_order (FK → sku_master, supply_planning_master, production_line)
    """CREATE TABLE IF NOT EXISTS production_order (
        prod_order_id       INTEGER PRIMARY KEY,
        sku_id              INTEGER REFERENCES sku_master(sku_id),
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        line_id             INTEGER REFERENCES production_line(line_id),
        destination_id      INTEGER REFERENCES location_master(location_id),
        planned_order_no    TEXT,
        posting_date        TEXT,
        production_version  TEXT,
        shift               TEXT,
        planned_qty         REAL DEFAULT 0,
        initial_qty         REAL DEFAULT 0,
        gr_qty              REAL DEFAULT 0,
        scv_qty             REAL DEFAULT 0,
        uom                 TEXT DEFAULT 'CS',
        batch_no            TEXT,
        pallet_id           TEXT,
        mrp_controller      TEXT,
        order_status        TEXT
    )""",

    # ── Fact 4: atp_stock (FK → sku_master, supply_planning_master)
    """CREATE TABLE IF NOT EXISTS atp_stock (
        atp_id              INTEGER PRIMARY KEY,
        sku_id              INTEGER REFERENCES sku_master(sku_id),
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        snapshot_date       TEXT,
        unrestricted_stock  REAL DEFAULT 0,
        so_committed        REAL DEFAULT 0,
        do_committed        REAL DEFAULT 0,
        on_floor_total      REAL DEFAULT 0,
        total_atp           REAL DEFAULT 0,
        convert_to_pl       REAL,
        uom                 TEXT DEFAULT 'CS'
    )""",

    # ── Fact 5: wh_space (FK → location_master)
    """CREATE TABLE IF NOT EXISTS wh_space (
        wh_space_id         INTEGER PRIMARY KEY,
        location_id         INTEGER REFERENCES location_master(location_id),
        week_date           TEXT,
        week_label          TEXT,
        begin_pallets       REAL DEFAULT 0,
        production_in_pallets REAL DEFAULT 0,
        demand_out_pallets  REAL DEFAULT 0,
        end_pallets         REAL DEFAULT 0,
        capacity_pallets    REAL DEFAULT 0,
        utilization_pct     REAL DEFAULT 0
    )""",

    # ── Fact 6: line_load_plan (FK → production_line, supply_planning_master)
    """CREATE TABLE IF NOT EXISTS line_load_plan (
        load_id             INTEGER PRIMARY KEY,
        line_id             INTEGER REFERENCES production_line(line_id),
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        plan_date           TEXT,
        week_label          TEXT,
        day_label           TEXT,
        record_type         TEXT,
        volume_load_rc      REAL DEFAULT 0,
        efficiency_pct      REAL DEFAULT 0,
        working_hours       REAL DEFAULT 0,
        load_pct            REAL DEFAULT 0
    )""",

    # ── Analytical 1: sc_indicators
    """CREATE TABLE IF NOT EXISTS sc_indicators (
        indicator_id        INTEGER PRIMARY KEY,
        sku_id              INTEGER REFERENCES sku_master(sku_id),
        location_id         INTEGER REFERENCES location_master(location_id),
        line_id             INTEGER REFERENCES production_line(line_id),
        period_date         TEXT,
        dos_7d              REAL,
        dos_30d             REAL,
        inv_ma_7d           REAL,
        fill_rate           REAL,
        ne_actual_pct       REAL,
        line_load_pct       REAL,
        demand_vs_supply    REAL,
        risk_score          REAL,
        reorder_signal      INTEGER DEFAULT 0,
        demand_momentum     REAL
    )""",

    # ── Analytical 2: alert_events
    """CREATE TABLE IF NOT EXISTS alert_events (
        alert_id            INTEGER PRIMARY KEY,
        sku_id              INTEGER REFERENCES sku_master(sku_id),
        location_id         INTEGER REFERENCES location_master(location_id),
        plant_id            INTEGER REFERENCES supply_planning_master(plant_id),
        alert_date          TEXT,
        alert_type          TEXT,
        severity            TEXT CHECK(severity IN ('LOW','MEDIUM','HIGH','CRITICAL')),
        message             TEXT,
        is_resolved         INTEGER DEFAULT 0,
        meta                TEXT  -- JSON blob
    )""",
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_ohlc_sku_date ON inventory_ohlc(sku_id, period_date)",
    "CREATE INDEX IF NOT EXISTS idx_demand_sku_date ON demand_signal(sku_id, period_date, demand_type)",
    "CREATE INDEX IF NOT EXISTS idx_po_sku_date ON production_order(sku_id, posting_date)",
    "CREATE INDEX IF NOT EXISTS idx_ohlc_plant_date ON inventory_ohlc(plant_id, period_date)",
    "CREATE INDEX IF NOT EXISTS idx_sku_material ON sku_master(material_code)",
    "CREATE INDEX IF NOT EXISTS idx_sku_brand ON sku_master(brand)",
]


def get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def drop_all(conn):
    tables = [
        "alert_events","sc_indicators","line_load_plan","wh_space",
        "atp_stock","production_order","demand_signal","inventory_ohlc",
        "unit_conversion","production_line","sku_master",
        "location_master","supply_planning_master",
    ]
    for t in tables:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.commit()
    print("[DB] Dropped all tables")


def create_schema(conn):
    for ddl in DDL:
        conn.execute(ddl)
    for idx in INDEXES:
        conn.execute(idx)
    conn.commit()
    print(f"[DB] Created {len(DDL)} tables + {len(INDEXES)} indexes")


def load_csv(conn, csv_path: Path, table: str, col_map: dict | None = None):
    """Load a CSV into a SQLite table, mapping columns as needed."""
    if not csv_path.exists() or csv_path.stat().st_size < 5:
        print(f"[DB] Skip {csv_path.name} — file missing or empty")
        return 0

    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    if df.empty:
        print(f"[DB] Skip {csv_path.name} — no data rows")
        return 0

    # Apply column rename if provided
    if col_map:
        df = df.rename(columns=col_map)

    # Get table columns from DB
    cur = conn.execute(f"PRAGMA table_info({table})")
    db_cols = {row[1] for row in cur.fetchall()}

    # Keep only columns that exist in the DB table
    shared = [c for c in df.columns if c in db_cols]
    df = df[shared]

    # Replace NaN/None/empty strings
    df = df.replace({"": None, "nan": None, "None": None, "NaN": None, "NULL": None})

    df.to_sql(table, conn, if_exists="append", index=False)
    conn.commit()
    print(f"[DB] {table}: loaded {len(df):,} rows")
    return len(df)


def resolve_fks(conn):
    """Update FK references (sku_id, line_id, location_id) where material_code / line_code used."""
    print("[DB] Resolving foreign key references...")

    # unit_conversion.sku_id via material_code
    conn.execute("""
        UPDATE unit_conversion SET sku_id = (
            SELECT sku_id FROM sku_master
            WHERE sku_master.material_code = unit_conversion.material_code
            LIMIT 1
        ) WHERE sku_id IS NULL
    """)

    # demand_signal.sku_id via material_code join
    conn.execute("""
        UPDATE demand_signal SET sku_id = (
            SELECT sku_id FROM sku_master
            WHERE sku_master.material_code = demand_signal.material_code
            LIMIT 1
        ) WHERE sku_id IS NULL AND material_code IS NOT NULL
    """)

    # demand_signal.location_id via plant_code
    conn.execute("""
        UPDATE demand_signal SET location_id = (
            SELECT location_id FROM supply_planning_master spm
            WHERE spm.plant_code = demand_signal.plant_code
            LIMIT 1
        ) WHERE location_id IS NULL AND plant_code IS NOT NULL
    """)

    # production_order.sku_id
    conn.execute("""
        UPDATE production_order SET sku_id = (
            SELECT sku_id FROM sku_master
            WHERE sku_master.material_code = production_order.material_code
            LIMIT 1
        ) WHERE sku_id IS NULL AND material_code IS NOT NULL
    """)

    # production_order.plant_id
    conn.execute("""
        UPDATE production_order SET plant_id = (
            SELECT plant_id FROM supply_planning_master
            WHERE plant_code = production_order.plant_code
            LIMIT 1
        ) WHERE plant_id IS NULL AND plant_code IS NOT NULL
    """)

    # production_order.line_id via line_code + plant_id
    conn.execute("""
        UPDATE production_order SET line_id = (
            SELECT line_id FROM production_line pl
            WHERE pl.line_code = production_order.line_code
              AND pl.plant_id = production_order.plant_id
            LIMIT 1
        ) WHERE line_id IS NULL AND line_code IS NOT NULL
    """)

    # line_load_plan.plant_id
    conn.execute("""
        UPDATE line_load_plan SET plant_id = (
            SELECT plant_id FROM supply_planning_master
            WHERE plant_code = line_load_plan.plant_code
            LIMIT 1
        ) WHERE plant_id IS NULL AND plant_code IS NOT NULL
    """)

    # line_load_plan.line_id
    conn.execute("""
        UPDATE line_load_plan SET line_id = (
            SELECT line_id FROM production_line pl
            WHERE pl.line_code = line_load_plan.line_code
              AND pl.plant_id = line_load_plan.plant_id
            LIMIT 1
        ) WHERE line_id IS NULL AND line_code IS NOT NULL
    """)

    # atp_stock.plant_id
    conn.execute("""
        UPDATE atp_stock SET plant_id = (
            SELECT plant_id FROM supply_planning_master
            WHERE plant_code = atp_stock.plant_code
            LIMIT 1
        ) WHERE plant_id IS NULL AND plant_code IS NOT NULL
    """)

    # atp_stock.sku_id
    conn.execute("""
        UPDATE atp_stock SET sku_id = (
            SELECT sku_id FROM sku_master
            WHERE material_code = atp_stock.material_code
            LIMIT 1
        ) WHERE sku_id IS NULL AND material_code IS NOT NULL
    """)

    # wh_space.location_id via location_code
    conn.execute("""
        UPDATE wh_space SET location_id = (
            SELECT location_id FROM location_master
            WHERE location_code = wh_space.location_code
            LIMIT 1
        ) WHERE location_id IS NULL AND location_code IS NOT NULL
    """)

    conn.commit()
    print("[DB] FK resolution complete")


def compute_sc_indicators(conn):
    """Compute basic SC indicators from loaded data."""
    print("[DB] Computing SC indicators...")
    conn.execute("DELETE FROM sc_indicators")

    conn.execute("""
        INSERT INTO sc_indicators (
            indicator_id, sku_id, location_id, period_date,
            dos_7d, dos_30d, demand_vs_supply, reorder_signal
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY o.sku_id, o.period_date),
            o.sku_id,
            o.location_id,
            o.period_date,
            CASE WHEN o.units_consumed > 0
                 THEN ROUND(o.stock_close / (o.units_consumed / 7.0), 1)
                 ELSE NULL END AS dos_7d,
            CASE WHEN o.units_consumed > 0
                 THEN ROUND(o.stock_close / (o.units_consumed / 30.0), 1)
                 ELSE NULL END AS dos_30d,
            ROUND(o.units_produced - o.units_consumed, 0) AS demand_vs_supply,
            CASE WHEN o.stock_close < o.units_consumed * 1.5 THEN 1 ELSE 0 END AS reorder_signal
        FROM inventory_ohlc o
        WHERE o.sku_id IS NOT NULL
    """)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM sc_indicators").fetchone()[0]
    print(f"[DB] Computed {count:,} SC indicator rows")


def print_summary(conn):
    tables = [
        "supply_planning_master","location_master","sku_master","production_line",
        "unit_conversion","inventory_ohlc","demand_signal","production_order",
        "atp_stock","wh_space","line_load_plan","sc_indicators","alert_events",
    ]
    print("\n" + "="*50)
    print(" Database Summary")
    print("="*50)
    for t in tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:<35} {n:>8,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",   default=str(DB_PATH), help="SQLite DB path")
    parser.add_argument("--drop", action="store_true",  help="Drop all tables first")
    args = parser.parse_args()

    db_path = Path(args.db)
    print(f"[DB] Database: {db_path}")

    conn = get_conn(db_path)

    if args.drop:
        drop_all(conn)

    create_schema(conn)

    # Load in FK dependency order
    LOADS = [
        ("supply_planning_master.csv", "supply_planning_master", None),
        ("location_master.csv",        "location_master",        None),
        ("sku_master.csv",             "sku_master",             None),
        ("production_line.csv",        "production_line",        None),
        ("unit_conversion.csv",        "unit_conversion",        None),
        ("inventory_ohlc.csv",         "inventory_ohlc",         None),
        ("demand_signal.csv",          "demand_signal",          {"signal_id":"signal_id"}),
        ("production_orders.csv",      "production_order",       {"prod_order_id":"prod_order_id"}),
        ("atp_stock.csv",              "atp_stock",              None),
        ("wh_space.csv",               "wh_space",               None),
        ("line_load_plan.csv",         "line_load_plan",         None),
    ]

    total = 0
    for fname, table, col_map in LOADS:
        n = load_csv(conn, PROCESSED / fname, table, col_map)
        total += n

    resolve_fks(conn)
    compute_sc_indicators(conn)
    print_summary(conn)

    print(f"\n[DB] Total rows loaded: {total:,}")
    print(f"[DB] Database saved to: {db_path}")
    conn.close()


if __name__ == "__main__":
    main()

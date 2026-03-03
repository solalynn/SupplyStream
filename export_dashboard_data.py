"""export_dashboard_data.py — Export SQLite → JSON for the dashboard
Creates data/api/*.json files consumed by dashboard.html via fetch().

Usage:
  python export_dashboard_data.py
  python export_dashboard_data.py --db custom.db --top 50
"""
import sqlite3, json, argparse, sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "etl"))
from config import DB_PATH, API_DIR


def get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def q(conn, sql, params=()):
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def export_sku_list(conn, top=200):
    """SKU dropdown data: sku_id, material_code, name, brand, pack_type, plant."""
    rows = q(conn, """
        SELECT s.sku_id, s.material_code,
               COALESCE(s.description_eng, s.material_code) AS name,
               s.brand, s.pack_type,
               p.plant_code, p.plant_short, p.plant_name,
               s.std_rc_8oz, s.cases_per_pallet
        FROM sku_master s
        LEFT JOIN supply_planning_master p ON s.plant_id = p.plant_id
        WHERE s.is_active = 1
        ORDER BY s.brand, s.description_eng
        LIMIT ?
    """, (top,))
    return rows


def export_ohlc(conn, sku_id=None, plant_id=None):
    """OHLC candlestick data per SKU."""
    where = "WHERE 1=1"
    params = []
    if sku_id:
        where += " AND o.sku_id = ?"
        params.append(sku_id)
    if plant_id:
        where += " AND o.plant_id = ?"
        params.append(plant_id)

    rows = q(conn, f"""
        SELECT o.sku_id, o.plant_id, o.period_date AS time,
               o.week_label,
               o.stock_open  AS open,
               o.stock_high  AS high,
               o.stock_low   AS low,
               o.stock_close AS close,
               o.units_produced,
               o.units_consumed,
               o.stockout_flag,
               o.unrestricted_stock,
               o.unit_type
        FROM inventory_ohlc o
        {where}
        ORDER BY o.sku_id, o.period_date
    """, params)
    return rows


def export_demand(conn, sku_id=None, plant_id=None):
    """Demand signal series per SKU per type."""
    where = "WHERE 1=1"
    params = []
    if sku_id:
        where += " AND d.sku_id = ?"
        params.append(sku_id)

    rows = q(conn, f"""
        SELECT d.sku_id, d.period_date AS time,
               d.demand_type, d.week_label,
               d.volume_rc AS value
        FROM demand_signal d
        {where}
        ORDER BY d.sku_id, d.demand_type, d.period_date
    """, params)
    return rows


def export_production_orders(conn, sku_id=None, plant_id=None):
    """Production order markers."""
    where = "WHERE po.posting_date IS NOT NULL"
    params = []
    if sku_id:
        where += " AND po.sku_id = ?"
        params.append(sku_id)
    if plant_id:
        where += " AND po.plant_id = ?"
        params.append(plant_id)

    rows = q(conn, f"""
        SELECT po.prod_order_id, po.sku_id, po.plant_id,
               po.posting_date AS time,
               po.planned_qty, po.gr_qty,
               po.production_version, po.line_code,
               po.order_status
        FROM production_order po
        {where}
        ORDER BY po.sku_id, po.posting_date
    """, params)
    return rows


def export_wh_space(conn):
    rows = q(conn, """
        SELECT w.wh_space_id, l.location_code, l.location_name,
               l.plant_id,
               w.week_date AS time, w.week_label,
               w.begin_pallets, w.production_in_pallets,
               w.demand_out_pallets, w.end_pallets,
               w.capacity_pallets, w.utilization_pct
        FROM wh_space w
        LEFT JOIN location_master l ON w.location_id = l.location_id
        ORDER BY l.location_code, w.week_date
    """)
    return rows


def export_line_load(conn):
    rows = q(conn, """
        SELECT ll.load_id, ll.plant_id, pl.line_code, pl.line_name, pl.line_type,
               spm.plant_short,
               ll.plan_date AS time, ll.week_label, ll.day_label,
               ll.record_type,
               ll.volume_load_rc, ll.efficiency_pct,
               ll.working_hours, ll.load_pct
        FROM line_load_plan ll
        LEFT JOIN production_line pl  ON ll.line_id  = pl.line_id
        LEFT JOIN supply_planning_master spm ON ll.plant_id = spm.plant_id
        ORDER BY ll.plant_id, pl.line_code, ll.plan_date
    """)
    return rows


def export_atp(conn):
    rows = q(conn, """
        SELECT a.atp_id, s.material_code,
               COALESCE(s.description_eng, s.material_code) AS name,
               s.brand, spm.plant_code, spm.plant_short,
               a.snapshot_date, a.unrestricted_stock,
               a.so_committed, a.do_committed,
               a.on_floor_total, a.total_atp,
               a.convert_to_pl, a.uom
        FROM atp_stock a
        LEFT JOIN sku_master s ON a.sku_id = s.sku_id
        LEFT JOIN supply_planning_master spm ON a.plant_id = spm.plant_id
        ORDER BY a.total_atp DESC
    """)
    return rows


def export_kpis(conn):
    """Top-level KPI summary."""
    data = {}

    data["total_skus"] = conn.execute("SELECT COUNT(*) FROM sku_master WHERE is_active=1").fetchone()[0]
    data["total_plants"] = conn.execute("SELECT COUNT(*) FROM supply_planning_master WHERE is_active=1").fetchone()[0]

    # Current stock (latest ohlc week)
    row = conn.execute("""
        SELECT SUM(stock_close) AS total_stock,
               SUM(units_consumed) AS total_demand,
               SUM(units_produced) AS total_supply,
               SUM(stockout_flag) AS stockouts,
               COUNT(DISTINCT sku_id) AS sku_count
        FROM inventory_ohlc
        WHERE period_date = (SELECT MAX(period_date) FROM inventory_ohlc)
    """).fetchone()
    if row:
        data["current_stock_rc"]  = row[0] or 0
        data["weekly_demand_rc"]  = row[1] or 0
        data["weekly_supply_rc"]  = row[2] or 0
        data["stockouts_count"]   = row[3] or 0
        data["active_sku_count"]  = row[4] or 0
        data["dos"] = round((row[0] or 0) / max(row[1] or 1, 1) * 7, 1)

    data["snapshot_date"] = "2026-03-02"
    return data


def export_alerts(conn):
    """Top risk SKUs = those approaching stockout."""
    rows = q(conn, """
        SELECT s.material_code,
               COALESCE(s.description_eng, s.material_code) AS name,
               s.brand, spm.plant_short,
               ind.dos_7d, ind.dos_30d,
               ind.demand_vs_supply, ind.reorder_signal,
               ind.period_date
        FROM sc_indicators ind
        JOIN sku_master s ON ind.sku_id = s.sku_id
        LEFT JOIN supply_planning_master spm ON s.plant_id = spm.plant_id
        WHERE ind.period_date = (SELECT MAX(period_date) FROM sc_indicators)
          AND ind.dos_7d IS NOT NULL
        ORDER BY ind.dos_7d ASC
        LIMIT 20
    """)
    return rows


def write_json(path: Path, data, label: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str, ensure_ascii=False)
    size_kb = path.stat().st_size / 1024
    print(f"[EXPORT] {label}: {len(data) if isinstance(data, list) else '—'} rows → {path.name} ({size_kb:.1f} KB)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",  default=str(DB_PATH))
    parser.add_argument("--top", type=int, default=200, help="Max SKUs in sku_list")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERROR] Database not found: {db_path}")
        print("  Run: python load_db.py")
        sys.exit(1)

    conn = get_conn(db_path)
    API_DIR.mkdir(parents=True, exist_ok=True)

    # ── Export each dataset ────────────────────────────────────────────────────
    write_json(API_DIR / "sku_list.json",          export_sku_list(conn, args.top),  "sku_list")
    write_json(API_DIR / "inventory_ohlc.json",    export_ohlc(conn),                "inventory_ohlc")
    write_json(API_DIR / "demand_signal.json",     export_demand(conn),              "demand_signal")
    write_json(API_DIR / "production_orders.json", export_production_orders(conn),   "production_orders")
    write_json(API_DIR / "wh_space.json",          export_wh_space(conn),            "wh_space")
    write_json(API_DIR / "line_load.json",         export_line_load(conn),           "line_load")
    write_json(API_DIR / "atp_stock.json",         export_atp(conn),                 "atp_stock")
    write_json(API_DIR / "kpis.json",              export_kpis(conn),                "kpis")
    write_json(API_DIR / "alerts.json",            export_alerts(conn),              "alerts")

    conn.close()
    print(f"\n[EXPORT] Done — open dashboard.html in browser")
    print(f"         (serve with: python -m http.server 8000)")


if __name__ == "__main__":
    main()

"""ETL Master Runner
Runs all 7 ETL scripts in order and generates INVENTORY_OHLC table.

Usage:
  python etl/run_all.py              # run all
  python etl/run_all.py --step 03   # run specific step

After completion, run:
  python load_db.py                  # load CSVs into SQLite
  python export_dashboard_data.py    # export JSON for dashboard
"""
import sys, argparse, traceback, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import PROCESSED

import importlib


STEPS = [
    ("01", "01_extract_sku_master"),
    ("02", "02_extract_inventory"),
    ("03", "03_extract_demand"),
    ("04", "04_extract_production"),
    ("05", "05_extract_unit_conversion"),
    ("06", "06_extract_wh_utilization"),
    ("07", "07_extract_line_load"),
]


def compute_ohlc():
    """Derive INVENTORY_OHLC from inventory snapshot + demand/supply plans."""
    import pandas as pd
    import numpy as np
    from datetime import date, timedelta

    print("\n[OHLC] Computing INVENTORY_OHLC time-series...")

    # Load required CSVs
    def load(name):
        p = PROCESSED / f"{name}.csv"
        if p.exists() and p.stat().st_size > 5:
            return pd.read_csv(p, dtype=str, low_memory=False)
        return pd.DataFrame()

    sku_df  = load("sku_master")
    inv_df  = load("inventory_snapshot")
    dem_df  = load("demand_signal")
    sup_df  = load("supply_plan")

    if sku_df.empty or inv_df.empty:
        print("[OHLC] Missing sku_master or inventory_snapshot — skipping OHLC computation")
        pd.DataFrame().to_csv(PROCESSED / "inventory_ohlc.csv", index=False)
        return

    # Build sku_id lookup: material_code + plant_code → sku_id
    sku_df["plant_code"] = sku_df["plant_code"].astype(str)
    sku_map = sku_df.set_index(["material_code","plant_code"])["sku_id"].to_dict()

    # Pivot demand to week × material × plant (use POST_FCST if available, else first type)
    def pivot_weekly(df, value_col="volume_rc"):
        if df.empty or "material_code" not in df.columns:
            return pd.DataFrame()
        df = df.copy()
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0)
        # Prefer POST_FCST > IBP > PLAN > first
        priority = {"POST_FCST": 0, "IBP": 1, "PRE_FCST": 2, "PLAN": 3}
        if "demand_type" in df.columns:
            df["_priority"] = df["demand_type"].map(priority).fillna(9)
            df = df.sort_values("_priority").drop_duplicates(
                subset=["material_code","plant_code","period_date"])
        return df[["material_code","plant_code","period_date", value_col]]

    dem_pivot = pivot_weekly(dem_df)
    sup_pivot = pivot_weekly(sup_df)

    # Merge demand + supply with inventory snapshot as week-0
    inv_df["plant_code"] = inv_df["plant_code"].astype(str)
    inv_df["unrestricted_stock"] = pd.to_numeric(inv_df["unrestricted_stock"], errors="coerce").fillna(0)
    inv_df["in_transit_stock"]   = pd.to_numeric(inv_df.get("in_transit_stock", 0), errors="coerce").fillna(0)
    inv_df["quality_stock"]      = pd.to_numeric(inv_df.get("quality_stock", 0), errors="coerce").fillna(0)
    inv_df["block_stock"]        = pd.to_numeric(inv_df.get("block_stock", 0), errors="coerce").fillna(0)

    snap_map = inv_df.groupby(["material_code","plant_code"]).agg(
        unrestricted=("unrestricted_stock","sum"),
        quality=("quality_stock","sum"),
        blocked=("block_stock","sum"),
        in_transit=("in_transit_stock","sum"),
    ).to_dict("index")

    # Get all material×plant combos from demand
    combos = set()
    for df in [dem_pivot, sup_pivot]:
        if not df.empty:
            for _, row in df.iterrows():
                combos.add((row["material_code"], row.get("plant_code","7101") or "7101"))

    # All weeks
    W01 = date(2025, 12, 29)
    weeks = [(W01 + timedelta(weeks=i)).isoformat() for i in range(52)]
    week_labels = [f"w{i+1:02d}" for i in range(52)]

    ohlc_rows = []
    ohlc_id   = 1
    location_map = {"7101": 1, "7102": 2, "7104": 3, "7106": 4}
    plant_id_map = {"7101": 1, "7102": 2, "7104": 3, "7106": 4}
    SNAPSHOT = "2026-03-02"

    for (mat, plant) in combos:
        snap = snap_map.get((mat, plant), {})
        start_stock = snap.get("unrestricted", 0) + snap.get("in_transit", 0)
        if start_stock == 0:
            continue  # skip SKUs with no inventory data

        # Find week index of snapshot (W09 = index 8)
        snap_idx = 8  # W09 = Feb 23 2026

        # Build demand/supply lookup
        def build_lookup(df, mat, plant):
            if df.empty:
                return {}
            sub = df[(df["material_code"] == mat) & (df["plant_code"].astype(str) == plant)]
            return dict(zip(sub["period_date"], pd.to_numeric(sub.iloc[:, -1], errors="coerce").fillna(0)))

        dem_lkp = build_lookup(dem_pivot, mat, plant)
        sup_lkp = build_lookup(sup_pivot, mat, plant)

        stock = start_stock
        # Walk backwards from snap_idx to fill historical
        for i in range(snap_idx - 1, -1, -1):
            wdate = weeks[i]
            d = dem_lkp.get(wdate, 0)
            s = sup_lkp.get(wdate, 0)
            stock_end = stock
            stock_start = stock + d - s  # reverse the flow
            stock = max(0, stock_start)

        # Walk forward from 0
        stock = max(0, stock)  # use reconstructed week-0 stock
        snap_stock = start_stock  # re-anchor at snapshot

        # Rebuild forward from snap_idx with actual snapshot stock
        for i, (wdate, wlabel) in enumerate(zip(weeks, week_labels)):
            d = dem_lkp.get(wdate, 0)
            s = sup_lkp.get(wdate, 0)

            if i < snap_idx:
                # Historical: estimate from snapshot working backward
                open_stock = snap_stock
                close_stock = max(0, snap_stock - d * (snap_idx - i) * 0.8)
            elif i == snap_idx:
                open_stock  = snap_stock
                close_stock = max(0, snap_stock + s - d)
            else:
                # Future: rolling balance
                open_stock  = close_stock if i > 0 else snap_stock
                close_stock = max(0, open_stock + s - d)
                if i == snap_idx + 1:
                    open_stock = max(0, snap_stock + s - d)  # first future week

            high_stock = open_stock + s if s > 0 else max(open_stock, close_stock)
            low_stock  = min(open_stock, close_stock)

            sku_id = sku_map.get((mat, plant))
            plant_id = plant_id_map.get(plant, 1)
            loc_id = location_map.get(plant, 1)

            ohlc_rows.append({
                "ohlc_id":           ohlc_id,
                "sku_id":            sku_id,
                "location_id":       loc_id,
                "plant_id":          plant_id,
                "period_date":       wdate,
                "period_type":       "WEEK",
                "week_label":        wlabel,
                "stock_open":        round(open_stock, 0),
                "stock_high":        round(high_stock, 0),
                "stock_low":         round(low_stock, 0),
                "stock_close":       round(close_stock, 0),
                "unrestricted_stock":round(snap.get("unrestricted", 0), 0) if i == snap_idx else None,
                "quality_stock":     round(snap.get("quality", 0), 0) if i == snap_idx else None,
                "block_stock":       round(snap.get("blocked", 0), 0) if i == snap_idx else None,
                "in_transit_stock":  round(snap.get("in_transit", 0), 0) if i == snap_idx else None,
                "units_produced":    round(s, 0),
                "units_consumed":    round(d, 0),
                "stockout_flag":     1 if close_stock <= 0 else 0,
                "unit_type":         "RC",
            })
            ohlc_id += 1
            # Update rolling stock for next iteration
            if i >= snap_idx:
                close_stock = max(0, open_stock + s - d)

    ohlc_df = pd.DataFrame(ohlc_rows)
    out_path = PROCESSED / "inventory_ohlc.csv"
    ohlc_df.to_csv(out_path, index=False)
    print(f"[OHLC] Wrote {len(ohlc_df):,} OHLC rows → {out_path.name}")


def main():
    parser = argparse.ArgumentParser(description="Run Supply Chain ETL pipeline")
    parser.add_argument("--step", help="Run only this step (e.g. 03)", default=None)
    args = parser.parse_args()

    print("=" * 60)
    print(" SUPPLY STREAM ETL — Suntory PepsiCo Thailand")
    print("=" * 60)

    t0 = time.time()
    errors = []

    for step_id, module_name in STEPS:
        if args.step and args.step != step_id:
            continue
        print(f"\n{'─'*40}")
        print(f" Step {step_id}: {module_name}")
        print(f"{'─'*40}")
        try:
            mod = importlib.import_module(module_name)
            mod.run()
        except Exception as e:
            print(f"[ERROR] Step {step_id} failed: {e}")
            traceback.print_exc()
            errors.append(f"Step {step_id}: {e}")

    # Compute derived OHLC
    if not args.step or args.step == "ohlc":
        print(f"\n{'─'*40}")
        try:
            compute_ohlc()
        except Exception as e:
            print(f"[ERROR] OHLC computation failed: {e}")
            traceback.print_exc()

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    if errors:
        print(f" DONE with {len(errors)} error(s) in {elapsed:.1f}s")
        for e in errors:
            print(f"  ✗ {e}")
    else:
        print(f" DONE — all steps completed in {elapsed:.1f}s")
    print(f"{'='*60}")
    print(f"\n Next steps:")
    print(f"  python load_db.py")
    print(f"  python export_dashboard_data.py")
    print(f"  open dashboard.html")


if __name__ == "__main__":
    main()

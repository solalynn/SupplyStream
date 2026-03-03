"""ETL 03 — Demand & Supply Signals
Sources : Demand_Supply Part 1.xlsx + Demand_Supply Part 2.xlsx
Sheets  : D-Weekly (demand), P-Weekly (supply plan)
Output  : data/processed/demand_signal.csv
          data/processed/supply_plan.csv

Both sheets are expected in wide format:
  Material | Description | [Plant] | [Type] | W01 | W02 | ... | W52

If a "Type" column exists, rows represent different demand types (IBP, Pre-Fcst, etc.).
If no Type column, all rows are treated as the primary forecast (POST_FCST for demand, PLAN for supply).
"""
import sys, re
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_FILES, PROCESSED, SHEETS, DEMAND_TYPE_MAP, WEEK_LABELS

# ISO W01-2026 = Dec 29 2025 (Monday)
W01_DATE = date(2025, 12, 29)

MATERIAL_ALIASES = ["material", "material number", "material no", "mat.", "mat no", "matl", "material code"]
PLANT_ALIASES    = ["plant", "plant code", "plnt"]
TYPE_ALIASES     = ["type", "demand type", "forecast type", "stream", "category", "fcst type"]


def week_to_date(week_label: str) -> str:
    """Convert w01 / W01 / W1 → ISO date of that Monday."""
    m = re.search(r"(\d+)", str(week_label))
    if not m:
        return None
    wnum = int(m.group(1)) - 1  # 0-indexed
    d = W01_DATE + timedelta(weeks=wnum)
    return d.isoformat()


def find_material_col(df):
    lmap = {c.lower().strip(): c for c in df.columns}
    for alias in MATERIAL_ALIASES:
        if alias in lmap:
            return lmap[alias]
    return None


def find_plant_col(df):
    lmap = {c.lower().strip(): c for c in df.columns}
    for alias in PLANT_ALIASES:
        if alias in lmap:
            return lmap[alias]
    return None


def find_type_col(df):
    lmap = {c.lower().strip(): c for c in df.columns}
    for alias in TYPE_ALIASES:
        if alias in lmap:
            return lmap[alias]
    return None


def detect_week_cols(df):
    """Return list of (col_name, week_date_str) for week columns."""
    result = []
    for col in df.columns:
        if re.search(r"\bw\s*0*(\d{1,2})\b", str(col), re.IGNORECASE):
            d = week_to_date(col)
            if d:
                result.append((col, d))
    return result


def normalise_type(raw_type: str) -> str:
    if pd.isna(raw_type):
        return "POST_FCST"
    t = str(raw_type).lower().strip()
    for key, val in DEMAND_TYPE_MAP.items():
        if key in t:
            return val
    return str(raw_type).upper().strip()


def parse_sheet(xf, sheet_name, default_type):
    """Read a wide-format sheet and melt to long format."""
    df = None
    for hr in [0, 1, 2]:
        raw = pd.read_excel(xf, sheet_name=sheet_name, header=hr, dtype=str)
        raw = raw.dropna(how="all").map(lambda x: x.strip() if isinstance(x, str) else x)
        if find_material_col(raw):
            df = raw
            break
    if df is None:
        print(f"  [WARN] Cannot find material column in sheet '{sheet_name}'")
        return pd.DataFrame()

    mat_col   = find_material_col(df)
    plant_col = find_plant_col(df)
    type_col  = find_type_col(df)
    week_cols = detect_week_cols(df)

    if not week_cols:
        print(f"  [WARN] No week columns detected in sheet '{sheet_name}'")
        return pd.DataFrame()

    print(f"  Sheet '{sheet_name}': {len(df)} rows, {len(week_cols)} week cols, type_col={'yes' if type_col else 'no'}")

    records = []
    for _, row in df.iterrows():
        mat = str(row[mat_col]).strip()
        if not re.match(r"\d{5,}", mat):
            continue
        plant = str(row[plant_col]).strip() if plant_col else None
        rtype = normalise_type(row[type_col]) if type_col else default_type

        for col, period_date in week_cols:
            val = row[col]
            if pd.isna(val) or str(val).strip() in ("", "-", "N/A", "n/a"):
                continue
            try:
                volume = float(str(val).replace(",", ""))
            except ValueError:
                continue
            records.append({
                "material_code": mat,
                "plant_code":    plant,
                "period_date":   period_date,
                "period_type":   "WEEK",
                "demand_type":   rtype,
                "volume_rc":     volume,
            })

    return pd.DataFrame(records)


def find_sheet(xf, candidates):
    sheets_lower = {s.lower(): s for s in xf.sheet_names}
    for c in candidates:
        if c.lower() in sheets_lower:
            return sheets_lower[c.lower()]
    return None


def run():
    PROCESSED.mkdir(parents=True, exist_ok=True)

    all_demand = []
    all_supply = []

    for part_key in ["demand_part1", "demand_part2"]:
        src = RAW_FILES[part_key]
        if not src.exists():
            print(f"[WARN] {src.name} not found — skipping")
            continue

        print(f"[03] Reading {src.name}")
        xf = pd.ExcelFile(src)
        print(f"  Sheets: {xf.sheet_names}")

        # D-Weekly → demand
        d_sheet = find_sheet(xf, [SHEETS["d_weekly"], "D-Weekly", "D_Weekly", "demand weekly", "weekly demand"])
        if d_sheet:
            df_d = parse_sheet(xf, d_sheet, default_type="POST_FCST")
            if not df_d.empty:
                all_demand.append(df_d)
        else:
            print(f"  [WARN] D-Weekly sheet not found in {src.name}")

        # P-Weekly → supply plan
        p_sheet = find_sheet(xf, [SHEETS["p_weekly"], "P-Weekly", "P_Weekly", "supply weekly", "weekly supply", "production weekly"])
        if p_sheet:
            df_p = parse_sheet(xf, p_sheet, default_type="PLAN")
            if not df_p.empty:
                all_supply.append(df_p)
        else:
            print(f"  [WARN] P-Weekly sheet not found in {src.name}")

    # Combine
    demand_df = pd.concat(all_demand, ignore_index=True) if all_demand else pd.DataFrame()
    supply_df = pd.concat(all_supply, ignore_index=True) if all_supply else pd.DataFrame()

    # Add week labels
    def add_week_label(df):
        if df.empty:
            return df
        df["week_label"] = df["period_date"].apply(
            lambda d: f"w{int((pd.Timestamp(d) - pd.Timestamp('2025-12-29')).days / 7) + 1:02d}"
        )
        return df

    demand_df = add_week_label(demand_df)
    supply_df = add_week_label(supply_df)

    # Add surrogate keys
    if not demand_df.empty:
        demand_df.insert(0, "signal_id", range(1, len(demand_df) + 1))
    if not supply_df.empty:
        supply_df.insert(0, "plan_id", range(1, len(supply_df) + 1))

    out_d = PROCESSED / "demand_signal.csv"
    out_s = PROCESSED / "supply_plan.csv"
    demand_df.to_csv(out_d, index=False)
    supply_df.to_csv(out_s, index=False)
    print(f"[03] Demand: {len(demand_df):,} rows → {out_d.name}")
    print(f"[03] Supply: {len(supply_df):,} rows → {out_s.name}")


if __name__ == "__main__":
    run()

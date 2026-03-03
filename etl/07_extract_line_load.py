"""ETL 07 — Line Load Plan
Source : WH_Utilization.xlsx → sheets "RY Line Load", "SR Line Load"
Output : data/processed/line_load_plan.csv

Expected columns:
  Date / Week | Day | Line | Record Type | Volume (RC) | Efficiency % |
  Working Hours | Load %
"""
import sys, re
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_FILES, PROCESSED, SHEETS

ALIASES = {
    "plan_date":    ["date", "plan date", "schedule date", "day", "prod date"],
    "week_label":   ["week", "week label", "week no", "wk", "week#"],
    "day_label":    ["day", "day label", "day of week", "weekday"],
    "line_code":    ["line", "line code", "line name", "prod line", "line no"],
    "record_type":  ["type", "record type", "plan type", "category"],
    "volume_rc":    ["volume", "volume rc", "volume (rc)", "load volume", "rc", "quantity rc"],
    "efficiency":   ["efficiency", "eff %", "efficiency %", "ne %", "ne", "ne%"],
    "hours":        ["working hours", "hours", "run hours", "scheduled hours", "hr"],
    "load_pct":     ["load %", "load", "load pct", "loading %", "utilization %", "util %"],
}


def find_col(df, key):
    lmap = {c.lower().strip(): c for c in df.columns}
    for alias in ALIASES.get(key, []):
        if alias.lower() in lmap:
            return lmap[alias.lower()]
    return None


def find_sheet(xf, candidates):
    sheets_lower = {s.lower(): s for s in xf.sheet_names}
    for c in candidates:
        if c.lower() in sheets_lower:
            return sheets_lower[c.lower()]
    return None


def parse_line_load(xf, sheet_name, plant_code):
    print(f"  Parsing line load: '{sheet_name}' (plant {plant_code})")
    for hr in [0, 1, 2]:
        raw = pd.read_excel(xf, sheet_name=sheet_name, header=hr, dtype=str)
        raw = raw.dropna(how="all").map(lambda x: x.strip() if isinstance(x, str) else x)
        if find_col(raw, "line_code") or find_col(raw, "plan_date"):
            df = raw
            break
    else:
        print(f"  [WARN] Cannot parse sheet '{sheet_name}'")
        return pd.DataFrame()

    out = pd.DataFrame()
    out["plant_code"]   = plant_code
    out["line_code"]    = df[find_col(df, "line_code")] if find_col(df, "line_code") else ""
    out["record_type"]  = df[find_col(df, "record_type")] if find_col(df, "record_type") else "PLAN"
    out["day_label"]    = df[find_col(df, "day_label")] if find_col(df, "day_label") else ""
    out["week_label"]   = df[find_col(df, "week_label")] if find_col(df, "week_label") else ""

    # Date
    date_col = find_col(df, "plan_date")
    if date_col:
        out["plan_date"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    else:
        out["plan_date"] = None

    def pct_col(key):
        col = find_col(df, key)
        if col:
            s = pd.to_numeric(df[col].str.replace(",","").str.replace("%",""), errors="coerce").fillna(0)
            if s.mean() < 2 and s.max() <= 1:
                s = s * 100
            return s.round(2)
        return 0.0

    out["volume_load_rc"]  = pd.to_numeric(df[find_col(df, "volume_rc")].str.replace(",",""), errors="coerce").fillna(0) if find_col(df, "volume_rc") else 0.0
    out["efficiency_pct"]  = pct_col("efficiency")
    out["working_hours"]   = pd.to_numeric(df[find_col(df, "hours")].str.replace(",",""), errors="coerce").fillna(0) if find_col(df, "hours") else 0.0
    out["load_pct"]        = pct_col("load_pct")

    out = out.dropna(subset=["plan_date"], how="all")
    return out


def run():
    src = RAW_FILES["wh_utilization"]
    if not src.exists():
        print(f"[WARN] {src.name} not found — writing empty line_load_plan.csv")
        PROCESSED.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(PROCESSED / "line_load_plan.csv", index=False)
        return

    print(f"[07] Reading {src.name}")
    xf = pd.ExcelFile(src)
    PROCESSED.mkdir(parents=True, exist_ok=True)

    all_loads = []
    for plant_code, candidates in [
        ("7101", [SHEETS["ry_line_load"], "RY Line Load", "RY_Line_Load", "RY Line", "Rayong Line Load", "Line Load RY"]),
        ("7102", [SHEETS["sr_line_load"], "SR Line Load", "SR_Line_Load", "SR Line", "Saraburi Line Load", "Line Load SR"]),
    ]:
        sheet = find_sheet(xf, candidates)
        if sheet:
            df = parse_line_load(xf, sheet, plant_code)
            if not df.empty:
                all_loads.append(df)
        else:
            print(f"  [WARN] No line load sheet for plant {plant_code}")

    if all_loads:
        combined = pd.concat(all_loads, ignore_index=True)
        combined.insert(0, "load_id", range(1, len(combined) + 1))
    else:
        combined = pd.DataFrame()

    out_path = PROCESSED / "line_load_plan.csv"
    combined.to_csv(out_path, index=False)
    print(f"[07] Wrote {len(combined):,} line load rows → {out_path.name}")


if __name__ == "__main__":
    run()

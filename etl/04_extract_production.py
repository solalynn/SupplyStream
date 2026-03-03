"""ETL 04 — Production Orders (SAP)
Source : Outputs_128_Feb_2026.xlsx → sheets "Outputs 7101", "Outputs 7102"
Output : data/processed/production_orders.csv

Expected SAP production order columns:
  Order | Order Type | Material | Description | Plant | Version | Posting Date |
  Planned Qty | GR Qty | SCV Qty | UoM | Line | Shift | MRP Controller | Status
"""
import sys, re
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_FILES, PROCESSED, SHEETS

ALIASES = {
    "order_no":     ["order", "planned order", "planned order no", "order no", "order number", "prd order"],
    "material":     ["material", "material number", "mat.", "mat no"],
    "description":  ["description", "material description", "desc"],
    "plant":        ["plant", "plant code", "plnt"],
    "version":      ["production version", "prod version", "version", "prod ver"],
    "posting_date": ["posting date", "date", "order date", "prod date", "plan date"],
    "planned_qty":  ["planned qty", "planned quantity", "plan qty", "total plan qty", "qty"],
    "gr_qty":       ["gr qty", "goods receipt", "gr quantity", "confirmed qty", "delivered qty", "actual qty"],
    "scv_qty":      ["scv qty", "scv", "variance qty"],
    "uom":          ["uom", "unit", "base unit", "uom", "unit of measure"],
    "line":         ["line", "prod line", "production line", "line code", "line no"],
    "shift":        ["shift", "shift type"],
    "mrp_ctrl":     ["mrp controller", "mrp ctrl", "mrp", "controller"],
    "status":       ["status", "order status", "sys status"],
    "batch":        ["batch", "batch no", "batch number"],
    "pallet_id":    ["pallet", "pallet id", "pallet no"],
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


def parse_orders(xf, sheet_name, plant_code):
    print(f"  Parsing sheet: '{sheet_name}' (plant {plant_code})")
    df = None
    for hr in [0, 1, 2]:
        raw = pd.read_excel(xf, sheet_name=sheet_name, header=hr, dtype=str)
        raw = raw.dropna(how="all").map(lambda x: x.strip() if isinstance(x, str) else x)
        if find_col(raw, "material"):
            df = raw
            break
    if df is None:
        print(f"  [WARN] Cannot detect material column in '{sheet_name}'")
        return pd.DataFrame()

    out = pd.DataFrame()
    out["planned_order_no"]   = df[find_col(df, "order_no")]  if find_col(df, "order_no")  else ""
    out["material_code"]      = df[find_col(df, "material")].astype(str)
    out["description"]        = df[find_col(df, "description")] if find_col(df, "description") else ""
    out["plant_code"]         = df[find_col(df, "plant")].astype(str) if find_col(df, "plant") else plant_code
    out["production_version"] = df[find_col(df, "version")] if find_col(df, "version") else ""
    out["line_code"]          = df[find_col(df, "line")]  if find_col(df, "line")  else ""
    out["shift"]              = df[find_col(df, "shift")] if find_col(df, "shift") else ""
    out["mrp_controller"]     = df[find_col(df, "mrp_ctrl")] if find_col(df, "mrp_ctrl") else ""
    out["order_status"]       = df[find_col(df, "status")] if find_col(df, "status") else ""
    out["batch_no"]           = df[find_col(df, "batch")]     if find_col(df, "batch")    else ""
    out["pallet_id"]          = df[find_col(df, "pallet_id")] if find_col(df, "pallet_id") else ""
    out["uom"]                = df[find_col(df, "uom")]       if find_col(df, "uom")      else "CS"

    def to_num(key):
        col = find_col(df, key)
        if col:
            return pd.to_numeric(df[col].str.replace(",", ""), errors="coerce").fillna(0)
        return 0.0

    out["planned_qty"] = to_num("planned_qty")
    out["gr_qty"]      = to_num("gr_qty")
    out["scv_qty"]     = to_num("scv_qty")

    # Posting date
    date_col = find_col(df, "posting_date")
    if date_col:
        out["posting_date"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    else:
        out["posting_date"] = ""

    # Keep only rows with numeric material code
    out = out[out["material_code"].str.match(r"\d{5,}", na=False)]
    return out


def run():
    src = RAW_FILES["outputs"]
    if not src.exists():
        print(f"[WARN] {src.name} not found — writing empty production_orders.csv")
        PROCESSED.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(PROCESSED / "production_orders.csv", index=False)
        return

    print(f"[04] Reading {src.name}")
    xf = pd.ExcelFile(src)
    print(f"  Sheets: {xf.sheet_names}")

    all_orders = []
    for plant_code, candidates in [
        ("7101", [SHEETS["outputs_7101"], "Outputs 7101", "7101", "RY", "Rayong"]),
        ("7102", [SHEETS["outputs_7102"], "Outputs 7102", "7102", "SR", "Saraburi"]),
    ]:
        sheet = find_sheet(xf, candidates)
        if sheet:
            orders = parse_orders(xf, sheet, plant_code)
            if not orders.empty:
                all_orders.append(orders)
        else:
            print(f"  [WARN] No sheet found for plant {plant_code}")

    if all_orders:
        combined = pd.concat(all_orders, ignore_index=True)
        combined.insert(0, "prod_order_id", range(1, len(combined) + 1))
    else:
        combined = pd.DataFrame()

    PROCESSED.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED / "production_orders.csv"
    combined.to_csv(out_path, index=False)
    print(f"[04] Wrote {len(combined):,} production orders → {out_path.name}")


if __name__ == "__main__":
    run()

"""ETL 02 — Inventory Snapshot
Source : Inventory_2_Mar_2026.xlsx  →  sheet "Inv-SCP"
Output : data/processed/inventory_snapshot.csv
         (feeds INVENTORY_OHLC as week-0 starting point)

Expected SAP columns (flexible matching):
  Plant | Storage Loc | Material | Description | Unrestricted |
  Qual.Insp | Blocked | In Transit | Total | UoM
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_FILES, PROCESSED, SHEETS, SNAPSHOT_DATE

ALIASES = {
    "plant":         ["plant", "plant code", "plnt", "plant_code"],
    "storage_loc":   ["storage location", "stor. loc.", "stor.loc", "sloc", "storage loc", "wh"],
    "material_code": ["material", "material number", "mat.", "mat no", "matl"],
    "description":   ["material description", "description", "desc"],
    "unrestricted":  ["unrestricted", "unrestricted stock", "unrestd", "free stock", "unrestricted (cs)"],
    "quality":       ["quality inspection", "qual. insp.", "qual.insp", "quality", "qm stock", "qual insp"],
    "blocked":       ["blocked", "blocked stock", "block", "blk"],
    "in_transit":    ["stock in transit", "in transit", "transit", "in_transit", "intransit"],
    "total":         ["total", "total stock", "total (cs)", "total stock (cs)"],
    "uom":           ["base unit", "uom", "unit", "base unit of measure", "bun"],
}


def find_col(df, key):
    lower_map = {c.lower().strip(): c for c in df.columns}
    for alias in ALIASES.get(key, []):
        if alias.lower() in lower_map:
            return lower_map[alias.lower()]
    return None


def find_sheet(xf, candidates):
    sheets_lower = {s.lower(): s for s in xf.sheet_names}
    for c in candidates:
        if c.lower() in sheets_lower:
            return sheets_lower[c.lower()]
    # fallback: first sheet
    return xf.sheet_names[0]


def run():
    src = RAW_FILES["inventory"]
    if not src.exists():
        print(f"[WARN] {src.name} not found — writing empty inventory_snapshot.csv")
        PROCESSED.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["material_code","plant_code","unrestricted_stock","quality_stock",
                               "block_stock","in_transit_stock","total_stock","uom","snapshot_date"]).to_csv(
            PROCESSED / "inventory_snapshot.csv", index=False)
        return

    print(f"[02] Reading {src.name}")
    xf = pd.ExcelFile(src)
    sheet = find_sheet(xf, [SHEETS["inv_scp"], "Inv-SCP", "inventory", "stock", "Inv SCP"])
    print(f"[02] Using sheet: {sheet}")

    df = None
    for hr in [0, 1, 2]:
        raw = pd.read_excel(xf, sheet_name=sheet, header=hr, dtype=str)
        raw = raw.dropna(how="all").map(lambda x: x.strip() if isinstance(x, str) else x)
        if find_col(raw, "material_code"):
            df = raw
            break

    if df is None:
        print("[ERROR] Cannot detect material column in inventory sheet")
        return

    out = pd.DataFrame()
    out["plant_code"]        = df[find_col(df, "plant")]       if find_col(df, "plant")       else "7101"
    out["storage_loc"]       = df[find_col(df, "storage_loc")] if find_col(df, "storage_loc") else "FG01"
    out["material_code"]     = df[find_col(df, "material_code")].astype(str)
    out["description"]       = df[find_col(df, "description")] if find_col(df, "description") else ""

    def num(col_key):
        col = find_col(df, col_key)
        if col:
            return pd.to_numeric(df[col].str.replace(",", ""), errors="coerce").fillna(0)
        return 0.0

    out["unrestricted_stock"] = num("unrestricted")
    out["quality_stock"]      = num("quality")
    out["block_stock"]        = num("blocked")
    out["in_transit_stock"]   = num("in_transit")
    out["total_stock"]        = num("total")
    out["uom"]                = df[find_col(df, "uom")].str.upper() if find_col(df, "uom") else "CS"
    out["snapshot_date"]      = SNAPSHOT_DATE

    # Keep only numeric material codes
    out = out[out["material_code"].str.match(r"\d{5,}", na=False)]
    out = out.dropna(subset=["plant_code"])

    # If total_stock is 0, compute from components
    mask = out["total_stock"] == 0
    out.loc[mask, "total_stock"] = (
        out.loc[mask, "unrestricted_stock"] +
        out.loc[mask, "quality_stock"] +
        out.loc[mask, "block_stock"]
    )

    PROCESSED.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED / "inventory_snapshot.csv"
    out.to_csv(out_path, index=False)
    print(f"[02] Wrote {len(out):,} inventory rows → {out_path.name}")


if __name__ == "__main__":
    run()

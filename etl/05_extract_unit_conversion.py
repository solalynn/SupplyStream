"""ETL 05 — Unit Conversion Factors
Source : Demand_Supply Part 1 or 2 → sheet "Parameter"
         (fallback: Outputs file)
Output : data/processed/unit_conversion.csv

Parameter sheet columns (typical):
  Material | Transcode | Pack Size Desc | Content (ml) |
  Bottles/Case | Cases/Pallet | Factor 8oz | Factor RC→CV | Type Desc
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_FILES, PROCESSED, SHEETS

ALIASES = {
    "material":         ["material", "material number", "mat.", "mat no", "material code"],
    "transcode":        ["transcode", "trans code", "planning code", "trans desc", "transco"],
    "pack_size_desc":   ["pack size", "pack size desc", "pack description", "size desc", "pack desc"],
    "content_ml":       ["content", "content (ml)", "ml", "net content", "net content (ml)", "content ml"],
    "bottles_per_case": ["bottles per case", "bot/case", "unit per case", "qty per case", "pcs/case"],
    "cases_per_pallet": ["cases per pallet", "case/pallet", "cs/pl", "pallet qty"],
    "factor_8oz":       ["factor 8oz", "8oz factor", "8oz", "factor_8oz", "8oz conv", "rc to 8oz"],
    "factor_rc_to_cv":  ["factor rc to cv", "rc to cv", "rc/cv", "cv factor", "factor cv"],
    "type_desc":        ["type", "type desc", "product type", "category", "pack type"],
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


def parse_parameter(xf, sheet_name):
    for hr in [0, 1, 2]:
        raw = pd.read_excel(xf, sheet_name=sheet_name, header=hr, dtype=str)
        raw = raw.dropna(how="all").map(lambda x: x.strip() if isinstance(x, str) else x)
        if find_col(raw, "material"):
            return raw
    return None


def run():
    PROCESSED.mkdir(parents=True, exist_ok=True)
    all_params = []

    for key in ["demand_part1", "demand_part2", "outputs"]:
        src = RAW_FILES[key]
        if not src.exists():
            continue
        xf = pd.ExcelFile(src)
        sheet = find_sheet(xf, [SHEETS["parameter"], "Parameter", "param", "unit conversion",
                                 "conversion", "factor", "factors"])
        if not sheet:
            continue

        print(f"[05] Reading Parameter from {src.name} / {sheet}")
        df = parse_parameter(xf, sheet)
        if df is None:
            continue

        out = pd.DataFrame()
        out["material_code"]    = df[find_col(df, "material")].astype(str) if find_col(df, "material") else ""
        out["transcode"]        = df[find_col(df, "transcode")]        if find_col(df, "transcode")        else ""
        out["pack_size_desc"]   = df[find_col(df, "pack_size_desc")]   if find_col(df, "pack_size_desc")   else ""
        out["type_desc"]        = df[find_col(df, "type_desc")]        if find_col(df, "type_desc")        else ""

        def num(key, default=0.0):
            col = find_col(df, key)
            if col:
                return pd.to_numeric(df[col].str.replace(",", ""), errors="coerce").fillna(default)
            return default

        out["content_ml"]       = num("content_ml")
        out["bottles_per_case"] = num("bottles_per_case").astype(int)
        out["cases_per_pallet"] = num("cases_per_pallet").astype(int)
        out["factor_8oz"]       = num("factor_8oz", np.nan)
        out["factor_rc_to_cv"]  = num("factor_rc_to_cv", np.nan)

        out = out[out["material_code"].str.match(r"\d{5,}", na=False)]
        all_params.append(out)
        break  # only need from one file

    if all_params:
        combined = pd.concat(all_params, ignore_index=True).drop_duplicates(subset=["material_code"])
        combined.insert(0, "conv_id", range(1, len(combined) + 1))
        # sku_id will be joined in load_db.py
        combined["sku_id"] = None
    else:
        print("[WARN] Parameter sheet not found in any source file — writing empty unit_conversion.csv")
        combined = pd.DataFrame(columns=["conv_id","sku_id","material_code","transcode","pack_size_desc",
                                          "content_ml","bottles_per_case","cases_per_pallet",
                                          "factor_8oz","factor_rc_to_cv","type_desc"])

    out_path = PROCESSED / "unit_conversion.csv"
    combined.to_csv(out_path, index=False)
    print(f"[05] Wrote {len(combined):,} conversion rows → {out_path.name}")


if __name__ == "__main__":
    run()

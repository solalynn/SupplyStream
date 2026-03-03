"""ETL 01 — SKU Master
Source : Product_Information_Update.xlsx  (434 SKUs, ~44 columns)
Output : data/processed/sku_master.csv + supply_planning_master.csv + location_master.csv
"""
import sys, re
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_FILES, PROCESSED, PLANTS

# ── Flexible column finder ─────────────────────────────────────────────────────
ALIASES = {
    "material_code":    ["material", "material no", "material number", "mat", "mat.", "mat no", "matl"],
    "description_eng":  ["material description", "description", "description (en)", "description_eng",
                         "eng description", "eng desc", "material desc"],
    "description_tha":  ["thai description", "description (th)", "thai desc", "description_tha", "thai"],
    "brand":            ["brand", "brand name", "trade mark", "trademark"],
    "pack_type":        ["pack type", "pack_type", "pack", "packaging", "package type"],
    "flavor":           ["flavor", "flavour", "flavor level5", "flavour level5", "flavor_level5", "taste"],
    "prod_hierarchy":   ["prod hierarchy", "production hierarchy", "prod_hierarchy", "hierarchy", "sap hierarchy"],
    "net_content_ml":   ["net content", "net content (ml)", "content ml", "net_content_ml", "ml", "content (ml)"],
    "unit_per_case":    ["unit per case", "units per case", "pcs/case", "bottles per case", "unit/case",
                         "bot/case", "pcs per case", "qty per case"],
    "cases_per_pallet": ["cases per pallet", "case/pallet", "cases/pallet", "case per pallet",
                         "pallet qty", "cases_per_pallet", "cs/pl"],
    "pallet_height_m":  ["pallet height", "pallet height (m)", "height m", "pallet_height_m"],
    "shelf_life_days":  ["shelf life", "shelf life (days)", "shelf life days", "shelf_life_days", "sl"],
    "tray_type":        ["tray type", "tray_type", "tray", "packaging style"],
    "case_gross_wt_kg": ["case gross weight", "gross weight", "gross wt", "case weight", "gross weight (kg)"],
    "case_dim_mm":      ["case dimension", "case dim", "dim mm", "dimensions", "w x d x h", "case size"],
    "std_rc_8oz":       ["8oz factor", "rc to 8oz", "std rc 8oz", "std_rc_8oz", "8oz conv", "factor 8oz"],
    "dual_plant_flag":  ["dual plant", "dual plant flag", "dual_plant", "dual_plant_flag"],
    "is_active":        ["active", "is active", "is_active", "status", "active flag"],
    "plant_code":       ["plant", "plant code", "plant_code", "producing plant", "main plant"],
}


def find_col(df: pd.DataFrame, key: str) -> str | None:
    lower_map = {c.lower().strip(): c for c in df.columns}
    for alias in ALIASES.get(key, []):
        if alias.lower() in lower_map:
            return lower_map[alias.lower()]
    return None


def find_hier_cols(df: pd.DataFrame) -> list[str]:
    """Find SAP hierarchy level columns (L1…L7 or Hier_1…Hier_7)."""
    pattern = re.compile(r"hier.*?(\d)", re.IGNORECASE)
    found = {}
    for col in df.columns:
        m = pattern.search(col)
        if m:
            lvl = int(m.group(1))
            if 1 <= lvl <= 7:
                found[lvl] = col
    return [found.get(i, "") for i in range(1, 8)]


def clean_pack_type(val: str) -> str:
    if pd.isna(val):
        return "PET"
    val = str(val).upper().strip()
    if "CAN" in val:   return "CAN"
    if "BIB" in val:   return "BIB"
    if "CO2" in val:   return "CO2"
    return "PET"


def plant_code_to_id(code) -> int | None:
    code = str(code).strip()
    mapping = {"7101": 1, "7102": 2, "7104": 3, "7106": 4}
    return mapping.get(code, None)


def run():
    src = RAW_FILES["product_info"]
    if not src.exists():
        print(f"[WARN] {src.name} not found — writing empty placeholder CSVs")
        _write_placeholders()
        return

    print(f"[01] Reading {src.name}")
    # Try reading; some exports have header on row 0, some on row 1
    df = None
    for header_row in [0, 1, 2]:
        raw = pd.read_excel(src, header=header_row, dtype=str)
        mat_col = find_col(raw, "material_code")
        if mat_col:
            df = raw
            break
    if df is None:
        print("[ERROR] Cannot detect material column — aborting 01")
        return

    # Strip whitespace from all string columns
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.dropna(how="all")

    # ── Map columns ──────────────────────────────────────────────────────────
    out = pd.DataFrame()
    out["material_code"]    = df[find_col(df, "material_code")].astype(str)
    out["description_eng"]  = df[find_col(df, "description_eng")] if find_col(df, "description_eng") else ""
    out["description_tha"]  = df[find_col(df, "description_tha")] if find_col(df, "description_tha") else ""
    out["brand"]            = df[find_col(df, "brand")]            if find_col(df, "brand")           else "UNKNOWN"
    out["pack_type"]        = (df[find_col(df, "pack_type")] if find_col(df, "pack_type") else "PET").apply(clean_pack_type)
    out["flavor"]           = df[find_col(df, "flavor")]           if find_col(df, "flavor")          else ""
    out["prod_hierarchy"]   = df[find_col(df, "prod_hierarchy")]   if find_col(df, "prod_hierarchy")  else ""
    out["net_content_ml"]   = pd.to_numeric(df[find_col(df, "net_content_ml")]  if find_col(df, "net_content_ml")  else 0, errors="coerce").fillna(0).astype(int)
    out["unit_per_case"]    = pd.to_numeric(df[find_col(df, "unit_per_case")]   if find_col(df, "unit_per_case")   else 0, errors="coerce").fillna(0).astype(int)
    out["cases_per_pallet"] = pd.to_numeric(df[find_col(df, "cases_per_pallet")]if find_col(df, "cases_per_pallet") else 0, errors="coerce").fillna(0).astype(int)
    out["pallet_height_m"]  = pd.to_numeric(df[find_col(df, "pallet_height_m")] if find_col(df, "pallet_height_m") else np.nan, errors="coerce")
    out["shelf_life_days"]  = pd.to_numeric(df[find_col(df, "shelf_life_days")] if find_col(df, "shelf_life_days") else 0, errors="coerce").fillna(0).astype(int)
    out["tray_type"]        = df[find_col(df, "tray_type")]        if find_col(df, "tray_type")       else "TRAY"
    out["case_gross_wt_kg"] = pd.to_numeric(df[find_col(df, "case_gross_wt_kg")]if find_col(df, "case_gross_wt_kg") else np.nan, errors="coerce")
    out["case_dim_mm"]      = df[find_col(df, "case_dim_mm")]      if find_col(df, "case_dim_mm")     else ""
    out["std_rc_8oz"]       = pd.to_numeric(df[find_col(df, "std_rc_8oz")]      if find_col(df, "std_rc_8oz")      else np.nan, errors="coerce")
    out["dual_plant_flag"]  = df[find_col(df, "dual_plant_flag")].map(lambda x: 1 if str(x).strip().upper() in ("Y","YES","1","TRUE","X") else 0) if find_col(df, "dual_plant_flag") else 0
    out["is_active"]        = df[find_col(df, "is_active")].map(lambda x: 1 if str(x).strip().upper() in ("Y","YES","1","TRUE","X","A","ACTIVE") else 0) if find_col(df, "is_active") else 1

    # Plant
    plant_col = find_col(df, "plant_code")
    out["plant_code"] = df[plant_col].astype(str).str.strip() if plant_col else "7101"
    out["plant_id"]   = out["plant_code"].apply(plant_code_to_id)

    # Hierarchy levels
    hier_cols = find_hier_cols(df)
    for i, col in enumerate(hier_cols, 1):
        out[f"hier_l{i}"] = df[col] if col else ""

    # Surrogate key
    out = out.reset_index(drop=True)
    out.insert(0, "sku_id", range(1, len(out) + 1))

    # Drop rows with empty material code
    out = out[out["material_code"].str.match(r"\d{5,}", na=False)]

    PROCESSED.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED / "sku_master.csv"
    out.to_csv(out_path, index=False)
    print(f"[01] Wrote {len(out):,} SKUs → {out_path.name}")

    # ── Also seed master dimension tables ─────────────────────────────────────
    _write_plant_master()
    _write_location_master()
    _write_production_lines()


def _write_plant_master():
    rows = [
        {"plant_id": 1, "plant_code": "7101", "plant_name": "Rayong",      "plant_short": "RY", "plant_type": "OWN",     "location_id": 1, "total_lines": 9, "std_lead_time_days": 2, "is_active": 1},
        {"plant_id": 2, "plant_code": "7102", "plant_name": "Saraburi",    "plant_short": "SR", "plant_type": "OWN",     "location_id": 2, "total_lines": 5, "std_lead_time_days": 2, "is_active": 1},
        {"plant_id": 3, "plant_code": "7104", "plant_name": "Siam Water",  "plant_short": "SW", "plant_type": "CO-PACK", "location_id": 3, "total_lines": 0, "std_lead_time_days": 5, "is_active": 1},
        {"plant_id": 4, "plant_code": "7106", "plant_name": "Prime Water", "plant_short": "PW", "plant_type": "CO-PACK", "location_id": 4, "total_lines": 0, "std_lead_time_days": 5, "is_active": 1},
    ]
    pd.DataFrame(rows).to_csv(PROCESSED / "supply_planning_master.csv", index=False)
    print("[01] Wrote supply_planning_master.csv")


def _write_location_master():
    rows = [
        {"location_id": 1, "location_code": "FG01", "location_name": "Rayong FG Warehouse",      "location_type": "WH", "plant_id": 1, "region": "East",     "capacity_pallets": 8000,  "capacity_cases": 960000},
        {"location_id": 2, "location_code": "FG02", "location_name": "Saraburi FG Warehouse",    "location_type": "WH", "plant_id": 2, "region": "Central",  "capacity_pallets": 5000,  "capacity_cases": 600000},
        {"location_id": 3, "location_code": "SW01", "location_name": "Siam Water Warehouse",     "location_type": "WH", "plant_id": 3, "region": "Central",  "capacity_pallets": 2000,  "capacity_cases": 240000},
        {"location_id": 4, "location_code": "PW01", "location_name": "Prime Water Warehouse",    "location_type": "WH", "plant_id": 4, "region": "East",     "capacity_pallets": 2000,  "capacity_cases": 240000},
        {"location_id": 5, "location_code": "DC01", "location_name": "Bangkok Central DC",       "location_type": "DC", "plant_id": None, "region": "Central","capacity_pallets": 15000, "capacity_cases": 1800000},
        {"location_id": 6, "location_code": "DC02", "location_name": "Northern DC",              "location_type": "DC", "plant_id": None, "region": "North",  "capacity_pallets": 5000,  "capacity_cases": 600000},
    ]
    pd.DataFrame(rows).to_csv(PROCESSED / "location_master.csv", index=False)
    print("[01] Wrote location_master.csv")


def _write_production_lines():
    lines = []
    lid = 1
    # RY Lines 1-9
    ry_types = ["PET","PET","PET","PET","CAN","CAN","CAN","PET","BIB"]
    ry_bpm   = [600, 600, 700, 700, 800, 800, 800, 500, 200]
    for i in range(9):
        lines.append({"line_id": lid, "plant_id": 1, "line_code": f"LINE2-{i+1}", "line_name": f"Line {i+1}",
                      "line_type": ry_types[i], "line_series": "MS1", "bpm_capacity": ry_bpm[i],
                      "ne_target_pct": 0.90, "sku_count": 0, "moq_cases": 5000, "is_active": 1})
        lid += 1
    # SR Lines 1-5
    sr_types = ["PET","PET","CAN","CAN","PET"]
    sr_bpm   = [600, 600, 800, 800, 500]
    for i in range(5):
        lines.append({"line_id": lid, "plant_id": 2, "line_code": f"LINE3-{i+1}", "line_name": f"Line {i+1}",
                      "line_type": sr_types[i], "line_series": "SS1", "bpm_capacity": sr_bpm[i],
                      "ne_target_pct": 0.88, "sku_count": 0, "moq_cases": 4000, "is_active": 1})
        lid += 1
    pd.DataFrame(lines).to_csv(PROCESSED / "production_line.csv", index=False)
    print("[01] Wrote production_line.csv")


def _write_placeholders():
    PROCESSED.mkdir(parents=True, exist_ok=True)
    pd.DataFrame().to_csv(PROCESSED / "sku_master.csv", index=False)
    _write_plant_master()
    _write_location_master()
    _write_production_lines()


if __name__ == "__main__":
    run()

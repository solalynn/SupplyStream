"""ETL 06 — Warehouse Utilization & ATP Stock
Source : WH_Utilization.xlsx → sheets "WH Space", "ATP Stock"
Output : data/processed/wh_space.csv
         data/processed/atp_stock.csv

WH Space columns (typical):
  Week | Week Label | Location | Begin Pallets | Production In | Demand Out |
  End Pallets | Capacity | Utilization %

ATP Stock columns:
  Material | Plant | Unrestricted | SO Committed | DO Committed |
  On Floor Total | ATP | Convert to PL | UoM | Snapshot Date
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_FILES, PROCESSED, SHEETS, SNAPSHOT_DATE

WH_ALIASES = {
    "week_date":       ["week", "week date", "week start", "date", "period date", "period"],
    "week_label":      ["week label", "week no", "w#", "week_label", "wk"],
    "location_code":   ["location", "location code", "warehouse", "wh", "plant", "plant code"],
    "begin_pallets":   ["begin pallets", "begin", "opening pallets", "open pallets", "stock begin", "start pallets"],
    "prod_in_pallets": ["production in", "production in pallets", "prod in", "inbound pallets", "receipt pallets"],
    "demand_out":      ["demand out", "demand out pallets", "demand pallets", "outbound pallets", "shipment pallets"],
    "end_pallets":     ["end pallets", "end", "closing pallets", "close pallets", "stock end"],
    "capacity":        ["capacity", "capacity pallets", "max pallets", "total capacity"],
    "utilization_pct": ["utilization", "util %", "util%", "utilization %", "utilization_pct", "occupancy"],
}

ATP_ALIASES = {
    "material":     ["material", "material number", "mat.", "mat no"],
    "plant":        ["plant", "plant code", "plnt"],
    "unrestricted": ["unrestricted", "unrestricted stock", "unrestd"],
    "so_committed": ["so committed", "sales order committed", "so", "so commit"],
    "do_committed": ["do committed", "delivery order committed", "do", "do commit"],
    "on_floor":     ["on floor", "on floor total", "floor stock", "wip"],
    "atp":          ["atp", "total atp", "available to promise", "available stock"],
    "convert_pl":   ["convert to pl", "pallet qty", "convert pl", "pl qty"],
    "uom":          ["uom", "unit", "base unit"],
}


def find_col(df, aliases_dict, key):
    lmap = {c.lower().strip(): c for c in df.columns}
    for alias in aliases_dict.get(key, []):
        if alias.lower() in lmap:
            return lmap[alias.lower()]
    return None


def find_sheet(xf, candidates):
    sheets_lower = {s.lower(): s for s in xf.sheet_names}
    for c in candidates:
        if c.lower() in sheets_lower:
            return sheets_lower[c.lower()]
    return None


def read_best(xf, sheet_name, key_aliases_dict, key_col):
    for hr in [0, 1, 2]:
        raw = pd.read_excel(xf, sheet_name=sheet_name, header=hr, dtype=str)
        raw = raw.dropna(how="all").map(lambda x: x.strip() if isinstance(x, str) else x)
        if find_col(raw, key_aliases_dict, key_col):
            return raw
    return None


def num(df, aliases_dict, key, default=0.0):
    col = find_col(df, aliases_dict, key)
    if col:
        return pd.to_numeric(df[col].str.replace(",", "").str.replace("%", ""), errors="coerce").fillna(default)
    return default


def run():
    src = RAW_FILES["wh_utilization"]
    if not src.exists():
        print(f"[WARN] {src.name} not found — writing empty WH CSVs")
        PROCESSED.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(PROCESSED / "wh_space.csv", index=False)
        pd.DataFrame().to_csv(PROCESSED / "atp_stock.csv", index=False)
        return

    print(f"[06] Reading {src.name}")
    xf = pd.ExcelFile(src)
    print(f"  Sheets: {xf.sheet_names}")
    PROCESSED.mkdir(parents=True, exist_ok=True)

    # ── WH Space ──────────────────────────────────────────────────────────────
    wh_sheet = find_sheet(xf, [SHEETS["wh_space"], "WH Space", "WH_Space", "Warehouse", "Utilization", "Space"])
    if wh_sheet:
        df = read_best(xf, wh_sheet, WH_ALIASES, "begin_pallets")
        if df is not None:
            out = pd.DataFrame()
            week_col = find_col(df, WH_ALIASES, "week_date")
            if week_col:
                out["week_date"] = pd.to_datetime(df[week_col], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
            else:
                out["week_date"] = None
            out["week_label"]      = df[find_col(df, WH_ALIASES, "week_label")] if find_col(df, WH_ALIASES, "week_label") else ""
            out["location_code"]   = df[find_col(df, WH_ALIASES, "location_code")] if find_col(df, WH_ALIASES, "location_code") else "FG01"
            out["begin_pallets"]   = num(df, WH_ALIASES, "begin_pallets")
            out["prod_in_pallets"] = num(df, WH_ALIASES, "prod_in_pallets")
            out["demand_out"]      = num(df, WH_ALIASES, "demand_out")
            out["end_pallets"]     = num(df, WH_ALIASES, "end_pallets")
            out["capacity_pallets"]= num(df, WH_ALIASES, "capacity")

            # Compute utilization if missing
            util_col = find_col(df, WH_ALIASES, "utilization_pct")
            if util_col:
                raw_util = pd.to_numeric(df[util_col].str.replace(",","").str.replace("%",""), errors="coerce")
                # If values look like percentages (0-1 range), multiply by 100
                if raw_util.dropna().mean() < 2:
                    raw_util = raw_util * 100
                out["utilization_pct"] = raw_util.fillna(0).round(2)
            else:
                out["utilization_pct"] = np.where(
                    out["capacity_pallets"] > 0,
                    (out["end_pallets"] / out["capacity_pallets"] * 100).round(2),
                    0.0
                )

            out = out.dropna(subset=["week_date"])
            out.insert(0, "wh_space_id", range(1, len(out) + 1))
            out.to_csv(PROCESSED / "wh_space.csv", index=False)
            print(f"[06] Wrote {len(out):,} WH space rows → wh_space.csv")
        else:
            print("[WARN] Could not parse WH Space sheet")
            pd.DataFrame().to_csv(PROCESSED / "wh_space.csv", index=False)
    else:
        print("[WARN] WH Space sheet not found")
        pd.DataFrame().to_csv(PROCESSED / "wh_space.csv", index=False)

    # ── ATP Stock ─────────────────────────────────────────────────────────────
    atp_sheet = find_sheet(xf, [SHEETS["atp_stock"], "ATP Stock", "ATP_Stock", "ATP", "Available Stock"])
    if atp_sheet:
        df = read_best(xf, atp_sheet, ATP_ALIASES, "material")
        if df is not None:
            out = pd.DataFrame()
            out["material_code"]  = df[find_col(df, ATP_ALIASES, "material")].astype(str)
            out["plant_code"]     = df[find_col(df, ATP_ALIASES, "plant")].astype(str) if find_col(df, ATP_ALIASES, "plant") else "7101"
            out["snapshot_date"]  = SNAPSHOT_DATE
            out["unrestricted_stock"]  = num(df, ATP_ALIASES, "unrestricted")
            out["so_committed"]        = num(df, ATP_ALIASES, "so_committed")
            out["do_committed"]        = num(df, ATP_ALIASES, "do_committed")
            out["on_floor_total"]      = num(df, ATP_ALIASES, "on_floor")
            out["total_atp"]           = num(df, ATP_ALIASES, "atp")
            out["convert_to_pl"]       = num(df, ATP_ALIASES, "convert_pl")
            out["uom"]                 = df[find_col(df, ATP_ALIASES, "uom")] if find_col(df, ATP_ALIASES, "uom") else "CS"

            out = out[out["material_code"].str.match(r"\d{5,}", na=False)]
            out.insert(0, "atp_id", range(1, len(out) + 1))
            out.to_csv(PROCESSED / "atp_stock.csv", index=False)
            print(f"[06] Wrote {len(out):,} ATP rows → atp_stock.csv")
        else:
            print("[WARN] Could not parse ATP Stock sheet")
            pd.DataFrame().to_csv(PROCESSED / "atp_stock.csv", index=False)
    else:
        print("[WARN] ATP Stock sheet not found")
        pd.DataFrame().to_csv(PROCESSED / "atp_stock.csv", index=False)


if __name__ == "__main__":
    run()

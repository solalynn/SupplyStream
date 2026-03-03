"""Central configuration for the Supply Chain ETL pipeline.
Suntory PepsiCo Thailand — Supply Stream Dashboard
"""
from pathlib import Path

ROOT        = Path(__file__).parent.parent
RAW_DIR     = ROOT / "data" / "raw"
PROCESSED   = ROOT / "data" / "processed"
API_DIR     = ROOT / "data" / "api"
DB_PATH     = ROOT / "supply_chain.db"

# ── Source file paths ──────────────────────────────────────────────────────────
RAW_FILES = {
    "product_info":   RAW_DIR / "Product_Information_Update.xlsx",
    "inventory":      RAW_DIR / "Inventory_2_Mar_2026.xlsx",
    "outputs":        RAW_DIR / "Outputs_128_Feb_2026.xlsx",
    "demand_part1":   RAW_DIR / "Demand_Supply Part 1.xlsx",
    "demand_part2":   RAW_DIR / "Demand_Supply Part 2.xlsx",
    "wh_utilization": RAW_DIR / "WH_Utilization.xlsx",
}

# ── Expected sheet names ───────────────────────────────────────────────────────
SHEETS = {
    "d_weekly":     "D-Weekly",
    "p_weekly":     "P-Weekly",
    "d_daily":      "D-Daily",
    "p_daily":      "P-Daily",
    "d_monthly":    "D-Monthly",
    "p_monthly":    "P-Monthly",
    "inv_scp":      "Inv-SCP",
    "outputs_7101": "Outputs 7101",
    "outputs_7102": "Outputs 7102",
    "parameter":    "Parameter",
    "atp_stock":    "ATP Stock",
    "wh_space":     "WH Space",
    "ry_line_load": "RY Line Load",
    "sr_line_load": "SR Line Load",
}

# ── Plants ─────────────────────────────────────────────────────────────────────
PLANTS = {
    "7101": {"plant_id": 1, "name": "Rayong",      "short": "RY", "type": "OWN",     "lines": 9,  "lead_days": 2},
    "7102": {"plant_id": 2, "name": "Saraburi",    "short": "SR", "type": "OWN",     "lines": 5,  "lead_days": 2},
    "7104": {"plant_id": 3, "name": "Siam Water",  "short": "SW", "type": "CO-PACK", "lines": 0,  "lead_days": 5},
    "7106": {"plant_id": 4, "name": "Prime Water", "short": "PW", "type": "CO-PACK", "lines": 0,  "lead_days": 5},
}

SNAPSHOT_DATE = "2026-03-02"

# ── Week labels w01..w52 ───────────────────────────────────────────────────────
WEEK_LABELS = [f"w{i:02d}" for i in range(1, 53)]

# ── Demand type mapping (as they appear in source files) ──────────────────────
DEMAND_TYPE_MAP = {
    "ibp":        "IBP",
    "pre":        "PRE_FCST",
    "pre fcst":   "PRE_FCST",
    "pre-fcst":   "PRE_FCST",
    "pre_fcst":   "PRE_FCST",
    "post":       "POST_FCST",
    "post fcst":  "POST_FCST",
    "post-fcst":  "POST_FCST",
    "post_fcst":  "POST_FCST",
    "actual":     "ACTUAL",
    "billing":    "BILLING",
}

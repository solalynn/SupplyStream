"""generate_sample_data.py
Creates realistic sample Excel files in data/raw/ so the ETL pipeline runs end-to-end.
Run once: python generate_sample_data.py
"""
import random, math
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

random.seed(42)
np.random.seed(42)

RAW = Path(__file__).parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

# ── ISO W01-2026 = Dec 29 2025 ────────────────────────────────────────────────
W01 = date(2025, 12, 29)
WEEKS = [(W01 + timedelta(weeks=i)).isoformat() for i in range(52)]
WLABELS = [f"w{i+1:02d}" for i in range(52)]          # w01..w52
SNAPSHOT_WK = 9                                         # index of 2-Mar-2026 week

SEASON = [1.00,1.00,1.00,1.00, 1.10,1.10,1.10,1.10,   # Jan-Feb (w01-w08)
          1.25,1.30,1.30,1.35, 1.35,1.30,1.30,1.25,   # Mar-Apr  (w09-w16)
          1.20,1.20,1.15,1.10, 1.05,0.98,0.95,0.93,   # May-Jun  (w17-w24)
          0.90,0.90,0.90,0.90, 0.92,0.92,0.93,0.93,   # Jul-Aug  (w25-w32)
          0.95,0.98,1.00,1.00, 1.05,1.08,1.10,1.10,   # Sep-Oct  (w33-w40)
          1.15,1.18,1.20,1.22, 1.28,1.30,1.32,1.35]   # Nov-Dec  (w41-w52)

# ── SKU master ────────────────────────────────────────────────────────────────
SKUS = [
  dict(mat="32000001",desc="PEPSI PET 1.25L P12",       brand="PEPSI",        pack="PET",flavor="Cola",        ml=1250,upc=12,cpl=60,ph=1.90,sl=180,plant="7101",d=9800, s=45000),
  dict(mat="32000002",desc="PEPSI PET 600ml P24",        brand="PEPSI",        pack="PET",flavor="Cola",        ml=600, upc=24,cpl=72,ph=1.85,sl=180,plant="7101",d=7200, s=32000),
  dict(mat="32000003",desc="PEPSI PET 2L P6",            brand="PEPSI",        pack="PET",flavor="Cola",        ml=2000,upc=6, cpl=40,ph=2.10,sl=180,plant="7101",d=3800, s=14000),
  dict(mat="32000004",desc="PEPSI CAN 325ml P24",        brand="PEPSI",        pack="CAN",flavor="Cola",        ml=325, upc=24,cpl=80,ph=1.60,sl=365,plant="7101",d=8300, s=38000),
  dict(mat="32000005",desc="PEPSI ZERO PET 600ml P24",   brand="PEPSI",        pack="PET",flavor="Cola Zero",   ml=600, upc=24,cpl=72,ph=1.85,sl=180,plant="7101",d=5500, s=22000),
  dict(mat="32000006",desc="7UP PET 1.25L P12",          brand="7UP",          pack="PET",flavor="Lemon Lime",  ml=1250,upc=12,cpl=60,ph=1.90,sl=180,plant="7101",d=6500, s=28000),
  dict(mat="32000007",desc="7UP PET 600ml P24",          brand="7UP",          pack="PET",flavor="Lemon Lime",  ml=600, upc=24,cpl=72,ph=1.85,sl=180,plant="7101",d=5200, s=21000),
  dict(mat="32000008",desc="7UP CAN 325ml P24",          brand="7UP",          pack="CAN",flavor="Lemon Lime",  ml=325, upc=24,cpl=80,ph=1.60,sl=365,plant="7101",d=4200, s=17500),
  dict(mat="32000009",desc="MIRINDA ORA PET 600ml P24",  brand="MIRINDA",      pack="PET",flavor="Orange",      ml=600, upc=24,cpl=72,ph=1.85,sl=180,plant="7102",d=5100, s=21000),
  dict(mat="32000010",desc="MIRINDA ORA PET 1.25L P12",  brand="MIRINDA",      pack="PET",flavor="Orange",      ml=1250,upc=12,cpl=60,ph=1.90,sl=180,plant="7102",d=4300, s=18000),
  dict(mat="32000011",desc="MIRINDA GRP PET 600ml P24",  brand="MIRINDA",      pack="PET",flavor="Grape",       ml=600, upc=24,cpl=72,ph=1.85,sl=180,plant="7102",d=3600, s=14000),
  dict(mat="32000012",desc="STING PET 330ml P24",        brand="STING",        pack="PET",flavor="Energy",      ml=330, upc=24,cpl=72,ph=1.85,sl=270,plant="7102",d=4800, s=19000),
  dict(mat="32000013",desc="MTN DEW PET 600ml P24",      brand="MOUNTAIN DEW", pack="PET",flavor="Citrus",      ml=600, upc=24,cpl=72,ph=1.85,sl=180,plant="7101",d=4100, s=16000),
  dict(mat="32000014",desc="MTN DEW CAN 325ml P24",      brand="MOUNTAIN DEW", pack="CAN",flavor="Citrus",      ml=325, upc=24,cpl=80,ph=1.60,sl=365,plant="7101",d=3200, s=12000),
  dict(mat="32000015",desc="BOSS GOLD CAN 180ml P24",    brand="BOSS",         pack="CAN",flavor="Coffee",      ml=180, upc=24,cpl=80,ph=1.60,sl=365,plant="7101",d=4600, s=18000),
  dict(mat="32000016",desc="BOSS LATTE CAN 190ml P24",   brand="BOSS",         pack="CAN",flavor="Latte",       ml=190, upc=24,cpl=80,ph=1.60,sl=365,plant="7101",d=3800, s=15000),
  dict(mat="32000017",desc="PEPSI CAN 250ml P24",        brand="PEPSI",        pack="CAN",flavor="Cola",        ml=250, upc=24,cpl=80,ph=1.60,sl=365,plant="7101",d=3500, s=13000),
  dict(mat="32000018",desc="7UP CAN 250ml P24",          brand="7UP",          pack="CAN",flavor="Lemon Lime",  ml=250, upc=24,cpl=80,ph=1.60,sl=365,plant="7102",d=2800, s=10000),
  dict(mat="32000019",desc="MIRINDA WTRMLN PET 600ml P24",brand="MIRINDA",     pack="PET",flavor="Watermelon",  ml=600, upc=24,cpl=72,ph=1.85,sl=180,plant="7102",d=2900, s=11000),
  dict(mat="32000020",desc="STING GOLD CAN 250ml P24",   brand="STING",        pack="CAN",flavor="Energy Gold", ml=250, upc=24,cpl=80,ph=1.60,sl=270,plant="7102",d=2600, s=9500),
]

def sf(i): return SEASON[i] if i < len(SEASON) else 1.0

def dem_wk(sku, i, noise=0.08):
    return max(0, round(sku['d'] * sf(i) * (1 + np.random.uniform(-noise, noise))))

def sup_wk(sku, i, cycle=3):
    seed_offset = int(sku['mat'][-2:])
    if (i + seed_offset) % cycle == 0:
        mult = 2.0 + np.random.uniform(0.3, 1.5)
        return max(0, round(sku['d'] * sf(i) * mult))
    return 0

# ─────────────────────────────────────────────────────────────────────────────
# 1. Product_Information_Update.xlsx
# ─────────────────────────────────────────────────────────────────────────────
print("[GEN] Product_Information_Update.xlsx ...")
factor_8oz = {  # RC-cases → 8oz-equivalent factor
    1250:  5.35, 600: 2.54, 2000: 8.47, 325: 1.38, 330: 1.40,
    250:   1.06, 180: 0.76, 190: 0.81,
}
rows = []
for sku in SKUS:
    f8 = factor_8oz.get(sku['ml'], round(sku['ml'] / 236.6, 2))
    rows.append({
        "Material":            sku['mat'],
        "Material Description":sku['desc'],
        "Brand":               sku['brand'],
        "Pack Type":           sku['pack'],
        "Flavor":              sku['flavor'],
        "Net Content (ml)":    sku['ml'],
        "Unit per Case":       sku['upc'],
        "Cases per Pallet":    sku['cpl'],
        "Pallet Height (m)":   sku['ph'],
        "Shelf Life (days)":   sku['sl'],
        "Tray Type":           "Tray" if sku['pack'] == "PET" else "Wrap",
        "Case Gross Weight":   round(sku['ml'] * sku['upc'] / 1000 * 1.05, 2),
        "Std RC 8oz":          f8,
        "Dual Plant":          "N",
        "Plant":               sku['plant'],
        "Status":              "Active",
        "Prod Hierarchy":      f"TH-{sku['brand'][:3]}-{sku['pack']}-{sku['ml']}",
        "Hier L1":             "THAILAND",
        "Hier L2":             sku['brand'],
        "Hier L3":             sku['pack'],
        "Hier L4":             sku['flavor'],
        "Hier L5":             f"{sku['ml']}ml",
        "Hier L6":             f"P{sku['upc']}",
        "Hier L7":             sku['plant'],
    })
pd.DataFrame(rows).to_excel(RAW / "Product_Information_Update.xlsx", index=False)
print(f"  → {len(rows)} SKUs")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Inventory_2_Mar_2026.xlsx  (Inv-SCP sheet)
# ─────────────────────────────────────────────────────────────────────────────
print("[GEN] Inventory_2_Mar_2026.xlsx ...")
inv_rows = []
for sku in SKUS:
    total = sku['s']
    unr   = round(total * np.random.uniform(0.62, 0.72))
    qual  = round(total * np.random.uniform(0.04, 0.08))
    blk   = round(total * np.random.uniform(0.00, 0.03))
    trn   = total - unr - qual - blk
    inv_rows.append({
        "Plant":                 sku['plant'],
        "Material":              sku['mat'],
        "Material Description":  sku['desc'],
        "Unrestricted":          unr,
        "Quality Inspection":    qual,
        "Blocked":               blk,
        "In Transit":            trn,
        "Total":                 total,
        "UoM":                   "CS",
    })

with pd.ExcelWriter(RAW / "Inventory_2_Mar_2026.xlsx", engine="openpyxl") as w:
    pd.DataFrame(inv_rows).to_excel(w, sheet_name="Inv-SCP", index=False)
print(f"  → {len(inv_rows)} stock rows")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Outputs_128_Feb_2026.xlsx  (Outputs 7101, Outputs 7102)
# ─────────────────────────────────────────────────────────────────────────────
print("[GEN] Outputs_128_Feb_2026.xlsx ...")
order_seq = 1000001
RY_LINES = ["LINE2-1","LINE2-2","LINE2-3","LINE2-5","LINE2-6"]
SR_LINES = ["LINE3-1","LINE3-2","LINE3-3"]
PLANT_LINES = {"7101": RY_LINES, "7102": SR_LINES}
SHIFTS = ["A", "B", "C"]
STATUSES = ["CNF", "TECO"]

def make_orders(plant_code):
    global order_seq
    orders = []
    plant_skus = [s for s in SKUS if s['plant'] == plant_code]
    lines = PLANT_LINES[plant_code]
    for sku in plant_skus:
        cycle = 3 if sku['d'] > 6000 else 4
        seed_off = int(sku['mat'][-2:])
        for wi in range(SNAPSHOT_WK):   # W01-W09 (historical)
            if (wi + seed_off) % cycle != 0:
                continue
            post_date = date.fromisoformat(WEEKS[wi]) + timedelta(days=np.random.randint(1, 5))
            planned = max(0, round(sku['d'] * sf(wi) * (2.0 + np.random.uniform(0.3, 1.5))))
            gr = round(planned * np.random.uniform(0.96, 1.00))
            orders.append({
                "Order":              order_seq,
                "Material":           sku['mat'],
                "Description":        sku['desc'],
                "Plant":              plant_code,
                "Production Version": f"V{np.random.randint(1,4):02d}",
                "Posting Date":       post_date.isoformat(),
                "Planned Qty":        planned,
                "GR Qty":             gr,
                "SCV Qty":            planned - gr,
                "UoM":                "CS",
                "Line":               np.random.choice(lines),
                "Shift":              np.random.choice(SHIFTS),
                "MRP Controller":     f"TH{np.random.randint(10,30)}",
                "Status":             np.random.choice(STATUSES, p=[0.2, 0.8]),
                "Batch":              f"B{order_seq:07d}",
                "Pallet":             "",
            })
            order_seq += 1
    return orders

with pd.ExcelWriter(RAW / "Outputs_128_Feb_2026.xlsx", engine="openpyxl") as w:
    orders_ry = make_orders("7101")
    orders_sr = make_orders("7102")
    pd.DataFrame(orders_ry).to_excel(w, sheet_name="Outputs 7101", index=False)
    pd.DataFrame(orders_sr).to_excel(w, sheet_name="Outputs 7102", index=False)
print(f"  → {len(orders_ry)} RY orders, {len(orders_sr)} SR orders")

# ─────────────────────────────────────────────────────────────────────────────
# 4 & 5. Demand_Supply Part 1.xlsx + Part 2.xlsx
#         sheets: D-Weekly, P-Weekly, Parameter
# ─────────────────────────────────────────────────────────────────────────────
print("[GEN] Demand_Supply Part 1 & 2.xlsx ...")

DEMAND_TYPES = [
    ("ibp",       1.04, 0.03),   # type label, bias, noise
    ("Post Fcst", 1.00, 0.04),
    ("Actual",    0.98, 0.06),   # only for historical weeks
]

def build_demand_sheet(sku_subset):
    rows = []
    for sku in sku_subset:
        for dtype, bias, noise in DEMAND_TYPES:
            row = {"Material": sku['mat'], "Description": sku['desc'],
                   "Plant": sku['plant'], "Type": dtype}
            for i, wl in enumerate(WLABELS):
                if dtype == "Actual" and i > SNAPSHOT_WK:
                    row[wl] = ""
                else:
                    v = max(0, round(sku['d'] * sf(i) * bias * (1 + np.random.uniform(-noise, noise))))
                    row[wl] = v
            rows.append(row)
    return pd.DataFrame(rows)

def build_supply_sheet(sku_subset):
    rows = []
    for sku in sku_subset:
        row = {"Material": sku['mat'], "Description": sku['desc'], "Plant": sku['plant']}
        for i, wl in enumerate(WLABELS):
            row[wl] = sup_wk(sku, i)
        rows.append(row)
    return pd.DataFrame(rows)

def build_parameter_sheet(sku_subset):
    f8map = {1250:5.35,600:2.54,2000:8.47,325:1.38,330:1.40,250:1.06,180:0.76,190:0.81}
    rows = []
    for sku in sku_subset:
        f8 = f8map.get(sku['ml'], round(sku['ml']/236.6, 2))
        rows.append({
            "Material":          sku['mat'],
            "Transcode":         f"TH{sku['ml']:04d}",
            "Pack Size":         f"{sku['ml']}ml x {sku['upc']}",
            "Content (ml)":      sku['ml'],
            "Bottles per Case":  sku['upc'],
            "Cases per Pallet":  sku['cpl'],
            "Factor 8oz":        f8,
            "Factor RC to CV":   round(f8 * 0.355 / (sku['ml']/1000), 2),
            "Type":              sku['pack'],
        })
    return pd.DataFrame(rows)

part1_skus = SKUS[:10]
part2_skus = SKUS[10:]

with pd.ExcelWriter(RAW / "Demand_Supply Part 1.xlsx", engine="openpyxl") as w:
    build_demand_sheet(part1_skus).to_excel(w, sheet_name="D-Weekly",  index=False)
    build_supply_sheet(part1_skus).to_excel(w, sheet_name="P-Weekly",  index=False)
    build_parameter_sheet(part1_skus).to_excel(w, sheet_name="Parameter", index=False)

with pd.ExcelWriter(RAW / "Demand_Supply Part 2.xlsx", engine="openpyxl") as w:
    build_demand_sheet(part2_skus).to_excel(w, sheet_name="D-Weekly",  index=False)
    build_supply_sheet(part2_skus).to_excel(w, sheet_name="P-Weekly",  index=False)
    build_parameter_sheet(part2_skus).to_excel(w, sheet_name="Parameter", index=False)

print(f"  → Part1: {len(part1_skus)} SKUs × 3 demand types + supply + param")
print(f"  → Part2: {len(part2_skus)} SKUs × 3 demand types + supply + param")

# ─────────────────────────────────────────────────────────────────────────────
# 6. WH_Utilization.xlsx
#    sheets: WH Space, ATP Stock, RY Line Load, SR Line Load
# ─────────────────────────────────────────────────────────────────────────────
print("[GEN] WH_Utilization.xlsx ...")

# WH Space — 2 warehouses × 52 weeks
WH_LOCS = [
    {"loc": "RY-FG01", "cap": 8000, "plant": "7101", "base_util": 0.70},
    {"SR-FG02": None, "loc": "SR-FG02", "cap": 5500, "plant": "7102", "base_util": 0.65},
]
WH_LOCS = [
    {"loc": "RY-FG01", "cap": 8000,  "plant": "7101", "base": 0.70},
    {"loc": "SR-FG02", "cap": 5500,  "plant": "7102", "base": 0.65},
]

wh_rows = []
for wh in WH_LOCS:
    prev_end = round(wh['cap'] * wh['base'])
    for i, wl in enumerate(WLABELS):
        util = wh['base'] * sf(i) * (1 + np.random.uniform(-0.05, 0.05))
        util = min(0.98, util)
        end  = round(wh['cap'] * util)
        prod = round(end * 0.13 * (1 + np.random.uniform(-0.3, 0.3)))
        dem  = round(end * 0.14 * (1 + np.random.uniform(-0.3, 0.3)))
        beg  = max(0, end - prod + dem)
        wh_rows.append({
            "Week":           WEEKS[i],
            "Week Label":     wl,
            "Location":       wh['loc'],
            "Begin Pallets":  beg,
            "Production In":  prod,
            "Demand Out":     dem,
            "End Pallets":    end,
            "Capacity":       wh['cap'],
            "Utilization %":  round(util * 100, 1),
        })
        prev_end = end

# ATP Stock — snapshot at 2-Mar-2026
atp_rows = []
for sku in SKUS:
    total = sku['s']
    unr   = round(total * np.random.uniform(0.62, 0.72))
    so    = round(unr * np.random.uniform(0.08, 0.15))
    do_   = round(unr * np.random.uniform(0.05, 0.10))
    floor = round(total * np.random.uniform(0.05, 0.12))
    atp   = max(0, unr - so - do_)
    atp_rows.append({
        "Material":       sku['mat'],
        "Plant":          sku['plant'],
        "Unrestricted":   unr,
        "SO Committed":   so,
        "DO Committed":   do_,
        "On Floor Total": floor,
        "ATP":            atp,
        "Convert to PL":  round(atp / sku['cpl'], 1),
        "UoM":            "CS",
    })

# RY Line Load — W06 to W15 (10 weeks around snapshot)
def build_line_load(plant_code, lines, start_wi=5, n_weeks=10):
    rows = []
    for wi in range(start_wi, min(start_wi + n_weeks, 52)):
        wdate = date.fromisoformat(WEEKS[wi])
        for day_off in range(5):  # Mon-Fri
            plan_date = wdate + timedelta(days=day_off)
            for line in lines:
                load = min(1.10, 0.55 + sf(wi) * np.random.uniform(0.15, 0.35))
                rows.append({
                    "Date":           plan_date.isoformat(),
                    "Week":           WLABELS[wi],
                    "Line":           line,
                    "Volume RC":      round(sku['d'] * load * 0.9 for sku in SKUS if sku['plant'] == plant_code).__next__() if False else round(10000 * load * (1 + np.random.uniform(-0.2, 0.2))),
                    "Efficiency %":   round(min(99, 82 + np.random.uniform(-5, 10)), 1),
                    "Working Hours":  round(np.random.uniform(18, 23), 1),
                    "Load %":         round(load * 100, 1),
                })
    return rows

ry_ll = build_line_load("7101", RY_LINES)
sr_ll = build_line_load("7102", SR_LINES)

with pd.ExcelWriter(RAW / "WH_Utilization.xlsx", engine="openpyxl") as w:
    pd.DataFrame(wh_rows).to_excel(w, sheet_name="WH Space",     index=False)
    pd.DataFrame(atp_rows).to_excel(w, sheet_name="ATP Stock",    index=False)
    pd.DataFrame(ry_ll).to_excel(w,   sheet_name="RY Line Load",  index=False)
    pd.DataFrame(sr_ll).to_excel(w,   sheet_name="SR Line Load",  index=False)

print(f"  → WH Space: {len(wh_rows)} rows | ATP: {len(atp_rows)} | RY LL: {len(ry_ll)} | SR LL: {len(sr_ll)}")

print("\n[DONE] All sample files written to data/raw/")
print("       Next: python etl/run_all.py")

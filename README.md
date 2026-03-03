# Supply Chain Trading Chart Dashboard
### Suntory PepsiCo Thailand — SC Visualization Engine

---

## Project Overview

A financial-chart-style supply chain dashboard that applies OHLC candlestick conventions to operational beverage manufacturing data. Demand signals, production plans, inventory levels, and warehouse utilization are visualized as chart primitives (candlesticks, overlays, bands) rather than traditional tables.

**Design philosophy:** treat supply chain data the same way a trader reads a price chart — open/high/low/close for inventory, volume bars for production, signal overlays for demand forecasts, alert markers for risk events.

---

## Plant Reference

| SAP Code | Short | Name | Type | Lines |
|---|---|---|---|---|
| 7101 | RY | Rayong | OWN | 9 lines (MS1: Line1–9) |
| 7102 | SR | Saraburi | OWN | 5 lines (SS1: Line1–5) |
| 7104 | SW | Siam Water | OWN | Water lines |
| 7106 | PW | Prime Water | OWN | Water lines |
| — | CP | Co-packer | CO-PACK | External |

---

## Database Schema — v2.0

**13 tables · 170+ fields · 24 relationships**

### Dimension Tables (Master Data)

| Table | Source File | Key Fields | Rows (est.) |
|---|---|---|---|
| `SKU_MASTER` | Product_Information_Update.xlsx | material_code, brand, pack_type, hier_l1–l7, std_rc_8oz | 434 |
| `LOCATION_MASTER` | WH_Utilization → Demand vs Supply | location_code, location_type, capacity_pallets | ~20 |
| `SUPPLY_PLANNING_MASTER` | All files | plant_code, plant_name, plant_short, total_lines | 5 |
| `PRODUCTION_LINE` | RY/SR Line Load + NE Target sheets | line_code, line_type, bpm_capacity, ne_target_pct | 14 |
| `UNIT_CONVERSION` | Parameter sheet (Demand_Supply Part 1) | material_code, factor_8oz, bottles_per_case, cases_per_pallet | 200 |

### Fact Tables (Time-Series)

| Table | Source File / Sheet | Granularity | Key Metrics |
|---|---|---|---|
| `INVENTORY_OHLC` | Inv-SCP + WH Space | Daily / Weekly | stock_open/high/low/close + 4 stock categories |
| `DEMAND_SIGNAL` | D-Daily/Weekly/Monthly + IBP Demand | Day/Week/Month | volume_rc/8oz/cv · demand_type ENUM |
| `PRODUCTION_ORDER` | Outputs_128_Feb_2026 (7101, 7102) + MFBF | Order-level | planned_qty, gr_qty (actual GR), batch_no |
| `ATP_STOCK` | WH_Utilization → ATP Stock | Snapshot | unrestricted, so_committed, do_committed, total_atp |
| `WH_SPACE` | Demand_Supply Part 1 → WH Space | Weekly | begin_pallets, production_in, demand_out, end_pallets |

### Analytical Tables

| Table | Purpose |
|---|---|
| `SC_INDICATORS` | Computed: DOS 7d/30d, fill rate, NE actual, line load %, D/S ratio, risk score |
| `ALERT_EVENTS` | Triggered alerts: STOCKOUT / OVERSTOCK / LINE_DOWN / DS_GAP |
| `LINE_LOAD_PLAN` | Daily line load from RY/SR Line Load sheets — ACT vs PLAN |

---

## OHLC Mapping Convention

```
INVENTORY_OHLC                    WH_SPACE
──────────────                    ────────
O = stock_open   (begin)          O = begin_pallets
H = stock_high   (peak period)    H = production_in_pallets  
L = stock_low    (min period)     L = demand_out_pallets
C = stock_close  (end)            C = end_pallets
```

---

## Demand Signal Types

| ENUM Value | Source Sheet | Granularity | Description |
|---|---|---|---|
| `IBP` | IBP Demand | Weekly | IBP system forecast |
| `PRE_FCST` | DPre-Daily / DPre-Monthly | Day/Month | Pre-approved forecast |
| `POST_FCST` | Post-Fcst | Daily | Post-adjusted forecast |
| `ACTUAL` | D-Daily / D-Weekly / D-Monthly | Day/Week/Month | Actual demand |
| `BILLING` | Billing | Daily | Actual billing (invoiced) |

---

## Unit System

| Unit | Description | Conversion |
|---|---|---|
| RC | Raw cases (base unit in D/P sheets) | 1 RC = 1 physical case |
| CV | Cases volume | RC × bottles_per_case / std_units |
| SCV | Standard cases volume | Normalized standard unit |
| 8oz | 8oz equivalent | RC × factor_8oz (per SKU, from Parameter sheet) |
| EA | Each (individual unit) | Used in SAP production orders |

---

## Source Data Files

```
data/raw/
├── Product_Information_Update.xlsx    203 KB   434 SKUs · 44 columns
├── Inventory_2_Mar_2026.xlsx          709 KB   Current snapshot (Inv-SCP)
├── Outputs_128_Feb_2026.xlsx           13 MB   SAP prod orders 7101/7102
├── Inventory_Projection.xlsx          920 KB   Forward projection
├── 2026_Ph_I_Demand_Supply_Upd_20260225_SOE__Part_1.xlsx   27 MB
│   Sheets: D-Daily, D-Weekly, D-Monthly, P-Daily, P-Weekly, P-Monthly,
│           RY Line Load, SR Line Load, WH Space, Billing, NE Target,
│           Parameter, Pre-Fcst, Post-Fcst, IBP RPA, CO-PACKER
├── 2026_Ph_I_Demand_Supply_Upd_20260225_SOE__Part_2.xlsx    4.4 MB
│   Sheets: D-Daily(2), D-Weekly(2), D-Monthly(2), Post-Fcst(2),
│           P-Daily(2), P-Weekly(2), P-Monthly(2)
└── 2026_WH_Utilization_Template_AOT_01032026.xlsx           14 MB
    Sheets: Demand vs Supply forecast, P-Daily, IBP Demand,
            Inv-SCP, ATP Stock, MFBF, MB51, MB5T-Intrans., DO for shuttle
```

---

## Key Excel Structure Notes

> **Critical for ETL** — all D/P sheets use wide pivot format (SKU rows × date columns). Header is multi-row (row 1=title, row 2=ACT/PLAN flag, row 3=day name, row 4=week label, row 5/6=dates + field names). Always use `pd.read_excel(header=None)` and manually slice from row 6+.

```python
# Correct pattern for D-Weekly / P-Weekly:
import pandas as pd
df = pd.read_excel(file, sheet_name='D-Weekly', header=None)
# Row 5 (index 4) = week labels (w01, w02…)
# Row 6 (index 5) = dates + col headers (TYPE, FLAVOR, NET FILL, MATERIAL DESC.)
# Row 7+ = data
date_row = df.iloc[5, 5:]   # date columns start at col index 5
data = df.iloc[6:, :]
```

### Production Line Parameter (RY Line Load sheet rows 8+)
```
Col B = Line config (e.g. "Line 1PET 1000")
Col C = Bottle size (mm)
Col D = SKU name
Col E = SAP CODE (material number)
Col F = FILL (L)
Col G = BPM (bottles per minute)
Col H = BT/CS (bottles per case)
Col I = MOQ (min order qty)
```

### Inventory Snapshot (Inv-SCP)
```
Columns: Month, Date, Type, Pack Size, Flavor, Opening Stock, From Date,
         Material, Material Description, Plant, Plant Description,
         Unrestricted Stock, Quality Stock, Block Stock,
         Subcontract Stock, Stock In Transit,
         Total Planned Plant Stock, Total Physical Stock
```

---

## Recommended Build Order

### Phase 1 — ETL Pipeline
```
01_extract_sku_master.py          Product_Information_Update.xlsx
02_extract_unit_conversion.py     Demand_Supply Part 1 → Parameter sheet
03_extract_inventory_snapshot.py  Inventory_2_Mar_2026.xlsx → Inv-SCP
04_extract_production_orders.py   Outputs_128 → sheets 7101, 7102
05_extract_demand_weekly.py       Demand_Supply Part 1 → D-Weekly
06_extract_production_weekly.py   Demand_Supply Part 1 → P-Weekly
07_extract_wh_space.py            Demand_Supply Part 1 → WH Space
08_extract_atp_stock.py           WH_Utilization → ATP Stock
09_extract_line_load.py           Demand_Supply Part 1 → RY/SR Line Load
10_build_ohlc.py                  Combine inventory + production + demand → INVENTORY_OHLC
11_compute_indicators.py          Compute DOS, fill rate, risk score → SC_INDICATORS
```

### Phase 2 — Database
```
db/schema.sql                     CREATE TABLE statements (SQLite)
db/load.py                        Load all CSVs → supply_chain.db
db/validate.py                    FK integrity checks
```

### Phase 3 — Dashboard
```
dashboard/index.html              Main chart view
dashboard/js/candlestick.js       OHLC renderer (D3 or ECharts)
dashboard/js/demand_overlay.js    Multi-signal demand overlay
dashboard/js/filters.js           SKU / plant / date / granularity
dashboard/js/alerts.js            Alert event markers
```

---

## Dashboard Views (Planned)

| View | Chart Type | X-axis | Y-axis | Overlays |
|---|---|---|---|---|
| **Inventory OHLC** | Candlestick | Date (day/week) | Cases / Pallets | Demand signals, safety stock band |
| **D vs S Gap** | Bar + Line | Week | RC volume | IBP vs Post-Fcst vs Actual |
| **WH Utilization** | Area + Threshold | Week | Pallets % capacity | RY vs SR dual-panel |
| **Line Load** | Heatmap / Bar | Day | % load per line | ACT vs PLAN |
| **Production Burn** | Step chart | Date | GR qty cumulative | Plan vs Actual |

---

## Schema Reference

See `schema/supply_chain_db_schema_v2.html` for full interactive ER diagram.

---

## Tech Stack

| Layer | Tool |
|---|---|
| ETL | Python 3 + pandas + openpyxl |
| Database | SQLite (portable) / PostgreSQL (production) |
| Dashboard | HTML + D3.js or Apache ECharts |
| Schema viz | SVG/HTML (self-contained) |

---

*Last updated: 02 Mar 2026 · Schema v2.0 · Suntory PepsiCo Thailand*

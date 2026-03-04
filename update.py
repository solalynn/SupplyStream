"""update.py — Weekly supply chain refresh  (Excel → JSON)
=====================================================================
Drop new SAP exports into data/raw/, then run:

    python update.py

Weekly file checklist:
  ✓ Inventory_<D>_<Mon>_<YYYY>.xlsx   ← SAP inventory snapshot
  ✓ Outputs_<NNN>_<Mon>_<YYYY>.xlsx   ← SAP production orders
  ✓ Demand_Supply Part 1.xlsx          ← only if demand was re-forecasted
  ✓ Demand_Supply Part 2.xlsx          ← only if demand was re-forecasted
  ✓ WH_Utilization.xlsx                ← only if WH/line data changed
  ✓ Product_Information_Update.xlsx    ← only if SKU master changed

Output: data/api/*.json  (consumed by dashboard.html)
"""

import re, json
from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
RAW  = ROOT / "data" / "raw"
API  = ROOT / "data" / "api"

# ── Week grid ───────────────────────────────────────────────────────────────
# Update W01 each planning year.  W01-2026 = Mon 29 Dec 2025.
W01     = date(2025, 12, 29)
N_WK    = 52
WLABELS = [f"w{i+1:02d}" for i in range(N_WK)]           # "w01".."w52"
WDATES  = [(W01 + timedelta(weeks=i)).isoformat() for i in range(N_WK)]
WDIDX   = {d: i for i, d in enumerate(WDATES)}            # date → 0-based index

# ── Plant definitions ───────────────────────────────────────────────────────
PLANTS = {
    "7101": {"plant_id": 1, "short": "RY", "name": "Rayong"},
    "7102": {"plant_id": 2, "short": "SR", "name": "Saraburi"},
    "7104": {"plant_id": 3, "short": "SW", "name": "Siam Water"},
    "7106": {"plant_id": 4, "short": "PW", "name": "Prime Water"},
}

DEMAND_TYPES = {
    "ibp": "IBP",
    "pre": "PRE_FCST",   "pre fcst": "PRE_FCST",  "pre-fcst": "PRE_FCST",
    "post": "POST_FCST", "post fcst": "POST_FCST", "post-fcst": "POST_FCST",
    "actual": "ACTUAL",  "billing": "BILLING",
}

# ── All column aliases in one place ─────────────────────────────────────────
A = {
    # SKU master
    "material":      ["material", "material no", "material number", "mat.", "mat no", "matl", "material code"],
    "desc":          ["material description", "description", "desc", "eng desc", "material desc"],
    "brand":         ["brand", "brand name", "trademark"],
    "pack":          ["pack type", "pack", "packaging"],
    "ml":            ["net content (ml)", "net content", "content ml", "net_content_ml"],
    "upc":           ["unit per case", "units per case", "pcs/case", "bot/case", "qty per case"],
    "cpl":           ["cases per pallet", "case/pallet", "cases/pallet", "cs/pl"],
    "shelf_life":    ["shelf life", "shelf life (days)", "shelf life days"],
    "std_rc_8oz":    ["std rc 8oz", "8oz factor", "rc to 8oz", "8oz conv"],
    "is_active":     ["active", "is active", "is_active", "status"],
    "plant":         ["plant", "plant code", "plant_code", "producing plant"],
    # Inventory
    "unrestricted":  ["unrestricted", "unrestricted stock", "unrestd", "free stock", "unrestricted (cs)"],
    "quality":       ["quality inspection", "qual. insp.", "quality", "qm stock", "qual insp"],
    "blocked":       ["blocked", "blocked stock", "block"],
    "in_transit":    ["stock in transit", "in transit", "transit", "intransit"],
    "total_stock":   ["total", "total stock", "total (cs)"],
    "uom":           ["base unit", "uom", "unit", "base unit of measure"],
    # Demand
    "demand_type":   ["type", "demand type", "forecast type", "stream", "category", "fcst type"],
    # Production orders
    "posting_date":  ["posting date", "date", "prod date"],
    "planned_qty":   ["planned qty", "planned quantity", "plan qty", "target qty"],
    "gr_qty":        ["gr qty", "goods receipt", "confirmed qty"],
    "scv_qty":       ["scv qty", "scv", "variance"],
    "order_status":  ["status", "order status", "sys. status"],
    "prod_version":  ["production version", "prod version", "ver"],
    "shift":         ["shift", "shift code"],
    "mrp_ctrl":      ["mrp controller", "mrp ctrl"],
    "line":          ["line", "line code", "prod line", "line no"],
    # WH Space
    "wk_date":       ["week", "week date", "week start", "period date", "period"],
    "wk_label":      ["week label", "week no", "wk"],
    "location":      ["location", "location code", "warehouse", "wh"],
    "begin_pl":      ["begin pallets", "begin", "opening pallets", "start pallets"],
    "prod_in_pl":    ["production in", "prod in", "inbound pallets"],
    "demand_out_pl": ["demand out", "demand out pallets", "outbound pallets"],
    "end_pl":        ["end pallets", "end", "closing pallets"],
    "capacity_pl":   ["capacity", "capacity pallets", "max pallets"],
    "util_pct":      ["utilization", "util %", "utilization %", "occupancy"],
    # ATP Stock
    "so_commit":     ["so committed", "sales order committed", "so"],
    "do_commit":     ["do committed", "delivery order committed", "do"],
    "on_floor":      ["on floor", "on floor total", "floor stock"],
    "atp":           ["atp", "total atp", "available to promise"],
    "convert_pl":    ["convert to pl", "pallet qty", "pl qty"],
    # Line Load
    "volume_rc":     ["volume", "volume rc", "volume (rc)", "rc", "quantity rc"],
    "efficiency":    ["efficiency", "eff %", "efficiency %", "ne %", "ne"],
    "hours":         ["working hours", "hours", "run hours"],
    "load_pct":      ["load %", "load", "loading %"],
    "record_type":   ["record type", "plan type"],
}

# ── Shared helpers ──────────────────────────────────────────────────────────

def col(df, key):
    """Return first column name that matches any alias for key, or None."""
    lmap = {c.lower().strip(): c for c in df.columns}
    for alias in A.get(key, []):
        if alias.lower() in lmap:
            return lmap[alias.lower()]
    return None


def find_sheet(xf, *candidates):
    """Return first matching sheet name, or None."""
    lmap = {s.lower(): s for s in xf.sheet_names}
    for c in candidates:
        if c.lower() in lmap:
            return lmap[c.lower()]
    return None


def read_sheet(xf, sheet, key_col):
    """Try header rows 0/1/2 until key_col is found; return cleaned DataFrame."""
    if sheet is None:
        return None
    for hr in [0, 1, 2]:
        df = pd.read_excel(xf, sheet_name=sheet, header=hr, dtype=str)
        df = df.dropna(how="all").map(lambda x: x.strip() if isinstance(x, str) else x)
        if col(df, key_col):
            return df
    return None


def week_cols(df):
    """Return [(col_name, iso_date, week_label)] for columns matching w01..w52."""
    out = []
    for c in df.columns:
        m = re.search(r"\bw\s*0*(\d{1,2})\b", str(c), re.IGNORECASE)
        if m:
            i = int(m.group(1)) - 1
            if 0 <= i < N_WK:
                out.append((c, WDATES[i], WLABELS[i]))
    return out


def n(val):
    """Safe numeric parse."""
    try:
        return float(str(val).replace(",", ""))
    except:
        return 0.0


def norm_type(t):
    s = str(t).lower().strip()
    for k, v in DEMAND_TYPES.items():
        if k in s:
            return v
    return str(t).upper().strip()


def latest_file(pattern):
    """Latest file in RAW matching glob pattern, or None."""
    matches = sorted(RAW.glob(pattern))
    return matches[-1] if matches else None


def parse_snap_date(path):
    """Parse date from filenames like Inventory_2_Mar_2026.xlsx."""
    months = dict(jan=1,feb=2,mar=3,apr=4,may=5,jun=6,jul=7,aug=8,sep=9,oct=10,nov=11,dec=12)
    m = re.search(r"(\d{1,2})[_\s]+([a-z]{3})[_\s]+(\d{4})", path.stem.lower())
    if m:
        try:
            return date(int(m.group(3)), months[m.group(2)], int(m.group(1)))
        except Exception:
            pass
    return date.today()


def write_json(name, data):
    path = API / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str, ensure_ascii=False)
    n_rows = len(data) if isinstance(data, list) else "-"
    kb = path.stat().st_size / 1024
    print(f"  {name:<32} {str(n_rows):>6} rows  {kb:>7.1f} KB")


# ── Read functions ──────────────────────────────────────────────────────────

def read_skus():
    src = RAW / "Product_Information_Update.xlsx"
    if not src.exists():
        print(f"  [WARN] {src.name} not found — no SKUs loaded")
        return []
    xf  = pd.ExcelFile(src)
    df  = read_sheet(xf, xf.sheet_names[0], "material")
    if df is None:
        return []
    skus = []
    for _, row in df.iterrows():
        mat = str(row[col(df, "material")]).strip()
        if not re.match(r"\d{5,}", mat):
            continue
        pc = str(row[col(df, "plant")] or "7101").strip() if col(df, "plant") else "7101"
        p  = PLANTS.get(pc, {"plant_id": 0, "short": pc, "name": pc})
        active_raw = str(row[col(df, "is_active")] or "").strip().upper() if col(df, "is_active") else "Y"
        is_active  = active_raw not in ("N", "NO", "0", "FALSE", "INACTIVE", "X")
        skus.append({
            "sku_id":          len(skus) + 1,
            "material_code":   mat,
            "name":            str(row[col(df, "desc")] or mat).strip() if col(df, "desc") else mat,
            "brand":           str(row[col(df, "brand")] or "").strip() if col(df, "brand") else "",
            "pack_type":       str(row[col(df, "pack")] or "PET").strip() if col(df, "pack") else "PET",
            "plant_code":      pc,
            "plant_id":        p["plant_id"],
            "plant_short":     p["short"],
            "plant_name":      p["name"],
            "std_rc_8oz":      n(row[col(df, "std_rc_8oz")]) if col(df, "std_rc_8oz") else 1.0,
            "cases_per_pallet":int(n(row[col(df, "cpl")])) if col(df, "cpl") else 60,
            "is_active":       is_active,
        })
    return skus


def read_inventory():
    """Return ({(material, plant): {total, unrestricted}}, snap_date_str)."""
    src = latest_file("Inventory*.xlsx")
    if not src:
        print("  [WARN] No Inventory*.xlsx found")
        return {}, WDATES[9]
    snap_date = parse_snap_date(src).isoformat()
    xf   = pd.ExcelFile(src)
    sheet = find_sheet(xf, "Inv-SCP", "inventory", "stock", "inv scp") or xf.sheet_names[0]
    df   = read_sheet(xf, sheet, "material")
    if df is None:
        return {}, snap_date
    inv = {}
    for _, row in df.iterrows():
        mat = str(row[col(df, "material")]).strip()
        if not re.match(r"\d{5,}", mat):
            continue
        pc  = str(row[col(df, "plant")] or "7101").strip() if col(df, "plant") else "7101"
        unr = n(row[col(df, "unrestricted")]) if col(df, "unrestricted") else 0
        tot = n(row[col(df, "total_stock")]) if col(df, "total_stock") else (
              unr + n(row[col(df, "quality")]) + n(row[col(df, "blocked")]) + n(row[col(df, "in_transit")]))
        inv[(mat, pc)] = {"total": tot, "unrestricted": unr}
    return inv, snap_date


def read_demand_supply():
    """Return (demand_rows, supply_rows) from Demand_Supply Part 1 & 2."""
    demand, supply = [], []
    for fname in ["Demand_Supply Part 1.xlsx", "Demand_Supply Part 2.xlsx"]:
        src = RAW / fname
        if not src.exists():
            continue
        xf = pd.ExcelFile(src)

        # — Demand (D-Weekly) —
        d_sheet = find_sheet(xf, "D-Weekly", "D_Weekly", "Demand Weekly", "Weekly Demand")
        df = read_sheet(xf, d_sheet, "material")
        if df is not None:
            type_c = col(df, "demand_type")
            for _, row in df.iterrows():
                mat = str(row[col(df, "material")]).strip()
                if not re.match(r"\d{5,}", mat):
                    continue
                pc = str(row[col(df, "plant")] or "").strip() if col(df, "plant") else ""
                dt = norm_type(row[type_c]) if type_c else "POST_FCST"
                for wc, wdate, wlbl in week_cols(df):
                    v = row[wc]
                    if pd.isna(v) or str(v).strip() in ("", "-", "N/A"):
                        continue
                    demand.append({"mat": mat, "plant": pc, "time": wdate,
                                   "week_label": wlbl, "demand_type": dt, "value": n(v)})

        # — Supply Plan (P-Weekly) —
        p_sheet = find_sheet(xf, "P-Weekly", "P_Weekly", "Supply Weekly")
        df = read_sheet(xf, p_sheet, "material")
        if df is not None:
            for _, row in df.iterrows():
                mat = str(row[col(df, "material")]).strip()
                if not re.match(r"\d{5,}", mat):
                    continue
                pc = str(row[col(df, "plant")] or "").strip() if col(df, "plant") else ""
                for wc, wdate, wlbl in week_cols(df):
                    v = row[wc]
                    if pd.isna(v) or str(v).strip() in ("", "-", "N/A"):
                        continue
                    supply.append({"mat": mat, "plant": pc, "time": wdate, "value": n(v)})
    return demand, supply


def read_production(sku_by_mat):
    src = latest_file("Outputs*.xlsx")
    if not src:
        print("  [WARN] No Outputs*.xlsx found")
        return []
    xf, orders = pd.ExcelFile(src), []
    for sheet in xf.sheet_names:
        pm = re.search(r"(\d{4})", sheet)
        sheet_plant = pm.group(1) if pm else None
        df = read_sheet(xf, sheet, "material")
        if df is None:
            continue
        pc = sheet_plant
        date_c  = col(df, "posting_date")
        plant_c = col(df, "plant")
        for _, row in df.iterrows():
            mat = str(row[col(df, "material")]).strip()
            if not re.match(r"\d{5,}", mat):
                continue
            sku = sku_by_mat.get(mat, {})
            rpc = pc or (str(row[plant_c]).strip() if plant_c else "")
            orders.append({
                "prod_order_id":    len(orders) + 1,
                "sku_id":           sku.get("sku_id"),
                "plant_id":         PLANTS.get(rpc, {}).get("plant_id"),
                "time":             str(row[date_c]).strip() if date_c else "",
                "planned_qty":      n(row[col(df, "planned_qty")]) if col(df, "planned_qty") else 0,
                "gr_qty":           n(row[col(df, "gr_qty")])      if col(df, "gr_qty")      else 0,
                "production_version": str(row[col(df, "prod_version")] or "").strip() if col(df, "prod_version") else "",
                "line_code":        str(row[col(df, "line")] or "").strip()        if col(df, "line")         else "",
                "order_status":     str(row[col(df, "order_status")] or "").strip() if col(df, "order_status") else "",
            })
    return orders


def read_wh_utilization():
    """Return (wh_rows, atp_rows, lineload_rows)."""
    src = RAW / "WH_Utilization.xlsx"
    if not src.exists():
        print("  [WARN] WH_Utilization.xlsx not found")
        return [], [], []
    xf = pd.ExcelFile(src)

    # WH Space
    wh_rows = []
    df = read_sheet(xf, find_sheet(xf, "WH Space", "wh space", "warehouse"), "begin_pl")
    if df is not None:
        for i, row in df.iterrows():
            cap  = n(row[col(df, "capacity_pl")]) if col(df, "capacity_pl") else 0
            end  = n(row[col(df, "end_pl")])       if col(df, "end_pl")      else 0
            util_c = col(df, "util_pct")
            util = n(row[util_c]) if util_c else (round(end / cap * 100, 1) if cap else 0)
            wh_rows.append({
                "wh_space_id":           i + 1,
                "location_code":         str(row[col(df, "location")] or "").strip() if col(df, "location") else "",
                "time":                  str(row[col(df, "wk_date")] or "").strip()  if col(df, "wk_date")  else "",
                "week_label":            str(row[col(df, "wk_label")] or "").strip() if col(df, "wk_label") else "",
                "begin_pallets":         n(row[col(df, "begin_pl")]),
                "production_in_pallets": n(row[col(df, "prod_in_pl")])    if col(df, "prod_in_pl")    else 0,
                "demand_out_pallets":    n(row[col(df, "demand_out_pl")])  if col(df, "demand_out_pl") else 0,
                "end_pallets":           end,
                "capacity_pallets":      cap,
                "utilization_pct":       util,
            })

    # ATP Stock
    atp_rows = []
    df = read_sheet(xf, find_sheet(xf, "ATP Stock", "atp stock", "atp"), "material")
    if df is not None:
        for i, row in df.iterrows():
            mat = str(row[col(df, "material")]).strip()
            if not re.match(r"\d{5,}", mat):
                continue
            pc = str(row[col(df, "plant")] or "").strip() if col(df, "plant") else ""
            p  = PLANTS.get(pc, {"plant_id": 0, "short": pc})
            atp_rows.append({
                "atp_id":             i + 1,
                "material_code":      mat,
                "plant_code":         pc,
                "plant_id":           p["plant_id"],
                "plant_short":        p["short"],
                "unrestricted_stock": n(row[col(df, "unrestricted")]) if col(df, "unrestricted") else 0,
                "so_committed":       n(row[col(df, "so_commit")])     if col(df, "so_commit")    else 0,
                "do_committed":       n(row[col(df, "do_commit")])     if col(df, "do_commit")    else 0,
                "on_floor_total":     n(row[col(df, "on_floor")])      if col(df, "on_floor")     else 0,
                "total_atp":          n(row[col(df, "atp")])           if col(df, "atp")          else 0,
                "convert_to_pl":      n(row[col(df, "convert_pl")])    if col(df, "convert_pl")   else 0,
                "uom":                str(row[col(df, "uom")] or "CS").strip() if col(df, "uom")  else "CS",
            })

    # Line Load (RY + SR)
    ll_rows = []
    for plant_code, *sheet_cands in [("7101", "RY Line Load", "ry line load"),
                                      ("7102", "SR Line Load", "sr line load")]:
        sheet = find_sheet(xf, *sheet_cands)
        if not sheet:
            continue
        df = read_sheet(xf, sheet, "line")
        if df is None:
            continue
        p    = PLANTS.get(plant_code, {"plant_id": 0, "short": plant_code})
        dc   = col(df, "posting_date") or col(df, "wk_date")
        wlc  = col(df, "wk_label")
        for _, row in df.iterrows():
            ll_rows.append({
                "load_id":       len(ll_rows) + 1,
                "plant_id":      p["plant_id"],
                "plant_short":   p["short"],
                "line_code":     str(row[col(df, "line")] or "").strip()        if col(df, "line")        else "",
                "time":          str(row[dc] or "").strip()                      if dc                     else "",
                "week_label":    str(row[wlc] or "").strip()                     if wlc                    else "",
                "record_type":   str(row[col(df, "record_type")] or "PLAN").strip() if col(df, "record_type") else "PLAN",
                "volume_load_rc":n(row[col(df, "volume_rc")])  if col(df, "volume_rc") else 0,
                "efficiency_pct":n(row[col(df, "efficiency")]) if col(df, "efficiency") else 0,
                "working_hours": n(row[col(df, "hours")])       if col(df, "hours")      else 0,
                "load_pct":      n(row[col(df, "load_pct")])    if col(df, "load_pct")   else 0,
            })

    return wh_rows, atp_rows, ll_rows


# ── Compute OHLC ────────────────────────────────────────────────────────────

def compute_ohlc(skus, inventory, demand_rows, supply_rows, snap_date):
    """52-week OHLC inventory time-series, anchored at the snapshot date."""
    snap_wk = WDIDX.get(snap_date, 9)

    # Best demand per (mat, plant, week): ACTUAL > POST_FCST > IBP > other
    PRIORITY = {"ACTUAL": 3, "POST_FCST": 2, "IBP": 1}
    dem = {}   # {(mat, plant or ""): {wk_idx: value}}
    for r in demand_rows:
        key = (r["mat"], r["plant"])
        wi  = WDIDX.get(r["time"])
        if wi is None:
            continue
        bucket = dem.setdefault(key, {})
        pnew   = PRIORITY.get(r["demand_type"], 0)
        pold   = bucket.get(wi, (0, -1))[1]
        if pnew >= pold:
            bucket[wi] = (r["value"], pnew)
    dem = {k: {wi: v for wi, (v, _) in d.items()} for k, d in dem.items()}

    sup = {}   # {(mat, plant or ""): {wk_idx: value}}
    for r in supply_rows:
        key = (r["mat"], r["plant"])
        wi  = WDIDX.get(r["time"])
        if wi is None:
            continue
        sup.setdefault(key, {})[wi] = sup.get(key, {}).get(wi, 0) + r["value"]

    def get_d(mat, pc, wi):
        return dem.get((mat, pc), dem.get((mat, ""), {})).get(wi, 0)

    def get_s(mat, pc, wi):
        return sup.get((mat, pc), sup.get((mat, ""), {})).get(wi, 0)

    ohlc = []
    for sku in skus:
        mat, pc = sku["material_code"], sku["plant_code"]
        sid, pid = sku["sku_id"], sku["plant_id"]
        inv_data = inventory.get((mat, pc), {})
        snap_stock = float(inv_data.get("total", 0))
        snap_unr   = float(inv_data.get("unrestricted", snap_stock * 0.65))

        # Build close[w] for all 52 weeks, anchored at snap_wk
        close = [0.0] * N_WK
        close[snap_wk] = snap_stock

        for w in range(snap_wk + 1, N_WK):          # forward
            close[w] = max(0.0, close[w-1] + get_s(mat, pc, w) - get_d(mat, pc, w))
        for w in range(snap_wk - 1, -1, -1):        # backward
            close[w] = max(0.0, close[w+1] - get_s(mat, pc, w+1) + get_d(mat, pc, w+1))

        for w in range(N_WK):
            o = close[w-1] if w > 0 else close[0]
            d = get_d(mat, pc, w)
            s = get_s(mat, pc, w)
            ohlc.append({
                "sku_id":            sid,
                "plant_id":          pid,
                "time":              WDATES[w],
                "week_label":        WLABELS[w],
                "open":              round(o),
                "high":              round(max(o, close[w])),
                "low":               round(max(0, min(o, close[w]))),
                "close":             round(close[w]),
                "units_produced":    round(s),
                "units_consumed":    round(d),
                "stockout_flag":     1 if close[w] <= 0 else 0,
                "unrestricted_stock":round(snap_unr) if w == snap_wk else round(close[w] * 0.65),
                "unit_type":         "RC",
            })
    return ohlc


# ── Compute KPIs & Alerts ───────────────────────────────────────────────────

def compute_kpis(skus, ohlc, snap_date):
    snap = [r for r in ohlc if r["time"] == snap_date]
    stock  = sum(r["close"]          for r in snap)
    demand = sum(r["units_consumed"] for r in snap)
    supply = sum(r["units_produced"] for r in snap)
    return {
        "total_skus":       len([s for s in skus if s["is_active"]]),
        "total_plants":     len(set(s["plant_code"] for s in skus)),
        "current_stock_rc": round(stock),
        "weekly_demand_rc": round(demand),
        "weekly_supply_rc": round(supply),
        "stockouts_count":  sum(r["stockout_flag"] for r in snap),
        "active_sku_count": len(snap),
        "dos":              round(stock / max(demand / 7, 1), 1),
        "snapshot_date":    snap_date,
    }


def compute_alerts(skus, ohlc, snap_date):
    by_id  = {s["sku_id"]: s for s in skus}
    snap   = {r["sku_id"]: r for r in ohlc if r["time"] == snap_date}
    alerts = []
    for sid, row in snap.items():
        sku  = by_id.get(sid, {})
        wdem = max(row["units_consumed"], 1)
        dos7 = round(row["close"] / wdem * 7,  1)
        dos30= round(row["close"] / wdem * 30, 1)
        alerts.append({
            "material_code":    sku.get("material_code", ""),
            "name":             sku.get("name", ""),
            "brand":            sku.get("brand", ""),
            "plant_short":      sku.get("plant_short", ""),
            "dos_7d":           dos7,
            "dos_30d":          dos30,
            "demand_vs_supply": round(row["units_produced"] / wdem, 2),
            "reorder_signal":   1 if dos7 < 14 else 0,
            "period_date":      snap_date,
        })
    return sorted(alerts, key=lambda x: x["dos_7d"])[:20]


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    import time
    t0 = time.time()
    print("=" * 60)
    print(" SUPPLY STREAM — weekly refresh")
    print("=" * 60)

    # 1. Read all sources
    print("\n[READ]")
    skus              = read_skus()
    inventory, snap_date = read_inventory()
    demand_rows, supply_rows = read_demand_supply()
    wh_rows, atp_rows, ll_rows = read_wh_utilization()

    print(f"  SKUs: {len(skus)} | Inventory: {len(inventory)} | "
          f"Demand: {len(demand_rows)} | Supply: {len(supply_rows)}")
    print(f"  Snapshot date: {snap_date}")

    # 2. Build lookups
    sku_by_mat = {s["material_code"]: s for s in skus}
    orders     = read_production(sku_by_mat)

    # 3. Enrich & compute
    print("\n[COMPUTE]")
    ohlc = compute_ohlc(skus, inventory, demand_rows, supply_rows, snap_date)
    print(f"  OHLC rows: {len(ohlc)}")

    # Enrich demand rows with sku_id (for dashboard filtering)
    demand_json = []
    for r in demand_rows:
        sku = sku_by_mat.get(r["mat"])
        if sku:
            demand_json.append({"sku_id": sku["sku_id"], "time": r["time"],
                                 "demand_type": r["demand_type"],
                                 "week_label":  r["week_label"],
                                 "value":       r["value"]})

    # Enrich ATP rows with SKU info
    for r in atp_rows:
        sku = sku_by_mat.get(r["material_code"], {})
        r["sku_id"]       = sku.get("sku_id")
        r["name"]         = sku.get("name", r["material_code"])
        r["brand"]        = sku.get("brand", "")
        r["snapshot_date"]= snap_date

    kpis   = compute_kpis(skus, ohlc, snap_date)
    alerts = compute_alerts(skus, ohlc, snap_date)

    # 4. Write JSON
    print("\n[WRITE]  data/api/")
    write_json("sku_list.json",          [s for s in skus if s.pop("is_active", True) or True])
    write_json("inventory_ohlc.json",    ohlc)
    write_json("demand_signal.json",     demand_json)
    write_json("production_orders.json", orders)
    write_json("wh_space.json",          wh_rows)
    write_json("line_load.json",         ll_rows)
    write_json("atp_stock.json",         atp_rows)
    write_json("kpis.json",              kpis)
    write_json("alerts.json",            alerts)

    print(f"\n{'='*60}")
    print(f" Done in {time.time()-t0:.1f}s — open dashboard.html")
    print(f" (serve with: python -m http.server 8000)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

# sdwis_ca_report.py
import sys
import re
import requests
import pandas as pd
import functools


# Optional: silence HTTPS warnings if your env uses unverified TLS
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from docx import Document
    HAVE_DOCX = True
except Exception:
    HAVE_DOCX = False

# ----------------------- Config -----------------------

# Fields to keep per table (UPPERCASE after fetch)
TABLE_FIELDS = {
    "WATER_SYSTEM": [
        "PWSID", "PWS_ACTIVITY_CODE", "PWS_TYPE_CODE", "PWS_NAME",
        "POPULATION_SERVED_COUNT", "PRIMARY_SOURCE_CODE", "OWNER_TYPE_CODE",
        "GW_SW_CODE", "IS_GRANT_ELIGIBLE_IND", "IS_WHOLESALER_IND",
        "SERVICE_CONNECTIONS_COUNT", "ORG_NAME", "ADMIN_NAME", "EMAIL_ADDR",
        "STATE_CODE", "CITY_NAME"
    ],
    "GEOGRAPHIC_AREA": [
        "PWSID", "TRIBAL_CODE", "STATE_SERVED", "CITY_SERVED", "COUNTY_SERVED"
    ],
    "VIOLATION": [
        "PWSID", "CONTAMINANT_CODE", "VIOLATION_CODE", "VIOLATION_CATEGORY_CODE",
        "IS_HEALTH_BASED_IND", "COMPLIANCE_STATUS_CODE", "RULE_GROUP_CODE"
    ],
    "WATER_SYSTEM_FACILITY": [
        "PWSID", "FACILITY_ID", "FACILITY_NAME", "STATE_FACILITY_ID",
        "FACILITY_ACTIVITY_CODE", "FACILITY_TYPE_CODE", "IS_SOURCE_IND",
        "WATER_TYPE_CODE", "AVAILABILITY_CODE"
    ],
    "SERVICE_AREA": [
        "PWSID", "SELLER_TREATMENT_CODE", "SELLER_PWSID", "SELLER_PWS_NAME",
        "IS_SOURCE_TREATED_IND", "SERVICE_AREA_TYPE_CODE", "IS_PRIMARY_SERVICE_AREA_CODE"
    ],
    "TREATMENT": [
        "COMMENTS_TEXT", "FACILITY_ID", "PWSID", "TREATMENT_ID",
        "TREATMENT_OBJECTIVE_CODE", "TREATMENT_PROCESS_CODE"
    ]
}

# Descriptions for common codes (expand as needed)
CODE_DESCRIPTIONS = {
    "PWS_ACTIVITY_CODE": {"A": "Active", "I": "Inactive"},
    "PRIMARY_SOURCE_CODE": {"GW": "Ground Water", "SW": "Surface Water", "GU": "GW under influence"},
    "GW_SW_CODE": {"GW": "Ground Water", "SW": "Surface Water", "GU": "GW under influence"},
    "OWNER_TYPE_CODE": {"P": "Private", "M": "Municipal", "S": "State", "F": "Federal", "N": "Non-Transient Non-Community"},
    "PWS_TYPE_CODE": {"C": "Community", "NTNC": "Non-Transient Non-Community", "NC": "Non-Community"},
    "IS_WHOLESALER_IND": {"Y": "Yes", "N": "No"},
    "IS_GRANT_ELIGIBLE_IND": {"Y": "Yes", "N": "No"},
    "IS_HEALTH_BASED_IND": {"Y": "Yes", "N": "No"},
    "COMPLIANCE_STATUS_CODE": {"S": "In Compliance", "V": "In Violation"},
}

# Paging for local pulls (bump if needed)
PAGE_SIZE = 50000
MAX_PAGES = 10   # up to ~500k rows per table

BASE = "https://data.epa.gov/efservice"
PWSID_URL = BASE + "/{table}/PWSID/{pwsid}/JSON"
ROWS_URL  = BASE + "/{table}/Rows/{start}:{end}/JSON"
FILTER_URL = BASE + "/{table}/{col}/{val}/Rows/{start}:{end}/JSON"

# ----------------------- HTTP helpers -----------------------

# (Optional) persistent session w/ small retry pool
_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

def api_get_json(url: str):
    """GET with verify True then fallback to False (common in corp envs)."""
    try:
        r = _session.get(url, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception:
        r = _session.get(url, timeout=60, verify=False)
        r.raise_for_status()
        return r.json()

def df_upper(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [c.upper() for c in out.columns]
    return out

def pull_rows_paged(table: str, page_size: int = PAGE_SIZE, max_pages: int = MAX_PAGES) -> pd.DataFrame:
    parts = []
    for p in range(max_pages):
        start = p * page_size
        end = start + page_size
        url = ROWS_URL.format(table=table, start=start, end=end)
        try:
            data = api_get_json(url)
            if not isinstance(data, list) or not data:
                break
            parts.append(pd.DataFrame(data))
        except Exception as e:
            print(f"[Rows {table} {start}:{end}] {e}")
            break
    if not parts:
        return pd.DataFrame()
    return df_upper(pd.concat(parts, ignore_index=True))

def pull_rows_filtered(table: str, col: str, val: str,
                       page_size: int = PAGE_SIZE, max_pages: int = MAX_PAGES) -> pd.DataFrame:
    """Pull rows from EPA API with a server-side filter to reduce data size."""
    parts = []
    for p in range(max_pages):
        start = p * page_size
        end = start + page_size
        url = FILTER_URL.format(table=table, col=col, val=val, start=start, end=end)
        try:
            data = api_get_json(url)
            if not isinstance(data, list) or not data:
                break
            parts.append(pd.DataFrame(data))
        except Exception as e:
            print(f"[Rows {table} {start}:{end}] {e}")
            break
    if not parts:
        return pd.DataFrame()
    return df_upper(pd.concat(parts, ignore_index=True))

def looks_like_pwsid(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]{2}\d{7}", s.strip()))

def token_and_contains(series: pd.Series, tokens: list[str]) -> pd.Series:
    """AND all tokens; case-insensitive; safe on NaN."""
    mask = pd.Series(True, index=series.index)
    for t in tokens:
        if t:
            mask &= series.astype(str).str.contains(re.escape(t), case=False, na=False)
    return mask

# ------- Lightweight caches for CLI --------

@functools.lru_cache(maxsize=64)
def _ws_by_state_cached(state_code: str) -> pd.DataFrame:
    """WATER_SYSTEM filtered by state, UPPER-cased columns."""
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", state_code)
    return df_upper(ws)

@functools.lru_cache(maxsize=1)
def _ga_all_cached() -> pd.DataFrame:
    """Bulk GEOGRAPHIC_AREA once; keep common columns; UPPER-cased."""
    ga = pull_rows_paged("GEOGRAPHIC_AREA")
    ga = df_upper(ga)
    keep = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED"] if c in ga.columns]
    ga = ga[keep] if keep else ga
    subset = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga.columns]
    return ga.drop_duplicates(subset=subset) if subset else ga.drop_duplicates()


# ----------------------- Search (state-aware) -----------------------

def search_by_name(state_code: str, name_query: str, county_filter: str | None) -> pd.DataFrame:
    """
    Fast CLI search:
      - WATER_SYSTEM filtered by STATE_CODE (server-side)  [cached]
      - GEOGRAPHIC_AREA pulled once and filtered locally   [cached]
      - Optional name token filter (AND across tokens)
      - Optional county/city filter against GA.COUNTY_SERVED or GA.CITY_SERVED
      - Returns a compact table with PWSID, PWS_NAME, CITY (prefers WS.CITY_NAME, else GA.CITY_SERVED), COUNTY_SERVED
    """
    sc = (state_code or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{2}", sc):
        return pd.DataFrame()

    # 1) Base WATER_SYSTEM (by state)
    ws = _ws_by_state_cached(sc)
    if ws.empty or "PWSID" not in ws.columns or "PWS_NAME" not in ws.columns:
        return pd.DataFrame()

    # Keep only what we need (include CITY_NAME if present)
    keep_ws = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in ws.columns]
    ws = ws[keep_ws].drop_duplicates("PWSID") if keep_ws else ws

    # 2) Optional name filter (AND across tokens)
    q = (name_query or "").strip()
    if q and "PWS_NAME" in ws.columns:
        tokens = re.findall(r"[A-Za-z0-9]+", q)
        if tokens:
            m = token_and_contains(ws["PWS_NAME"], tokens)
            ws = ws[m]
            if ws.empty:
                return pd.DataFrame()

    # 3) If no county/city filter → return early (fast)
    if not county_filter:
        out = ws.copy()
        # Build CITY from CITY_NAME if present
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        show = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not show:
            show = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[show].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # 4) County/city filter → do a single bulk GA filter & local join
    ga = _ga_all_cached()
    if ga.empty or "PWSID" not in ga.columns:
        # No GA available, fall back to WS-only
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        show = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not show:
            show = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[show].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # Filter GA to the state via PWSID prefix (robust even if STATE_SERVED missing)
    ga_state = ga[ga["PWSID"].astype(str).str.startswith(sc)]

    term = county_filter.strip().lower()
    # Use Series(False, index=ga_state.index) when the column is missing
    m_county = (
        ga_state["COUNTY_SERVED"].astype(str).str.lower().str.contains(term, na=False)
        if "COUNTY_SERVED" in ga_state.columns
        else pd.Series(False, index=ga_state.index)
    )
    m_citysv = (
        ga_state["CITY_SERVED"].astype(str).str.lower().str.contains(term, na=False)
        if "CITY_SERVED" in ga_state.columns
        else pd.Series(False, index=ga_state.index)
    )
    ga_match = ga_state[m_county | m_citysv]

    if ga_match.empty:
        # Nothing matched in GA; fall back to WS-only
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        show = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not show:
            show = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[show].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # Join WS with GA matches to keep only systems in the county/city selection
    merge_cols = ["PWSID", "CITY_SERVED", "COUNTY_SERVED"]
    merge_cols = [c for c in merge_cols if c in ga_match.columns]
    out = ws.merge(ga_match[merge_cols], on="PWSID", how="inner")

    # Build unified CITY column: prefer WS.CITY_NAME, else GA.CITY_SERVED
    if "CITY" not in out.columns:
        out["CITY"] = ""
    if "CITY_NAME" in out.columns:
        out["CITY"] = out["CITY"].mask(out["CITY"].eq(""), out["CITY_NAME"].fillna("").astype(str).str.strip())
    if "CITY_SERVED" in out.columns:
        out["CITY"] = out["CITY"].mask(out["CITY"].eq(""), out["CITY_SERVED"].fillna("").astype(str).str.strip())

    # Final view
    show = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in out.columns]
    if not show:
        show = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
    return out[show].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

# ----------------------- Fetch tables & report -----------------------

def fetch_table_by_pwsid(table: str, pwsid: str) -> pd.DataFrame:
    url = PWSID_URL.format(table=table, pwsid=pwsid)
    try:
        data = api_get_json(url)
        df = pd.DataFrame(data) if isinstance(data, list) and data else pd.DataFrame()
        return df_upper(df)
    except Exception as e:
        print(f"[Fetch {table}] {e}")
        return pd.DataFrame()

def fetch_all_selected(pwsid: str) -> dict[str, pd.DataFrame]:
    out = {}
    for table, wanted in TABLE_FIELDS.items():
        print(f"Fetching: {table}")
        df = fetch_table_by_pwsid(table, pwsid)
        if not df.empty:
            keep = [c for c in wanted if c in df.columns]
            if keep:
                df = df[keep]
        out[table] = df
    return out

def add_code_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col, mapping in CODE_DESCRIPTIONS.items():
        if col in out.columns:
            out[col + "_DESC"] = out[col].map(mapping).fillna("")
    return out

def generate_report(pwsid: str, data: dict[str, pd.DataFrame], out_path: str | None = None):
    """
    Build a structured SDWIS Word report:

    Summary Information for Water Utility [pwsid]
    USEPA Safe Drinking Water Information System (SDWIS)
    Water System Name: ...
    System Type / Activity Status / Ownership
    State / County Served
    Administrative Contact / Email
    Population Served / Service Connections
    Primary Source / Wholesale Supplier?

    Facilities
      Sources
      Treatment
      Storage

    Violations
      Health Based
      Non-Health Based
    """
    if not HAVE_DOCX:
        print("python-docx is not installed. Install it with: pip install python-docx")
        sys.exit(1)

    # --- helpers -------------------------------------------------------------
    def u(df: pd.DataFrame) -> pd.DataFrame:
        return df_upper(df)

    def get1(df: pd.DataFrame, col: str, default: str = "N/A") -> str:
        if df.empty or col not in df.columns:
            return default
        v = str(df.iloc[0].get(col, "")).strip()
        return v if v else default

    def desc(col: str, val: str) -> str:
        """Return 'CODE — Description' if we know a mapping; else just the value."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "N/A"
        s = str(val).strip()
        mapping = CODE_DESCRIPTIONS.get(col, {})
        d = mapping.get(s, "")
        return f"{s} — {d}" if d else s

    def yn_from(code: str | None) -> str:
        if code is None or (isinstance(code, float) and pd.isna(code)):
            return "N/A"
        s = str(code).strip().upper()
        return {"Y": "Yes", "N": "No"}.get(s, s or "N/A")

    def active_from(code: str | None) -> str:
        s = ("" if code is None else str(code).strip().upper())
        return {"A": "Yes", "I": "No"}.get(s, s or "N/A")

    def add_table(doc, headers: list[str], rows: list[list[str]]):
        t = doc.add_table(rows=1, cols=len(headers))
        # Be defensive about style (some environments don't have "Table Grid")
        try:
            t.style = "Table Grid"
        except Exception:
            pass
        for j, h in enumerate(headers):
            t.cell(0, j).text = str(h)
        for r in rows:
            cells = t.add_row().cells
            for j, val in enumerate(r):
                cells[j].text = "" if val is None or (isinstance(val, float) and pd.isna(val)) else str(val)

    # --- source tables -------------------------------------------------------
    ws = u(data.get("WATER_SYSTEM", pd.DataFrame()))
    ga = u(data.get("GEOGRAPHIC_AREA", pd.DataFrame()))
    wsf = u(data.get("WATER_SYSTEM_FACILITY", pd.DataFrame()))
    vio = u(data.get("VIOLATION", pd.DataFrame()))
    trt = u(data.get("TREATMENT", pd.DataFrame()))

    # --- summary fields ------------------------------------------------------
    ws_name = get1(ws, "PWS_NAME")
    pws_type = desc("PWS_TYPE_CODE", get1(ws, "PWS_TYPE_CODE"))
    pws_activity = desc("PWS_ACTIVITY_CODE", get1(ws, "PWS_ACTIVITY_CODE"))
    owner = desc("OWNER_TYPE_CODE", get1(ws, "OWNER_TYPE_CODE"))
    state = get1(ws, "STATE_CODE", pwsid[:2] if isinstance(pwsid, str) else "N/A")

    # County Served: prefer GA.COUNTY_SERVED if present (first non-empty)
    county = "N/A"
    if not ga.empty and "COUNTY_SERVED" in ga.columns:
        non_empty = ga["COUNTY_SERVED"].dropna().astype(str).str.strip()
        if not non_empty.empty:
            county = non_empty.iloc[0] or "N/A"

    admin = get1(ws, "ADMIN_NAME")
    email = get1(ws, "EMAIL_ADDR")
    pop = get1(ws, "POPULATION_SERVED_COUNT")
    svc_conn = get1(ws, "SERVICE_CONNECTIONS_COUNT")
    primary_src = desc("PRIMARY_SOURCE_CODE", get1(ws, "PRIMARY_SOURCE_CODE"))
    wholesaler = yn_from(get1(ws, "IS_WHOLESALER_IND"))

    # --- document build ------------------------------------------------------
    doc = Document()
    doc.add_heading(f"Summary Information for Water Utility {pwsid}", level=0)
    doc.add_paragraph("USEPA Safe Drinking Water Information System (SDWIS)")

    # Summary block
    doc.add_paragraph(f"Water System Name: {ws_name}")
    doc.add_paragraph(
        f"System Type: {pws_type}    "
        f"Activity Status: {pws_activity}    "
        f"Ownership: {owner}"
    )
    doc.add_paragraph(f"State: {state}    County Served: {county}")
    doc.add_paragraph(f"Administrative Contact: {admin}    Email address: {email}")
    doc.add_paragraph(f"Population Served: {pop}    Service Connections: {svc_conn}")
    doc.add_paragraph(f"Primary Source: {primary_src}    Wholesale Supplier to Other PWS’s: {wholesaler}")

    # --- Facilities ----------------------------------------------------------
    doc.add_heading("Facilities", level=1)

    # Sources (from WATER_SYSTEM_FACILITY where IS_SOURCE_IND == 'Y')
    doc.add_paragraph("Sources")
    source_rows = []
    if not wsf.empty:
        # Normalize columns we rely on
        for col in ["IS_SOURCE_IND", "FACILITY_ACTIVITY_CODE", "FACILITY_TYPE_CODE", "WATER_TYPE_CODE", "AVAILABILITY_CODE"]:
            if col in wsf.columns:
                wsf[col] = wsf[col].astype(str)
        mask_src = wsf["IS_SOURCE_IND"].astype(str).str.upper().eq("Y") if "IS_SOURCE_IND" in wsf.columns else pd.Series(False, index=wsf.index)
        src_df = wsf[mask_src] if "IS_SOURCE_IND" in wsf.columns else pd.DataFrame()
        if not src_df.empty:
            for _, r in src_df.iterrows():
                source_rows.append([
                    r.get("FACILITY_TYPE_CODE", ""),
                    active_from(r.get("FACILITY_ACTIVITY_CODE", "")),
                    r.get("FACILITY_NAME", ""),
                    r.get("FACILITY_ID", ""),
                    r.get("STATE_FACILITY_ID", ""),
                    r.get("WATER_TYPE_CODE", ""),
                    r.get("AVAILABILITY_CODE", ""),
                ])
    if source_rows:
        add_table(doc,
                  headers=["Type", "Active?", "Name", "SDWIS Facility ID", "State Facility ID", "Water Type", "Availability"],
                  rows=source_rows)
    else:
        doc.add_paragraph("No data available.")

    # Treatment (from TREATMENT table)
    doc.add_paragraph("")  # small spacer
    doc.add_paragraph("Treatment")
    tr_rows = []
    if not trt.empty:
        # Expect: COMMENTS_TEXT, FACILITY_ID, PWSID, TREATMENT_ID, TREATMENT_OBJECTIVE_CODE, TREATMENT_PROCESS_CODE
        # We'll merge facility info to show Name/State Facility ID if possible
        fac_cols = ["FACILITY_ID", "FACILITY_NAME", "STATE_FACILITY_ID", "FACILITY_ACTIVITY_CODE"]
        fac_min = wsf[fac_cols].drop_duplicates("FACILITY_ID") if not wsf.empty and "FACILITY_ID" in wsf.columns else pd.DataFrame()
        td = trt.copy()
        if not fac_min.empty:
            td = td.merge(fac_min, on="FACILITY_ID", how="left")
        for _, r in td.iterrows():
            tr_rows.append([
                r.get("FACILITY_NAME", ""),
                active_from(r.get("FACILITY_ACTIVITY_CODE", "")),
                r.get("FACILITY_ID", ""),
                r.get("STATE_FACILITY_ID", ""),
                r.get("TREATMENT_OBJECTIVE_CODE", ""),  # you can map codes later if you add a description dict
                r.get("TREATMENT_PROCESS_CODE", ""),
            ])
    if tr_rows:
        add_table(doc,
                  headers=["Name", "Active?", "SDWIS Facility ID", "State Facility ID", "Treatment Objective", "Treatment Process"],
                  rows=tr_rows)
    else:
        doc.add_paragraph("No data available.")

    # Storage (from WATER_SYSTEM_FACILITY where FACILITY_TYPE_CODE contains 'STORAGE')
    doc.add_paragraph("")  # small spacer
    doc.add_paragraph("Storage")
    stor_rows = []
    if not wsf.empty and "FACILITY_TYPE_CODE" in wsf.columns:
        storage_mask = wsf["FACILITY_TYPE_CODE"].astype(str).str.contains("STORAGE", case=False, na=False)
        stor_df = wsf[storage_mask]
        for _, r in stor_df.iterrows():
            stor_rows.append([
                r.get("FACILITY_NAME", ""),
                active_from(r.get("FACILITY_ACTIVITY_CODE", "")),
                r.get("FACILITY_ID", ""),
                r.get("STATE_FACILITY_ID", ""),
            ])
    if stor_rows:
        add_table(doc,
                  headers=["Name", "Active?", "SDWIS Facility ID", "State Facility ID"],
                  rows=stor_rows)
    else:
        doc.add_paragraph("No data available.")

    # --- Violations ----------------------------------------------------------
    doc.add_heading("Violations", level=1)

    def build_vio_rows(df: pd.DataFrame) -> list[list[str]]:
        rows = []
        for _, r in df.iterrows():
            rows.append([
                r.get("VIOLATION_CATEGORY_CODE", ""),
                r.get("VIOLATION_CODE", ""),
                r.get("CONTAMINANT_CODE", ""),
            ])
        return rows

    # Health Based
    doc.add_paragraph("Health Based")
    hb_rows = []
    if not vio.empty and "IS_HEALTH_BASED_IND" in vio.columns:
        hb = vio[vio["IS_HEALTH_BASED_IND"].astype(str).str.upper().eq("Y")]
        hb_rows = build_vio_rows(hb)
    if hb_rows:
        add_table(doc, headers=["Category", "Type", "Contaminant"], rows=hb_rows)
    else:
        doc.add_paragraph("No data available.")

    # Non-Health Based
    doc.add_paragraph("")  # spacer
    doc.add_paragraph("Non-Health Based")
    nh_rows = []
    if not vio.empty and "IS_HEALTH_BASED_IND" in vio.columns:
        nh = vio[vio["IS_HEALTH_BASED_IND"].astype(str).str.upper().eq("N")]
        nh_rows = build_vio_rows(nh)
    if nh_rows:
        add_table(doc, headers=["Category", "Type", "Contaminant"], rows=nh_rows)
    else:
        doc.add_paragraph("No data available.")

    # --- finalize ------------------------------------------------------------
    if out_path is None:
        out_path = f"{pwsid}_SDWIS_Report.docx"
    doc.save(out_path)
    print(f"Report saved: {out_path}")
    return out_path


# ----------------------- Main (state-agnostic CLI) -----------------------

def main():
    print("SDWIS Finder (state-aware)")
    print("  • Enter a PWSID (e.g., CA1010016), OR")
    print("  • Enter a 2-letter STATE code (e.g., CA), then (optional) name tokens and/or county")
    query = input("\nSearch (PWSID or STATE code): ").strip()

    if looks_like_pwsid(query):
        pwsid = query.upper()
    else:
        sc = query.strip().upper()
        if not re.fullmatch(r"[A-Z]{2}", sc):
            print("Please enter a valid PWSID or a 2-letter state code (e.g., CA, TX, NY).")
            sys.exit(0)

        name = input("Water system name (optional): ").strip()
        county_hint = input("County or City (optional): ").strip() or None
        print(f"\nSearching {sc}…")
        matches = search_by_name(sc, name_query=name, county_filter=county_hint)
        if matches.empty:
            print("No matches found. Try different words or adjust county filter.")
            sys.exit(0)

        # Show matches
        display = matches.reset_index(drop=True)
        display = display.reset_index().rename(columns={"index": "#"})
        print("\nMatches:")
        print(display.to_string(index=False))

        # Pick one
        try:
            pick = int(input("\nEnter the # of the system to use: ").strip())
            pwsid = matches.iloc[pick]["PWSID"]
        except Exception:
            print("Invalid selection.")
            sys.exit(1)

    print(f"\nFetching selected fields for PWSID: {pwsid}")
    all_data = fetch_all_selected(pwsid)

    if not HAVE_DOCX:
        print("Skipping Word report because python-docx is not installed.")
        sys.exit(0)

    generate_report(pwsid, all_data)

if __name__ == "__main__":
    main()

# sdwis_ca_report.py
import sys
import re
import requests
import pandas as pd

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
        "STATE_CODE"
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

# ----------------------- Search (state-aware) -----------------------

def search_by_name(state_code: str, name_query: str, county_filter: str | None) -> pd.DataFrame:
    """
    Generic search by STATE_CODE first, then optional name tokens and county.
    - Pull WATER_SYSTEM filtered by STATE_CODE (server-side).
    - If name is blank, return all systems for the state (optionally filter by county).
    - If county given, fetch GEOGRAPHIC_AREA per-candidate PWSID to match county (no STATE_SERVED dependency).
    """
    sc = (state_code or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{2}", sc):
        return pd.DataFrame()

    q = (name_query or "").strip()
    tokens = re.findall(r"[A-Za-z0-9]+", q) if q else []

    # 1) WATER_SYSTEM filtered by state
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", sc)
    print(f"[DEBUG] Pulled WATER_SYSTEM ({sc}): {len(ws)}")
    if ws.empty or "PWSID" not in ws.columns or "PWS_NAME" not in ws.columns:
        return pd.DataFrame()

    # 2) Name filtering (optional)
    if tokens:
        m = token_and_contains(ws["PWS_NAME"], tokens)
        ws = ws[m]
        print(f"[DEBUG] Name token matches ({sc}): {len(ws)}")
        if ws.empty:
            return pd.DataFrame()

    # If no county filter, return WS matches now
    if not county_filter:
        show_cols = [c for c in ["PWSID", "PWS_NAME"] if c in ws.columns]
        if not show_cols:
            show_cols = ["PWSID"]
        return ws[show_cols].drop_duplicates("PWSID").reset_index(drop=True)

    # 3) County filter given -> fetch GA per candidate (no STATE_SERVED filter)
    c = county_filter.strip().upper()
    rows = []
    cand_ws = ws[["PWSID", "PWS_NAME"]].drop_duplicates("PWSID")
    for _, r in cand_ws.iterrows():
        pwsid = r["PWSID"]
        ga_df = fetch_table_by_pwsid("GEOGRAPHIC_AREA", pwsid)
        ga_df = df_upper(ga_df)
        county = next((str(x).strip() for x in ga_df.get("COUNTY_SERVED", pd.Series()).dropna().astype(str) if x.strip()), "")
        city   = next((str(x).strip() for x in ga_df.get("CITY_SERVED",   pd.Series()).dropna().astype(str) if x.strip()), "")
        if county and c in county.upper():
            rows.append({"PWSID": pwsid, "PWS_NAME": r["PWS_NAME"], "CITY_SERVED": city, "COUNTY_SERVED": county})

    if not rows:
        # Soft fallback: name/state matches even if county missing
        return cand_ws.reset_index(drop=True)

    out = pd.DataFrame(rows).drop_duplicates("PWSID").sort_values(["COUNTY_SERVED", "PWS_NAME"])
    return out.reset_index(drop=True)

# Backward-compat convenience for CA-only callers (optional)
def search_by_name_ca(name_query: str, county_filter: str | None) -> pd.DataFrame:
    return search_by_name("CA", name_query, county_filter)

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
    if not HAVE_DOCX:
        print("python-docx is not installed. Install it with: pip install python-docx")
        sys.exit(1)

    doc = Document()
    doc.add_heading(f"SDWIS Report — {pwsid}", level=0)

    ws = data.get("WATER_SYSTEM", pd.DataFrame())
    ga = data.get("GEOGRAPHIC_AREA", pd.DataFrame())
    ws_name = ws.iloc[0]["PWS_NAME"] if not ws.empty and "PWS_NAME" in ws.columns else "N/A"
    county = ga.iloc[0]["COUNTY_SERVED"] if not ga.empty and "COUNTY_SERVED" in ga.columns else "N/A"
    city = ga.iloc[0]["CITY_SERVED"] if not ga.empty and "CITY_SERVED" in ga.columns else "N/A"
    state = ws.iloc[0]["STATE_CODE"] if not ws.empty and "STATE_CODE" in ws.columns else (pwsid[:2] if isinstance(pwsid, str) else "N/A")

    doc.add_paragraph(f"Water System Name: {ws_name}")
    doc.add_paragraph(f"PWSID: {pwsid}")
    doc.add_paragraph(f"State: {state}")
    doc.add_paragraph(f"County Served: {county}")
    doc.add_paragraph(f"City Served: {city}")

    for table, df in data.items():
        doc.add_heading(table.replace("_", " ").title(), level=1)
        if df.empty:
            doc.add_paragraph("No data available.")
            continue
        df2 = add_code_descriptions(df)
        ncols = len(df2.columns)
        header = list(df2.columns)
        t = doc.add_table(rows=1, cols=ncols)
        t.style = "Table Grid"
        for j, col in enumerate(header):
            t.cell(0, j).text = str(col)
        for _, row in df2.iterrows():
            cells = t.add_row().cells
            for j, col in enumerate(header):
                val = row[col]
                cells[j].text = "" if pd.isna(val) else str(val)

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
        county_hint = input("County (optional): ").strip() or None
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

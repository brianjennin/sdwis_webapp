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
        "SERVICE_CONNECTIONS_COUNT", "ORG_NAME", "ADMIN_NAME", "EMAIL_ADDR"
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

# ----------------------- HTTP helpers -----------------------

def api_get_json(url: str):
    """GET with verify True then fallback to False (common in corp envs)."""
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception:
        r = requests.get(url, timeout=60, verify=False)
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

def looks_like_pwsid(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]{2}\d{7}", s.strip()))

def token_and_contains(series: pd.Series, tokens: list[str]) -> pd.Series:
    """AND all tokens; case-insensitive; safe on NaN."""
    mask = pd.Series(True, index=series.index)
    for t in tokens:
        if t:
            mask &= series.astype(str).str.contains(re.escape(t), case=False, na=False)
    return mask

def enforce_california(df: pd.DataFrame) -> pd.DataFrame:
    """Keep rows that are clearly CA via STATE_SERVED OR STATE_CODE OR PWSID prefix."""
    if df.empty:
        return df
    masks = []
    if "STATE_SERVED" in df.columns:
        masks.append(df["STATE_SERVED"].astype(str).str.strip().str.upper().eq("CA"))
    if "STATE_CODE" in df.columns:
        masks.append(df["STATE_CODE"].astype(str).str.strip().str.upper().eq("CA"))
    if "PWSID" in df.columns:
        masks.append(df["PWSID"].astype(str).str[:2].str.upper().eq("CA"))
    if not masks:
        return df
    m = masks[0]
    for mm in masks[1:]:
        m = m | mm
    return df[m]

# ----------------------- Search (CA-only) -----------------------

def search_by_name_ca(name_query: str, county_filter: str | None) -> pd.DataFrame:
    q = name_query.strip()
    tokens = re.findall(r"[A-Za-z0-9]+", q)
    if not tokens:
        return pd.DataFrame()

    ws_all = pull_rows_paged("WATER_SYSTEM")
    ga_all = pull_rows_paged("GEOGRAPHIC_AREA")
    print(f"[DEBUG] Pulled WATER_SYSTEM: {len(ws_all)}; GEOGRAPHIC_AREA: {len(ga_all)}")

    if ws_all.empty or "PWS_NAME" not in ws_all.columns or "PWSID" not in ws_all.columns:
        return pd.DataFrame()

    # Name token match
    name_mask = token_and_contains(ws_all["PWS_NAME"], tokens)
    m_ws = ws_all[name_mask]
    print(f"[DEBUG] Name token matches (pre-CA): {len(m_ws)}")
    if m_ws.empty:
        return pd.DataFrame()

    # Merge GA for county/state/city
    if not ga_all.empty and "PWSID" in ga_all.columns:
        keep_ga = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED", "ZIP_CODE_SERVED"] if c in ga_all.columns]
        ga_small = ga_all[keep_ga].drop_duplicates() if keep_ga else pd.DataFrame()
        if not ga_small.empty:
            cand = m_ws.merge(ga_small, on="PWSID", how="left")
        else:
            cand = m_ws.copy()
    else:
        cand = m_ws.copy()

    # Enforce California (robust)
    before_ca = len(cand)
    cand = enforce_california(cand)
    print(f"[DEBUG] After enforce CA: {len(cand)} (from {before_ca})")
    if cand.empty:
        return pd.DataFrame()

    # Optional county filter
    if county_filter and "COUNTY_SERVED" in cand.columns:
        c = county_filter.strip().upper()
        before = len(cand)
        cand = cand[cand["COUNTY_SERVED"].astype(str).str.strip().str.upper().str.contains(re.escape(c), na=False)]
        print(f"[DEBUG] After county filter={c}: {len(cand)} (from {before})")
        if cand.empty:
            return pd.DataFrame()

    show_cols = [c for c in ["PWSID", "PWS_NAME", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED", "ZIP_CODE_SERVED"] if c in cand.columns]
    if not show_cols:
        show_cols = ["PWSID", "PWS_NAME"]
    cand = cand[show_cols].drop_duplicates(subset=["PWSID"]).reset_index(drop=True)
    return cand

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
    doc.add_heading(f"SDWIS Report (California) — {pwsid}", level=0)

    ws = data.get("WATER_SYSTEM", pd.DataFrame())
    ga = data.get("GEOGRAPHIC_AREA", pd.DataFrame())
    ws_name = ws.iloc[0]["PWS_NAME"] if not ws.empty and "PWS_NAME" in ws.columns else "N/A"
    county = ga.iloc[0]["COUNTY_SERVED"] if not ga.empty and "COUNTY_SERVED" in ga.columns else "N/A"
    city = ga.iloc[0]["CITY_SERVED"] if not ga.empty and "CITY_SERVED" in ga.columns else "N/A"

    doc.add_paragraph(f"Water System Name: {ws_name}")
    doc.add_paragraph(f"PWSID: {pwsid}")
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

# ----------------------- Main -----------------------

def main():
    print("SDWIS Finder (California-only)")
    print("  • Enter a PWSID (e.g., CA1010016), OR")
    print("  • Enter words from the WATER SYSTEM NAME (partial, case-insensitive)")
    query = input("\nSearch: ").strip()

    if looks_like_pwsid(query):
        pwsid = query.upper()
        if not pwsid.startswith("CA"):
            print("This script is limited to California (PWSIDs starting with 'CA').")
            sys.exit(0)
    else:
        county_hint = input("Optional COUNTY name (e.g., Tulare) [Enter to skip]: ").strip() or None
        print("\nSearching by water system name in California…")
        matches = search_by_name_ca(query, county_filter=county_hint)
        if matches.empty:
            print("No matches found. Try different words or adjust county filter.")
            sys.exit(0)

        # Show matches
        display = matches.reset_index().rename(columns={"index": "#"})
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

    # Save CSV snapshot too (optional)
    # with pd.ExcelWriter(f"{pwsid}_tables.xlsx") as xw:
    #     for tab, df in all_data.items():
    #         df.to_excel(xw, sheet_name=tab[:31], index=False)

    if not HAVE_DOCX:
        print("Skipping Word report because python-docx is not installed.")
        sys.exit(0)

    generate_report(pwsid, all_data)

if __name__ == "__main__":
    main()

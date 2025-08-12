# app.py — All states, fast county/city listing with bulk GA + caching
# Runs a Streamlit UI that:
#  - Filters WATER_SYSTEM by state (server-side), caching the result
#  - Pulls GEOGRAPHIC_AREA once (bulk), caching the result
#  - Matches county/city against WS.CITY_NAME (primary) or GA.CITY_SERVED (fallback)
#  - Joins locally for speed; supports blank name + county/city listing
#  - Generates a Word report for a selected PWSID

import os
import re
import tempfile
import pandas as pd
import streamlit as st

from sdwis_ca_report import (
    looks_like_pwsid,
    generate_report,
    fetch_all_selected,
    pull_rows_filtered,
    pull_rows_paged,
    df_upper,
    token_and_contains,
)

st.set_page_config(page_title="SDWIS – Report Generator (All States)", layout="centered")
st.title("SDWIS – Report Generator (All States)")
st.write("Pick a state, optionally add a name and/or county/city, or enter a PWSID. Download a Word report.")

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY",
    "LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH",
    "OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY","PR","VI"
]

# ---------------- Caching ----------------

@st.cache_data(ttl=60*60*12)  # 12 hours
def get_ws_by_state(state: str) -> pd.DataFrame:
    """Server-side filter WATER_SYSTEM by STATE_CODE; keep CITY_NAME (primary city)."""
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", (state or "").upper())
    ws = df_upper(ws)
    keep = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in ws.columns]
    return ws[keep].drop_duplicates("PWSID") if keep else ws

@st.cache_data(ttl=60*60*12)  # 12 hours
def get_ga_all() -> pd.DataFrame:
    """Fetch GEOGRAPHIC_AREA once; filter-by-state via PWSID prefix locally."""
    ga = pull_rows_paged("GEOGRAPHIC_AREA")
    ga = df_upper(ga)
    # GA typically has CITY_SERVED/COUNTY_SERVED (not CITY_NAME)
    keep_candidates = ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED"]
    keep = [c for c in keep_candidates if c in ga.columns]
    ga = ga[keep] if keep else ga
    subset = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga.columns]
    return ga.drop_duplicates(subset=subset) if subset else ga.drop_duplicates()

@st.cache_data(ttl=60*60*12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    return fetch_all_selected(pwsid)

# ---------------- Fast search (bulk GA + local join) ----------------

def fast_search(state: str, name_query: str, county_or_city: str | None) -> pd.DataFrame:
    """
    - Start from WS filtered by state (cached), which includes CITY_NAME when present.
    - Optionally AND-filter by name tokens.
    - If county_or_city provided, match against:
        * WS.CITY_NAME, and/or
        * GA (filtered by state): COUNTY_SERVED or CITY_SERVED
      Take the union of PWSIDs from those matches, then join GA for display.
    """
    sc = (state or "").strip().upper()
    ws = get_ws_by_state(sc)   # PWSID, PWS_NAME, (maybe) CITY_NAME
    ga = get_ga_all()

    # Restrict GA to this state by PWSID prefix (robust even if STATE_SERVED missing)
    ga_state = ga[ga["PWSID"].astype(str).str.startswith(sc)] if "PWSID" in ga.columns else ga.iloc[0:0]

    # Name filter (optional; AND across tokens)
    q = (name_query or "").strip()
    if q:
        tokens = re.findall(r"[A-Za-z0-9]+", q)
        if tokens and "PWS_NAME" in ws.columns:
            m = token_and_contains(ws["PWS_NAME"], tokens)
            ws = ws[m]

    # If no county/city filter: return WS matches with a CITY column from CITY_NAME (if available)
    if not county_or_city:
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not display_cols:
            display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in out.columns]
        return out[display_cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # County/City filter provided → match across both sources
    term = county_or_city.strip().lower()

    # (A) WS.CITY_NAME contains
    if "CITY_NAME" in ws.columns:
        ws_city_mask = ws["CITY_NAME"].astype(str).str.lower().str.contains(term, na=False)
        ws_city_match = ws.loc[ws_city_mask, ["PWSID"]]
    else:
        ws_city_match = pd.DataFrame(columns=["PWSID"])

    # (B) GA.COUNTY_SERVED or GA.CITY_SERVED contains
    m_county = ga_state["COUNTY_SERVED"].astype(str).str.lower().str.contains(term, na=False) if "COUNTY_SERVED" in ga_state.columns else False
    m_city_sv = ga_state["CITY_SERVED"].astype(str).str.lower().str.contains(term, na=False)   if "CITY_SERVED"   in ga_state.columns else False
    ga_match = ga_state[m_county | m_city_sv]
    ga_match = ga_match[["PWSID", "CITY_SERVED", "COUNTY_SERVED"]] if not ga_match.empty else ga_match

    # Union of PWSIDs
    pwsids_from_ws_city = set(ws_city_match["PWSID"]) if "PWSID" in ws_city_match.columns else set()
    pwsids_from_ga      = set(ga_match["PWSID"])      if "PWSID" in ga_match.columns else set()
    pwsid_union = pwsids_from_ws_city | pwsids_from_ga

    if not pwsid_union:
        # Nothing matched city/county; fall back to name-only matches
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not cols:
            cols = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # Keep only WS rows whose PWSID is in the union
    ws_union = ws[ws["PWSID"].isin(pwsid_union)] if "PWSID" in ws.columns else ws.copy()

    # Merge GA info if available (for COUNTY_SERVED / fallback city)
    if not ga_match.empty and "PWSID" in ga_match.columns:
        ws_union = ws_union.merge(ga_match, on="PWSID", how="left")

    # Build unified CITY column: prefer WS.CITY_NAME, else GA.CITY_SERVED
    if "CITY" not in ws_union.columns:
        ws_union["CITY"] = ""
    if "CITY_NAME" in ws_union.columns:
        ws_union["CITY"] = ws_union["CITY"].mask(ws_union["CITY"].eq(""), ws_union["CITY_NAME"].fillna("").astype(str).str.strip())
    if "CITY_SERVED" in ws_union.columns:
        ws_union["CITY"] = ws_union["CITY"].mask(ws_union["CITY"].eq(""), ws_union["CITY_SERVED"].fillna("").astype(str).str.strip())

    # Prepare output
    display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in ws_union.columns]
    if not display_cols:
        display_cols = [c for c in ["PWSID", "PWS_NAME"] if c in ws_union.columns]
    out = ws_union[display_cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)
    return out

# ---------------- UI ----------------

state = st.selectbox("State", STATES, index=STATES.index("AK") if "AK" in STATES else 0)

# Warm caches so first search shows a spinner once
with st.spinner(f"Loading data for {state}… (first time may take a few seconds)"):
    _ = get_ws_by_state(state)
    _ = get_ga_all()

mode = st.radio("Lookup by", ["PWSID", "Name / County or City"], horizontal=True)
pwsid = None

if mode == "PWSID":
    p = st.text_input("PWSID (e.g., AK1234567 or CA1010016)")
    if st.button("Generate report"):
        pid = (p or "").strip().upper()
        if not looks_like_pwsid(pid):
            st.error("Enter a valid PWSID like CA1010016.")
        else:
            if pid[:2] != state:
                st.info(f"Note: PWSID prefix {pid[:2]} differs from selected state {state}. Proceeding anyway.")
            pwsid = pid

else:
    name = st.text_input("Water system name (optional)")
    county_city = st.text_input("County or City (optional)")
    if "matches" not in st.session_state:
        st.session_state.matches = None

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Search"):
            if not name.strip() and not county_city.strip():
                st.warning("Enter a system name, OR a county/city.")
            else:
                with st.spinner(f"Searching {state} systems…"):
                    matches = fast_search(state, name, county_city or None)
                st.session_state.matches = None if matches.empty else matches.reset_index(drop=True)

    if st.session_state.matches is not None:
        st.subheader("Matches")
        display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in st.session_state.matches.columns]
        if not display_cols:
            display_cols = [c for c in ["PWSID", "PWS_NAME"] if c in st.session_state.matches.columns]
        st.dataframe(st.session_state.matches[display_cols], use_container_width=True)

        idx = st.selectbox(
            "Pick a system",
            list(range(len(st.session_state.matches))),
            format_func=lambda i: f"{st.session_state.matches.iloc[i]['PWSID']} — {st.session_state.matches.iloc[i].get('PWS_NAME','')}"
        )
        with col2:
            if st.button("Generate report"):
                pwsid = st.session_state.matches.iloc[idx]["PWSID"]

# ---------------- Report ----------------

if pwsid:
    with st.spinner(f"Fetching SDWIS data for {pwsid}…"):
        data = cached_fetch_all_selected(pwsid)
    with st.spinner("Building Word report…"):
        tmp = tempfile.mkdtemp()
        outpath = os.path.join(tmp, f"{pwsid}_SDWIS_Report.docx")
        outpath = generate_report(pwsid, data, out_path=outpath)
    with open(outpath, "rb") as f:
        st.download_button(
            "Download Word report",
            data=f.read(),
            file_name=os.path.basename(outpath),
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
    st.success("Report generated.")

with st.expander("Developer tools"):
    if st.button("Clear app cache"):
        st.cache_data.clear()
        st.success("Cache cleared. Next search will refetch.")

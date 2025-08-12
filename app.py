# -*- coding: utf-8 -*-
"""
Streamlit app for SDWIS report generation with STATE selection, caching & fast search
"""

import os
import re
import tempfile
import pandas as pd
import streamlit as st

from sdwis_ca_report import (
    looks_like_pwsid,
    generate_report,
    fetch_all_selected,
    # for caching & search
    pull_rows_filtered,
    fetch_table_by_pwsid,
    df_upper,
    token_and_contains,
    search_by_name,   # <-- new generic search you added
)

st.set_page_config(page_title="SDWIS – Report Generator", layout="centered")
st.title("SDWIS – Report Generator")
st.write("Pick a state, optionally enter a name and/or county, and download a Word report for the water system.")

# --------------------- State list ---------------------
STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY",
    "LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH",
    "OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY","PR","VI"
]

# ----------------------------- Caching helpers -----------------------------

@st.cache_data(ttl=60 * 60 * 12)  # 12 hours
def get_ws_by_state(state_code: str) -> pd.DataFrame:
    sc = (state_code or "").upper()
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", sc)
    keep = [c for c in ["PWSID", "PWS_NAME"] if c in ws.columns]
    if keep:
        ws = ws[keep].drop_duplicates("PWSID")
    return ws

@st.cache_data(ttl=60 * 60 * 12, max_entries=2000)
def get_ga_for(pwsid: str) -> pd.DataFrame:
    return df_upper(fetch_table_by_pwsid("GEOGRAPHIC_AREA", pwsid))

@st.cache_data(ttl=60 * 60 * 12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    return fetch_all_selected(pwsid)

def fast_search(state_code: str, name_query: str, county_filter: str | None) -> pd.DataFrame:
    """
    Fast, state-first search:
      - Uses cached WATER_SYSTEM for the chosen state.
      - If name blank: return all systems for that state (optionally county-filtered).
      - If county provided: fetch GA per candidate (cached) to filter by county.
    """
    sc = (state_code or "").strip().upper()
    ws = get_ws_by_state(sc)

    q = (name_query or "").strip()
    tokens = re.findall(r"[A-Za-z0-9]+", q) if q else []

    # Name token match (AND-of tokens) or no tokens = all WS for state
    if tokens:
        m = token_and_contains(ws["PWS_NAME"], tokens)
        cand_ws = ws[m][["PWSID", "PWS_NAME"]].drop_duplicates("PWSID")
    else:
        cand_ws = ws[["PWSID", "PWS_NAME"]].drop_duplicates("PWSID")

    if cand_ws.empty or not county_filter:
        return cand_ws.reset_index(drop=True)

    c = county_filter.strip().upper()
    rows = []
    for _, r in cand_ws.iterrows():
        pwsid = r["PWSID"]
        ga = get_ga_for(pwsid)
        county = next((str(x).strip() for x in ga.get("COUNTY_SERVED", pd.Series()).dropna().astype(str) if x.strip()), "")
        city   = next((str(x).strip() for x in ga.get("CITY_SERVED",   pd.Series()).dropna().astype(str) if x.strip()), "")
        if county and c in county.upper():
            rows.append({"PWSID": pwsid, "PWS_NAME": r["PWS_NAME"], "CITY_SERVED": city, "COUNTY_SERVED": county})

    if not rows:
        return cand_ws.reset_index(drop=True)

    out = pd.DataFrame(rows).drop_duplicates("PWSID").sort_values(["COUNTY_SERVED", "PWS_NAME"])
    return out.reset_index(drop=True)

# ------------------------------ UI / Workflow ------------------------------

state = st.selectbox("State", STATES, index=STATES.index("CA") if "CA" in STATES else 0)

# Warm the cache for the chosen state so first search is smoother
with st.spinner(f"Loading water systems for {state}… (first time may take a few seconds)"):
    _ = get_ws_by_state(state)

mode = st.radio("Lookup by", ["PWSID", "Name / County"], horizontal=True)
pwsid = None

if mode == "PWSID":
    pwsid_input = st.text_input("PWSID (e.g., CA1010016)", value="")
    if st.button("Generate report"):
        pid = pwsid_input.strip().upper()
        if not looks_like_pwsid(pid):
            st.error("Please enter a valid PWSID like CA1010016.")
        else:
            # Optional: warn if PWSID’s prefix doesn't match the selected state
            if state and pid[:2] != state:
                st.info(f"Note: PWSID starts with {pid[:2]}, not {state}. Proceeding anyway.")
            pwsid = pid

else:
    name = st.text_input("Water system name (optional)", value="")
    county = st.text_input("County (optional)", value="")
    if "matches" not in st.session_state:
        st.session_state.matches = None

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Search"):
            if not name.strip() and not county.strip():
                st.warning("Enter a system name, OR at least a county.")
            else:
                with st.spinner(f"Searching {state} systems..."):
                    # You can call either the module-level search_by_name(...)
                    # or this app's fast_search(...) which uses more caching.
                    matches = fast_search(state, name, county_filter=(county or None))
                if matches is None or matches.empty:
                    st.warning("No matches. Try different words or adjust the county.")
                    st.session_state.matches = None
                else:
                    st.session_state.matches = matches.reset_index(drop=True)

    if st.session_state.matches is not None:
        st.subheader("Matches")
        show_cols = [c for c in ["PWSID", "PWS_NAME", "CITY_SERVED", "COUNTY_SERVED"] if c in st.session_state.matches.columns]
        st.dataframe(st.session_state.matches[show_cols])
        idx = st.selectbox(
            "Pick a system",
            options=list(range(len(st.session_state.matches))),
            format_func=lambda i: f"{st.session_state.matches.iloc[i]['PWSID']} — {st.session_state.matches.iloc[i].get('PWS_NAME','')}",
        )
        with col2:
            if st.button("Generate report"):
                pwsid = st.session_state.matches.iloc[idx]["PWSID"]

# Build & offer the download if a PWSID is selected
if pwsid:
    with st.spinner(f"Fetching SDWIS data for {pwsid}..."):
        data = cached_fetch_all_selected(pwsid)

    with st.spinner("Building Word report..."):
        tmpdir = tempfile.mkdtemp()
        outpath = os.path.join(tmpdir, f"{pwsid}_SDWIS_Report.docx")
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

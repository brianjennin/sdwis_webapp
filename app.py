# -*- coding: utf-8 -*-
"""
Streamlit app for SDWIS CA report generation with caching & fast search
"""

import os
import re
import tempfile
import pandas as pd
import streamlit as st

from sdwis_ca_report import (
    # existing helpers you already had:
    looks_like_pwsid,
    generate_report,
    fetch_all_selected,
    # new imports used for caching & fast search:
    pull_rows_filtered,       # server-side filtered pull for WATER_SYSTEM
    fetch_table_by_pwsid,     # per-PWSID fetch for GEOGRAPHIC_AREA
    df_upper,
    token_and_contains,
)

st.set_page_config(page_title="SDWIS CA – Report Generator", layout="centered")
st.title("SDWIS California – Report Generator")
st.write("Generate a Word report for a California water system by PWSID or by searching its name.")

# ----------------------------- Caching helpers -----------------------------

@st.cache_data(ttl=60 * 60 * 12)  # 12 hours
def get_ws_ca() -> pd.DataFrame:
    """
    Fetch the WATER_SYSTEM table filtered to California (STATE_CODE='CA') once
    and keep only the fields we need for searching.
    """
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", "CA")
    keep = [c for c in ["PWSID", "PWS_NAME"] if c in ws.columns]
    if keep:
        ws = ws[keep].drop_duplicates("PWSID")
    return ws

@st.cache_data(ttl=60 * 60 * 12, max_entries=2000)
def get_ga_for(pwsid: str) -> pd.DataFrame:
    """
    Fetch GEOGRAPHIC_AREA rows for a specific PWSID (no STATE_SERVED filter).
    Cached per PWSID to avoid repeat network calls.
    """
    return df_upper(fetch_table_by_pwsid("GEOGRAPHIC_AREA", pwsid))

@st.cache_data(ttl=60 * 60 * 12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    """
    Cached version of the per-PWSID table bundle used for report generation.
    """
    return fetch_all_selected(pwsid)

def fast_search(name_query: str, county_filter: str | None) -> pd.DataFrame:
    """
    Fast name search:
      - Uses cached CA-only WATER_SYSTEM for token matching.
      - If county provided, fetches GA per candidate PWSID (cached) to filter by county.
    """
    ws = get_ws_ca()
    q = (name_query or "").strip()
    tokens = re.findall(r"[A-Za-z0-9]+", q)
    if not tokens:
        return pd.DataFrame()

    # AND-match all tokens against PWS_NAME
    m = token_and_contains(ws["PWS_NAME"], tokens)
    cand_ws = ws[m][["PWSID", "PWS_NAME"]].drop_duplicates("PWSID")

    if cand_ws.empty or not county_filter:
        return cand_ws.reset_index(drop=True)

    c = county_filter.strip().upper()
    rows = []
    for _, r in cand_ws.iterrows():
        pwsid = r["PWSID"]
        ga = get_ga_for(pwsid)

        # Take first non-empty county/city (if present)
        county_series = ga["COUNTY_SERVED"] if "COUNTY_SERVED" in ga.columns else pd.Series(dtype=str)
        city_series   = ga["CITY_SERVED"]   if "CITY_SERVED"   in ga.columns else pd.Series(dtype=str)

        county = next((str(x).strip() for x in county_series.dropna().astype(str) if x.strip()), "")
        city   = next((str(x).strip() for x in city_series.dropna().astype(str)   if x.strip()), "")

        if county and c in county.upper():
            rows.append({
                "PWSID": pwsid,
                "PWS_NAME": r["PWS_NAME"],
                "CITY_SERVED": city,
                "COUNTY_SERVED": county,
            })

    if not rows:
        # If no county data matched (often missing in GA), fall back to name-only matches
        return cand_ws.reset_index(drop=True)

    out = pd.DataFrame(rows).drop_duplicates("PWSID").sort_values(["COUNTY_SERVED", "PWS_NAME"])
    return out.reset_index(drop=True)

# Warm the cache so first user sees a spinner rather than a long delay
with st.spinner("Loading California system list (first time may take a few seconds)..."):
    _ = get_ws_ca()

# ------------------------------ UI / Workflow ------------------------------

mode = st.radio("Lookup by", ["PWSID", "Water system name"], horizontal=True)
pwsid = None

if mode == "PWSID":
    pwsid_input = st.text_input("PWSID (e.g., CA1010016)", value="")
    if st.button("Generate report"):
        if not looks_like_pwsid(pwsid_input) or not pwsid_input.upper().startswith("CA"):
            st.error("Please enter a valid California PWSID (e.g., CA1010016).")
        else:
            pwsid = pwsid_input.strip().upper()

else:
    name = st.text_input("Water system name", value="")
    county = st.text_input("County (optional)", value="")
    if "matches" not in st.session_state:
        st.session_state.matches = None

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Search"):
            if not name.strip():
                st.warning("Enter some part of the system name, e.g., 'City of'.")
            else:
                with st.spinner("Searching California systems..."):
                    matches = fast_search(name, county_filter=(county or None))
                if matches is None or matches.empty:
                    st.warning("No matches. Try different words or remove the county filter.")
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
        data = cached_fetch_all_selected(pwsid)  # cached

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

# Optional: a quick button to clear cache while developing
with st.expander("Developer tools"):
    if st.button("Clear app cache"):
        st.cache_data.clear()
        st.success("Cache cleared. Next search will refetch.")

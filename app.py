# app.py — Preload WS + GA for each state, then filter locally
# - On first state selection: fetch full WATER_SYSTEM + GEOGRAPHIC_AREA once
# - All searches afterwards are in-memory pandas filters (instant)
# - Word report generation still per-PWSID via cached_fetch_all_selected

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
    df_upper,
    token_and_contains,
)

st.set_page_config(page_title="SDWIS – Report Generator (All States)", layout="centered")
st.title("SDWIS – Report Generator (All States)")
st.write("Pick a state, optionally add a name and/or county/city, or enter a PWSID. Then download a Word report.")

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY",
    "LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH",
    "OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY","PR","VI"
]

# ---------------- Caching ----------------

@st.cache_data(ttl=60*60*12)  # 12 hours, persisted in memory/disk
def get_ga_by_state(state: str) -> pd.DataFrame:
    """Fetch GEOGRAPHIC_AREA by STATE_SERVED; keep city/county columns."""
    ga = pull_rows_filtered("GEOGRAPHIC_AREA", "STATE_SERVED", (state or "").upper())
    ga = df_upper(ga)
    keep = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED"] if c in ga.columns]
    ga = ga[keep] if keep else ga
    subset = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga.columns]
    return ga.drop_duplicates(subset=subset) if subset else ga.drop_duplicates()

@st.cache_data(ttl=60*60*12)  # 12 hours
def get_ws_by_state(state: str) -> pd.DataFrame:
    """Fetch WATER_SYSTEM by STATE_CODE; keep minimal columns."""
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", (state or "").upper())
    ws = df_upper(ws)
    keep = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in ws.columns]
    return ws[keep].drop_duplicates("PWSID") if keep else ws

@st.cache_data(ttl=60*60*12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    """Cache per-system tables for report generation."""
    return fetch_all_selected(pwsid)

# ---------------- Search helpers ----------------

def fast_search(state: str, name_query: str, county_or_city: str | None) -> pd.DataFrame:
    """
    All searches are local pandas filters on preloaded WS + GA.
    """
    sc = (state or "").strip().upper()
    ws = get_ws_by_state(sc)
    ga = get_ga_by_state(sc)

    if ws.empty:
        return pd.DataFrame(columns=["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"])

    # Add CITY column from WS
    df = ws.copy()
    df["CITY"] = df.get("CITY_NAME", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()

    # Filter by name tokens if provided
    q = (name_query or "").strip()
    if q and "PWS_NAME" in df.columns:
        tokens = re.findall(r"[A-Za-z0-9]+", q)
        if tokens:
            m = token_and_contains(df["PWS_NAME"], tokens)
            df = df[m]

    # Filter by county/city if provided
    if county_or_city and not ga.empty:
        term = county_or_city.strip().lower()
        m_county = ga["COUNTY_SERVED"].astype(str).str.lower().str.contains(term, na=False) if "COUNTY_SERVED" in ga.columns else False
        m_citysv = ga["CITY_SERVED"].astype(str).str.lower().str.contains(term, na=False)   if "CITY_SERVED"   in ga.columns else False
        ga_match = ga[m_county | m_citysv]

        if ga_match.empty:
            return pd.DataFrame(columns=["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"])

        df = df.merge(
            ga_match[["PWSID", "CITY_SERVED", "COUNTY_SERVED"]].drop_duplicates("PWSID"),
            on="PWSID", how="inner"
        )
        # Prefer CITY_NAME, fallback to GA city
        df["CITY"] = df["CITY"].mask(df["CITY"].eq(""), df["CITY_SERVED"].fillna("").astype(str).str.strip())

    cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in df.columns]
    return df[cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

# ---------------- UI ----------------

state = st.selectbox("State", STATES, index=STATES.index("CA") if "CA" in STATES else 0)

# Preload GA + WS cache for this state
with st.spinner(f"Loading all systems for {state} (first time may take up to a minute)…"):
    ga = get_ga_by_state(state)
    ws = get_ws_by_state(state)

st.success(f"{len(ws):,} water systems cached for {state}. Searches are now instant.")

mode = st.radio("Lookup by", ["PWSID", "Name / County or City"], horizontal=True)
pwsid_to_generate: str | None = None

if mode == "PWSID":
    p = st.text_input("PWSID (e.g., AK1234567 or CA1010016)")
    if st.button("Generate report"):
        pid = (p or "").strip().upper()
        if not looks_like_pwsid(pid):
            st.error("Enter a valid PWSID like CA1010016.")
        else:
            if pid[:2] != state:
                st.info(f"Note: PWSID prefix {pid[:2]} differs from selected state {state}. Proceeding anyway.")
            pwsid_to_generate = pid

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
                with st.spinner(f"Filtering {state} systems…"):
                    matches = fast_search(state, name, county_city or None)
                st.session_state.matches = None if matches.empty else matches.reset_index(drop=True)

    # Show results + in-table single selection
    if st.session_state.matches is not None:
        st.subheader("Matches")
        df = st.session_state.matches.copy()

        # Quick local filter
        st.write("Tip: filter by PWSID, name, city, or county. Multiple words allowed (e.g., `los angeles water`).")
        qf = st.text_input("Filter rows", key="quick_filter").strip()
        if qf:
            tokens = [t for t in re.findall(r"[A-Za-z0-9]+", qf) if t]
            if tokens:
                mask = pd.Series(True, index=df.index)
                for t in tokens:
                    hay = df.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                    mask &= hay.str.contains(re.escape(t.lower()), na=False)
                df = df[mask]

        st.caption(f"{len(df):,} systems shown")

        if "Select" not in df.columns:
            df.insert(0, "Select", False)
        disabled_cols = [c for c in df.columns if c != "Select"]

        edited = st.data_editor(
            df,
            hide_index=True,
            use_container_width=True,
            height=420,
            disabled=disabled_cols,
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    label="Select",
                    help="Tick one row to generate a report",
                    default=False,
                ),
                "PWSID": st.column_config.TextColumn("PWSID"),
                "PWS_NAME": st.column_config.TextColumn("Water System"),
                "CITY": st.column_config.TextColumn("City"),
                "COUNTY_SERVED": st.column_config.TextColumn("County"),
            },
            key="matches_editor",
        )

        selected_rows = edited[edited["Select"] == True]
        with col2:
            if st.button("Generate report for selected"):
                if len(selected_rows) == 0:
                    st.error("Select one row first.")
                elif len(selected_rows) > 1:
                    st.error("Only one row can be selected.")
                else:
                    pwsid_to_generate = str(selected_rows.iloc[0]["PWSID"])

# ---------------- Report ----------------

if pwsid_to_generate:
    with st.spinner(f"Fetching SDWIS data for {pwsid_to_generate}…"):
        data = cached_fetch_all_selected(pwsid_to_generate)
    with st.spinner("Building Word report…"):
        tmp = tempfile.mkdtemp()
        outpath = os.path.join(tmp, f"{pwsid_to_generate}_SDWIS_Report.docx")
        outpath = generate_report(pwsid_to_generate, data, out_path=outpath)
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

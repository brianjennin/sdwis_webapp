# app.py — All states, instant searches after first load (bulk GA cached once)
# - Caches WATER_SYSTEM by state (server-side filtered)
# - Caches GEOGRAPHIC_AREA once (paged) and derives a per-state view by PWSID prefix
# - Prefers WS.CITY_NAME; falls back to GA.CITY_SERVED/COUNTY_SERVED
# - Select a system directly in the results table; generate Word report

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
    """Server-side filter WATER_SYSTEM by STATE_CODE; keep CITY_NAME (preferred city)."""
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", (state or "").upper())
    ws = df_upper(ws)
    keep = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in ws.columns]
    return ws[keep].drop_duplicates("PWSID") if keep else ws

@st.cache_data(ttl=60*60*12)  # 12 hours
def get_ga_all() -> pd.DataFrame:
    """Fetch whole GEOGRAPHIC_AREA (paged) once; keep common columns; uppercase; de-dup."""
    ga = pull_rows_paged("GEOGRAPHIC_AREA")
    ga = df_upper(ga)
    keep = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED"] if c in ga.columns]
    ga = ga[keep] if keep else ga
    subset = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga.columns]
    return ga.drop_duplicates(subset=subset) if subset else ga.drop_duplicates()

@st.cache_data(ttl=60*60*12)  # 12 hours
def get_ga_by_state(state: str) -> pd.DataFrame:
    """
    Derive a per-state GA view from the fully cached GA by using PWSID prefix.
    This is instant after GA has been fetched once.
    """
    sc = (state or "").upper()
    ga = get_ga_all()
    if ga.empty or "PWSID" not in ga.columns:
        return ga.iloc[0:0]
    ga_state = ga[ga["PWSID"].astype(str).str.startswith(sc)]
    return ga_state.reset_index(drop=True)

@st.cache_data(ttl=60*60*12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    return fetch_all_selected(pwsid)

# ---------------- Fast search (all in-memory after first GA load) ----------------

def fast_search(state: str, name_query: str, county_or_city: str | None) -> pd.DataFrame:
    """
    - Start from WS filtered by state (cached), includes CITY_NAME when present.
    - Optional AND-filter by name tokens.
    - If county_or_city present, use GA-by-state (cached, in-memory) to match COUNTY_SERVED or CITY_SERVED.
    - Prefer WS.CITY_NAME; fallback to GA.CITY_SERVED for display.
    """
    sc = (state or "").strip().upper()
    ws = get_ws_by_state(sc)   # instant after first call per state
    ga = get_ga_by_state(sc)   # instant after first-ever GA fetch

    # Name filter (optional; AND across tokens)
    q = (name_query or "").strip()
    if q and "PWS_NAME" in ws.columns:
        tokens = re.findall(r"[A-Za-z0-9]+", q)
        if tokens:
            m = token_and_contains(ws["PWS_NAME"], tokens)
            ws = ws[m]

    # If no county/city filter → return WS with CITY from CITY_NAME
    if not (county_or_city or "").strip():
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not cols:
            cols = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # County/City filter via GA (all in memory)
    term = county_or_city.strip().lower()
    if ga.empty:
        # No GA available; fall back to WS + CITY_NAME only
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not cols:
            cols = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    m_county = ga["COUNTY_SERVED"].astype(str).str.lower().str.contains(term, na=False) if "COUNTY_SERVED" in ga.columns else False
    m_citysv = ga["CITY_SERVED"].astype(str).str.lower().str.contains(term, na=False)   if "CITY_SERVED"   in ga.columns else False
    ga_match = ga[m_county | m_citysv]
    if ga_match.empty:
        # No GA match → still show WS results (name filter only)
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not cols:
            cols = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    keep_ga = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga_match.columns]
    ga_small = ga_match[keep_ga].drop_duplicates("PWSID")
    out = ws.merge(ga_small, on="PWSID", how="inner")

    # Build unified CITY for display: prefer WS.CITY_NAME, else GA.CITY_SERVED
    if "CITY" not in out.columns:
        out["CITY"] = ""
    if "CITY_NAME" in out.columns:
        out["CITY"] = out["CITY"].mask(out["CITY"].eq(""), out["CITY_NAME"].fillna("").astype(str).str.strip())
    if "CITY_SERVED" in out.columns:
        out["CITY"] = out["CITY"].mask(out["CITY"].eq(""), out["CITY_SERVED"].fillna("").astype(str).str.strip())

    show = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in out.columns]
    if not show:
        show = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
    return out[show].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

# ---------------- UI ----------------

state = st.selectbox("State", STATES, index=STATES.index("AK") if "AK" in STATES else 0)

# Warm caches:
#  - WS by state is quick (server-side filtered)
#  - GA all may take longer the very first time ever (then it's cached)
with st.spinner(f"Loading data for {state}… (first load of GA may take longer once)"):
    _ = get_ws_by_state(state)
    _ = get_ga_by_state(state)

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

    # --- Show results only if we have them ---
    if st.session_state.matches is not None:
        st.subheader("Matches")

        # Working copy with columns to display
        show_cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in st.session_state.matches.columns]
        df = st.session_state.matches[show_cols].copy()

        # Quick local filter (token-AND over visible text)
        st.write("Tip: filter by PWSID, name, city, or county. You can type multiple words (e.g., `los angeles water`).")
        q = st.text_input("Filter rows", key="quick_filter").strip()
        if q:
            tokens = [t for t in re.findall(r"[A-Za-z0-9]+", q) if t]
            if tokens:
                hay = df.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                m = pd.Series(True, index=df.index)
                for t in tokens:
                    m &= hay.str.contains(re.escape(t.lower()), na=False)
                df = df[m]

        st.caption(f"{len(df):,} systems shown")

        # Add a checkbox column for selecting exactly one row
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

        # Enforce single-selection and trigger report
        selected_rows = edited[edited["Select"] == True]
        with col2:
            if st.button("Generate report for selected"):
                if len(selected_rows) == 0:
                    st.error("Select one row first.")
                elif len(selected_rows) > 1:
                    st.error("Only one row can be selected.")
                else:
                    pwsid = str(selected_rows.iloc[0]["PWSID"])

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

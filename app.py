# app.py — All states, fast search with per-state caching
# - Caches WATER_SYSTEM by STATE_CODE (server-side filtered)
# - Caches GEOGRAPHIC_AREA by STATE_SERVED (server-side filtered)
# - City preference: WATER_SYSTEM.CITY_NAME > GEOGRAPHIC_AREA.CITY_SERVED
# - Pick a system directly from the results table; single selection
# - Generate a Word report via sdwis_ca_report.generate_report

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

# ---------------- Caching (per state) ----------------

@st.cache_data(ttl=60*60*12)  # 12 hours
def get_ws_by_state(state: str) -> pd.DataFrame:
    """Server-side filter WATER_SYSTEM by STATE_CODE; keep city_name when present."""
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", (state or "").upper())
    ws = df_upper(ws)
    keep = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in ws.columns]
    return ws[keep].drop_duplicates("PWSID") if keep else ws

@st.cache_data(ttl=60*60*12)  # 12 hours
def get_ga_by_state(state: str) -> pd.DataFrame:
    """Server-side filter GEOGRAPHIC_AREA by STATE_SERVED; keep city/county columns."""
    ga = pull_rows_filtered("GEOGRAPHIC_AREA", "STATE_SERVED", (state or "").upper())
    ga = df_upper(ga)
    keep = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED"] if c in ga.columns]
    ga = ga[keep] if keep else ga
    subset = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga.columns]
    return ga.drop_duplicates(subset=subset) if subset else ga.drop_duplicates()

@st.cache_data(ttl=60*60*12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    """Cache the per-system table pulls used to build the report."""
    return fetch_all_selected(pwsid)

# ---------------- Fast search (join WS + GA per state) ----------------

def fast_search(state: str, name_query: str, county_or_city: str | None) -> pd.DataFrame:
    """
    Steps:
      1) WS filtered by state (cached), optional name token AND filter.
      2) If county_or_city provided, search GA (county or city) and also WS.CITY_NAME;
         union PWSIDs; join GA to WS for county display.
      3) Build unified CITY column: prefer WS.CITY_NAME, else GA.CITY_SERVED.
    """
    sc = (state or "").strip().upper()
    ws = get_ws_by_state(sc)  # PWSID, PWS_NAME, (maybe) CITY_NAME
    ga = get_ga_by_state(sc)  # PWSID, CITY_SERVED, COUNTY_SERVED

    # Optional name filter (AND across tokens)
    q = (name_query or "").strip()
    if q and "PWS_NAME" in ws.columns:
        tokens = re.findall(r"[A-Za-z0-9]+", q)
        if tokens:
            m = token_and_contains(ws["PWS_NAME"], tokens)
            ws = ws[m]
            if ws.empty:
                return pd.DataFrame(columns=["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"])

    # If no county/city provided, return WS with CITY from CITY_NAME
    if not county_or_city:
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not display_cols:
            display_cols = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[display_cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # County/City filter across GA and WS.CITY_NAME
    term = county_or_city.strip().lower()

    # From WS city_name
    if "CITY_NAME" in ws.columns:
        ws_city_mask = ws["CITY_NAME"].astype(str).str.lower().str.contains(term, na=False)
        ws_city_pwsids = set(ws.loc[ws_city_mask, "PWSID"])
    else:
        ws_city_pwsids = set()

    # From GA county or city_served
    m_county = ga["COUNTY_SERVED"].astype(str).str.lower().str.contains(term, na=False) if "COUNTY_SERVED" in ga.columns else False
    m_citysv = ga["CITY_SERVED"].astype(str).str.lower().str.contains(term, na=False) if "CITY_SERVED" in ga.columns else False
    ga_match = ga[m_county | m_citysv]
    ga_pwsids = set(ga_match["PWSID"]) if not ga_match.empty and "PWSID" in ga_match.columns else set()

    pws_union = ws_city_pwsids | ga_pwsids
    if not pws_union:
        # Nothing matched county/city; return name-only results
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not display_cols:
            display_cols = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[display_cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # Keep WS rows in union and merge GA for county display
    ws_union = ws[ws["PWSID"].isin(pws_union)]
    if not ga_match.empty and "PWSID" in ga_match.columns:
        ws_union = ws_union.merge(ga_match[["PWSID", "CITY_SERVED", "COUNTY_SERVED"]], on="PWSID", how="left")

    # Unified CITY column
    if "CITY" not in ws_union.columns:
        ws_union["CITY"] = ""
    if "CITY_NAME" in ws_union.columns:
        ws_union["CITY"] = ws_union["CITY"].mask(ws_union["CITY"].eq(""), ws_union["CITY_NAME"].fillna("").astype(str).str.strip())
    if "CITY_SERVED" in ws_union.columns:
        ws_union["CITY"] = ws_union["CITY"].mask(ws_union["CITY"].eq(""), ws_union["CITY_SERVED"].fillna("").astype(str).str.strip())

    display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in ws_union.columns]
    if not display_cols:
        display_cols = [c for c in ["PWSID", "PWS_NAME"] if c in ws_union.columns]
    return ws_union[display_cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

# ---------------- UI ----------------

state = st.selectbox("State", STATES, index=STATES.index("AK") if "AK" in STATES else 0)

# Warm caches for this state (keeps initial load quick & searches instant afterward)
with st.spinner(f"Loading data for {state}… (first time may take a few seconds)"):
    _ = get_ws_by_state(state)
    _ = get_ga_by_state(state)

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
                with st.spinner(f"Searching {state} systems…"):
                    matches = fast_search(state, name, county_city or None)
                st.session_state.matches = None if matches.empty else matches.reset_index(drop=True)

    # Show results + in-table selection
    if st.session_state.matches is not None:
        st.subheader("Matches")

        df = st.session_state.matches.copy()
        # Quick local filter across visible columns
        st.write("Tip: filter by PWSID, name, city, or county. You can type multiple words (e.g., `los angeles water`).")
        q = st.text_input("Filter rows", key="quick_filter").strip()
        if q:
            tokens = [t for t in re.findall(r"[A-Za-z0-9]+", q) if t]
            if tokens:
                hay = df.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                mask = pd.Series(True, index=df.index)
                for t in tokens:
                    mask &= hay.str.contains(re.escape(t.lower()), na=False)
                df = df[mask]

        st.caption(f"{len(df):,} systems shown")

        # Add selection column (single selection enforced)
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

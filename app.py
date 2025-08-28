# app.py — All states, fast county/city listing with lazy GA fetch + caching
# - Filters WATER_SYSTEM by state (server-side), caching the result
# - Fetches GEOGRAPHIC_AREA per PWSID on-demand (cached), instead of bulk-pulling the whole table
# - Matches county/city against WS.CITY_NAME (primary) or GA.CITY_SERVED/COUNTY_SERVED (fallback)
# - Lets users select a system directly from the table and generates a Word report

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
    fetch_table_by_pwsid,  # <-- needed for per-PWSID GA fetch
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

@st.cache_data(ttl=60*60*12, max_entries=5000)
def cached_ga_by_pwsid(pwsid: str) -> pd.DataFrame:
    """
    Fetch GEOGRAPHIC_AREA for a single PWSID and cache it.
    Returns an uppercase-column DataFrame (may be empty).
    """
    df = fetch_table_by_pwsid("GEOGRAPHIC_AREA", pwsid)
    return df_upper(df)

@st.cache_data(ttl=60*60*12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    return fetch_all_selected(pwsid)

# ---------------- Fast search (lazy GA fetch) ----------------

def fast_search(state: str, name_query: str, county_or_city: str, max_ga_candidates: int = 300) -> pd.DataFrame:
    """
    - Start from WS filtered by state (cached), includes CITY_NAME when present.
    - Optionally AND-filter by name tokens.
    - If county_or_city provided:
        * WS city filter via CITY_NAME (no GA calls)
        * GA county/city filter by lazily fetching GA only for a capped set of candidate PWSIDs
      Take the union of PWSIDs from both paths, merge GA fields if present, and display.
    """
    sc = (state or "").strip().upper()
    ws = get_ws_by_state(sc)   # PWSID, PWS_NAME, (maybe) CITY_NAME
    if ws.empty:
        return ws

    # Name filter (optional; AND across tokens)
    q = (name_query or "").strip()
    if q and "PWS_NAME" in ws.columns:
        tokens = re.findall(r"[A-Za-z0-9]+", q)
        if tokens:
            m = token_and_contains(ws["PWS_NAME"], tokens)
            ws = ws[m]

    # If no county/city filter: return WS matches with a CITY column from CITY_NAME (if available)
    if not (county_or_city or "").strip():
        out = ws.copy()
        if "CITY_NAME" in out.columns:
            out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()
        display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not display_cols:
            display_cols = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in out.columns]
        return out[display_cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # County/City filter provided
    term = (county_or_city or "").strip().lower()

    # (A) WS city matches via CITY_NAME (fast, no GA)
    if "CITY_NAME" in ws.columns:
        ws_city_mask = ws["CITY_NAME"].astype(str).str.lower().str.contains(term, na=False)
        ws_city_match = set(ws.loc[ws_city_mask, "PWSID"])
    else:
        ws_city_match = set()

    # (B) GA matches via COUNTY_SERVED or CITY_SERVED — lazy, per-PWSID, capped
    # Candidates for GA fetch: already name-filtered WS
    candidates = ws["PWSID"].tolist()

    too_many = False
    if len(candidates) > max_ga_candidates:
        # If the user gave no name, we cap GA calls to keep things fast
        too_many = True
        candidates = candidates[:max_ga_candidates]

    ga_rows = []
    for pid in candidates:
        ga_df = cached_ga_by_pwsid(pid)
        if ga_df.empty:
            continue
        # Keep first non-empty city/county (some systems have multiple rows)
        county = next((str(x).strip() for x in ga_df.get("COUNTY_SERVED", pd.Series()).dropna().astype(str) if x.strip()), "")
        citysv = next((str(x).strip() for x in ga_df.get("CITY_SERVED",   pd.Series()).dropna().astype(str) if x.strip()), "")
        if (county and term in county.lower()) or (citysv and term in citysv.lower()):
            ga_rows.append({"PWSID": pid, "CITY_SERVED": citysv, "COUNTY_SERVED": county})

    ga_match = pd.DataFrame(ga_rows) if ga_rows else pd.DataFrame(columns=["PWSID","CITY_SERVED","COUNTY_SERVED"])
    pwsids_from_ga = set(ga_match["PWSID"]) if not ga_match.empty else set()

    # Union of PWSIDs from both paths
    pwsid_union = ws_city_match | pwsids_from_ga
    if not pwsid_union:
        # Nothing matched in GA or WS city; fall back to name-only matches
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

    # Gentle note if we capped GA calls
    if too_many:
        st.info(f"Showing county/city matches from the first {max_ga_candidates} candidates for speed. Add a name or city to narrow further.")

    return out

# ---------------- UI ----------------

state = st.selectbox("State", STATES, index=STATES.index("AK") if "AK" in STATES else 0)

# Warm WS cache (fast); GA is now lazy per PWSID
with st.spinner(f"Loading water systems for {state}… (first time may take a few seconds)"):
    _ = get_ws_by_state(state)

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
                    matches = fast_search(state, name, county_city or "")
                st.session_state.matches = None if matches.empty else matches.reset_index(drop=True)

    # --- Show results only if we have them ---
    if st.session_state.matches is not None:
        st.subheader("Matches")

        # Build a working copy with columns to display
        show_cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in st.session_state.matches.columns]
        df = st.session_state.matches[show_cols].copy()

        # --- Quick local filter (token AND across all visible text columns) ---
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

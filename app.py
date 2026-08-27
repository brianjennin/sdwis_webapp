# app.py — Fetch only what a search actually needs
# - Selecting a state fetches nothing
# - PWSID lookups go straight to that system; no state data is touched
# - A name search is filtered server-side and returns only matching rows
# - The full-state pull is still available, but only on request, or as a
#   fallback when a targeted query fails

import os
import re
import tempfile
import pandas as pd
import streamlit as st
from sdwis_ca_report import generate_reports_zip


from sdwis_ca_report import (
    looks_like_pwsid,
    generate_report,
    fetch_all_selected,
    pull_rows_filtered,
    df_upper,
    token_and_contains,
    search_systems_targeted,
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
#
# The two by-state pulls below are the expensive path: each walks
# WATER_SYSTEM / GEOGRAPHIC_AREA for a whole state. They are no longer called
# on state selection -- only when a search genuinely needs the full list
# (a county-only search, or a targeted query that failed).

def get_ga_by_state(state: str, stats: list) -> pd.DataFrame:
    """Fetch GEOGRAPHIC_AREA by STATE_SERVED; keep city/county columns."""
    ga = _ga_by_state_cached((state or "").upper())
    stats.extend(ga.attrs.get("stats", []))
    return ga

@st.cache_data(ttl=60*60*12)  # 12 hours, persisted in memory/disk
def _ga_by_state_cached(state: str) -> pd.DataFrame:
    stats: list = []
    ga = pull_rows_filtered("GEOGRAPHIC_AREA", "STATE_SERVED", (state or "").upper(), stats=stats)
    ga = df_upper(ga)
    keep = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED"] if c in ga.columns]
    ga = ga[keep] if keep else ga
    subset = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga.columns]
    out = ga.drop_duplicates(subset=subset) if subset else ga.drop_duplicates()
    out.attrs["stats"] = stats
    return out

def get_ws_by_state(state: str, stats: list) -> pd.DataFrame:
    """Fetch WATER_SYSTEM by STATE_CODE; keep minimal columns."""
    ws = _ws_by_state_cached((state or "").upper())
    stats.extend(ws.attrs.get("stats", []))
    return ws

@st.cache_data(ttl=60*60*12)  # 12 hours
def _ws_by_state_cached(state: str) -> pd.DataFrame:
    stats: list = []
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", (state or "").upper(), stats=stats)
    ws = df_upper(ws)
    keep = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in ws.columns]
    out = ws[keep].drop_duplicates("PWSID") if keep else ws
    out.attrs["stats"] = stats
    return out

@st.cache_data(ttl=60*60*12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    """Cache per-system tables for report generation."""
    return fetch_all_selected(pwsid)

@st.cache_data(ttl=60*60*12, max_entries=200, show_spinner=False)
def cached_targeted_search(state: str, name: str, place: str):
    """Server-side filtered search; raises on failure so the caller can fall back."""
    return search_systems_targeted(state, name or None, place or None)

# ---------------- Search helpers ----------------

def bulk_search(state: str, name_query: str, county_or_city: str | None):
    """Full-state pull, then local pandas filters. The expensive path.

    Returns (results, stats). The stats matter here more than anywhere else:
    this path inner-joins WATER_SYSTEM against GEOGRAPHIC_AREA, so if either
    pull came back short the join silently drops real matches.
    """
    sc = (state or "").strip().upper()
    stats = []
    ws = get_ws_by_state(sc, stats)
    ga = get_ga_by_state(sc, stats)

    if ws.empty:
        return pd.DataFrame(columns=["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"]), stats

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
            return pd.DataFrame(columns=["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"]), stats

        df = df.merge(
            ga_match[["PWSID", "CITY_SERVED", "COUNTY_SERVED"]].drop_duplicates("PWSID"),
            on="PWSID", how="inner"
        )
        # Prefer CITY_NAME, fallback to GA city
        df["CITY"] = df["CITY"].mask(df["CITY"].eq(""), df["CITY_SERVED"].fillna("").astype(str).str.strip())

    cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in df.columns]
    return df[cols].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True), stats


def run_search(state: str, name_query: str, county_or_city: str | None):
    """Search with the cheapest method that can answer the question.

    Returns (results, how) where `how` explains which path ran, so the cost of
    a search is visible in the UI rather than hidden.
    """
    name = (name_query or "").strip()
    place = (county_or_city or "").strip()

    if name or place:
        try:
            results, stats = cached_targeted_search(state, name, place)
            return results, "targeted", stats
        except Exception as e:  # operator unsupported, service error, etc.
            results, stats = bulk_search(state, name, place or None)
            return results, f"fallback ({e.__class__.__name__}: {e})", stats

    results, stats = bulk_search(state, name, place or None)
    return results, "full-state", stats


# ---------------- UI ----------------

state = st.selectbox("State", STATES, index=STATES.index("CA") if "CA" in STATES else 0)

st.caption(
    "Searches are filtered server-side, then narrowed here. Check the "
    "'Matched on' column: 'served city' and 'county' mean the system serves "
    "that place; 'mailing address only' means just the operator's address "
    "matched."
)

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
                with st.spinner(f"Searching {state}…"):
                    matches, how, stats = run_search(state, name, county_city or None)
                st.session_state.matches = None if matches.empty else matches.reset_index(drop=True)
                st.session_state.search_how = how
                st.session_state.search_stats = stats

    # Show results + in-table single selection
    if st.session_state.matches is not None:
        st.subheader("Matches")

        how = st.session_state.get("search_how", "")
        stats = st.session_state.get("search_stats", []) or []
        incomplete = [s for s in stats if not s.complete]

        # Two different failures, previously conflated under one alarming
        # banner: a search that missed systems, versus a detail lookup that
        # failed for a system already listed.
        search_bad = [s for s in incomplete if "by PWSID" not in s.label]
        detail_bad = [s for s in incomplete if "by PWSID" in s.label]

        if search_bad:
            st.error(
                "**Systems may be missing.** A search query did not return "
                "everything the service holds — see 'How this result was "
                "fetched' below."
            )
        if detail_bad:
            st.warning(
                "All matching systems are listed, but county could not be "
                "retrieved for some of them (the per-system lookup failed after "
                "retries). Those rows show a blank County — re-run to retry."
            )
        if how == "full-state":
            st.info("Pulled the full state list — the slow path.")
        elif how.startswith("fallback"):
            st.warning(f"Targeted search unavailable, fell back to the full-state pull — {how}")

        if "MATCHED_ON" in st.session_state.matches.columns:
            counts = st.session_state.matches["MATCHED_ON"].value_counts()
            serves = int(counts.get("served city", 0) + counts.get("county", 0))
            mailing = int(counts.get("system address", 0))
            if mailing:
                st.caption(
                    f"All **{len(st.session_state.matches)}** matching systems are "
                    f"listed. SDWIS confirms **{serves}** as serving this place; "
                    f"the other **{mailing}** matched on the system's own address. "
                    "SDWIS records served-city sparsely, so that is not proof they "
                    "don't serve it — use the County column to judge."
                )

        with st.expander("How this result was fetched", expanded=bool(incomplete)):
            st.write(f"Path: **{how}**")
            st.caption(
                "The state is chained onto each search as PRIMACY_AGENCY_CODE "
                "(the regulating agency). Filtering on STATE_CODE instead would "
                "return systems regulated by other states whose operator posts "
                "mail from here. County comes from a per-system lookup."
            )
            if stats:
                for s in stats:
                    (st.error if not s.complete else st.write)(s.describe())
                st.caption(
                    "Every query above must say 'complete'. 'INCOMPLETE' means the "
                    "row-page cap was hit or a page errored, and the matches below "
                    "are a subset of what actually exists."
                )
            else:
                st.write("No fetch recorded (results served from cache).")

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
                "MATCHED_ON": st.column_config.TextColumn(
                    "Matched on",
                    help="Why this system is in the list, from its SDWIS "
                         "geographic-area record. 'served city' and 'county' "
                         "are confirmed by SDWIS. 'system address' means the "
                         "system's own address matched — check the County "
                         "column, since SDWIS records served-city sparsely.",
                ),
            },
            key="matches_editor",
        )
        selected_rows = edited[edited["Select"] == True]
        with col2:
            if st.button("Generate report for selected"):
                num = len(selected_rows)
                if num == 0:
                    st.error("Select at least one row.")
                elif num == 1:
                    # Single selection → generate one DOCX
                    pwsid_to_generate = str(selected_rows.iloc[0]["PWSID"])
                else:
                    # Multiple selections → build ZIP
                    pids = [str(x) for x in selected_rows["PWSID"].tolist()]
                    with st.spinner(f"Building {num} reports…"):
                        zip_path = generate_reports_zip(pids, cached_fetch_all_selected)
                    with open(zip_path, "rb") as f:
                        st.download_button(
                            "Download ZIP of Word reports",
                            data=f.read(),
                            file_name="SDWIS_Reports.zip",
                            mime="application/zip",
                        )
                    st.success(f"Created ZIP with {num} reports.")


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

# ---------------- Groundwater map ----------------
#
# Gated behind an expander that is closed by default AND checked before the
# module does any work. Streamlit runs the whole script on every rerun, so an
# always-rendered map would cost on every keystroke elsewhere in the page.

if pwsid_to_generate:
    with st.expander("Groundwater vulnerability & map", expanded=False):
        import gw_map
        gw_map.render(pwsid_to_generate)


with st.expander("Developer tools"):
    if st.button("Clear app cache"):
        st.cache_data.clear()
        st.success("Cache cleared. Next search will refetch.")

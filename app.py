# app.py — All states, fast search with state-scoped GA + multi-select ZIP

import os
import re
import io
import zipfile
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
st.write("Pick a state, optionally add a name and/or county/city, or enter a PWSID. Download one or many Word reports.")

STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY",
    "LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH",
    "OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY","PR","VI"
]

# ---------------- Caching ----------------

@st.cache_data(ttl=60*60*12)  # 12h
def get_ws_by_state(state: str) -> pd.DataFrame:
    """WATER_SYSTEM filtered server-side by STATE_CODE; keep city_name when present."""
    ws = pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", (state or "").upper())
    ws = df_upper(ws)
    keep = [c for c in ["PWSID", "PWS_NAME", "CITY_NAME"] if c in ws.columns]
    return ws[keep].drop_duplicates("PWSID") if keep else ws

@st.cache_data(ttl=60*60*12)  # 12h
def get_ga_by_state(state: str) -> pd.DataFrame:
    """
    GEOGRAPHIC_AREA filtered server-side by STATE_SERVED for the chosen state.
    This is MUCH smaller than pulling the entire GA table.
    Note: some systems lack STATE_SERVED; we’ll still show CITY from WS if available.
    """
    ga = pull_rows_filtered("GEOGRAPHIC_AREA", "STATE_SERVED", (state or "").upper())
    ga = df_upper(ga)
    keep = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED", "STATE_SERVED"] if c in ga.columns]
    ga = ga[keep] if keep else ga
    subset = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga.columns]
    return ga.drop_duplicates(subset=subset) if subset else ga.drop_duplicates()

@st.cache_data(ttl=60*60*12, max_entries=300)
def cached_fetch_all_selected(pwsid: str):
    return fetch_all_selected(pwsid)

# ---------------- Fast search (state-scoped GA) ----------------

def fast_search(state: str, name_query: str, county_or_city: str | None) -> pd.DataFrame:
    """
    - WS filtered by state (cached).
    - Optional name tokens (AND).
    - GA filtered by STATE_SERVED == state (cached on demand).
    - City shown from WS.CITY_NAME when present; county/city from GA else blank.
    """
    sc = (state or "").strip().upper()
    ws = get_ws_by_state(sc)  # PWSID, PWS_NAME, (maybe) CITY_NAME

    # Name filter (optional; AND)
    q = (name_query or "").strip()
    if q and "PWS_NAME" in ws.columns:
        tokens = re.findall(r"[A-Za-z0-9]+", q)
        if tokens:
            ws = ws[token_and_contains(ws["PWS_NAME"], tokens)]

    # Start building output with CITY from WS
    out = ws.copy()
    if "CITY_NAME" in out.columns:
        out["CITY"] = out["CITY_NAME"].fillna("").astype(str).str.strip()

    # If no county/city filter, just return WS view
    if not county_or_city:
        show = [c for c in ["PWSID", "PWS_NAME", "CITY"] if c in out.columns]
        if not show:
            show = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
        return out[show].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

    # County/city filter → fetch GA for this state (small) and match
    ga = get_ga_by_state(sc)
    if not ga.empty and "PWSID" in ga.columns:
        term = county_or_city.strip().lower()
        m_county = ga["COUNTY_SERVED"].astype(str).str.lower().str.contains(term, na=False) if "COUNTY_SERVED" in ga.columns else False
        m_citysv = ga["CITY_SERVED"].astype(str).str.lower().str.contains(term, na=False)   if "CITY_SERVED"   in ga.columns else False
        ga_match = ga[m_county | m_citysv]
        merge_cols = [c for c in ["PWSID", "CITY_SERVED", "COUNTY_SERVED"] if c in ga_match.columns]
        if merge_cols:
            out = out.merge(ga_match[merge_cols], on="PWSID", how="inner")
            # Fill CITY from GA if WS city was missing
            if "CITY" not in out.columns:
                out["CITY"] = ""
            if "CITY_SERVED" in out.columns:
                out["CITY"] = out["CITY"].mask(out["CITY"].eq(""), out["CITY_SERVED"].fillna("").astype(str).str.strip())
    # Prepare final columns
    show = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in out.columns]
    if not show:
        show = [c for c in ["PWSID", "PWS_NAME"] if c in out.columns]
    return out[show].drop_duplicates("PWSID").sort_values("PWS_NAME").reset_index(drop=True)

# ---------------- UI ----------------

state = st.selectbox("State", STATES, index=STATES.index("AK") if "AK" in STATES else 0)

# Warm only WS cache (fast). DO NOT warm GA here (that’s what caused the long initial load).
with st.spinner(f"Loading water systems for {state}…"):
    _ = get_ws_by_state(state)

mode = st.radio("Lookup by", ["PWSID", "Name / County or City"], horizontal=True)

# storage for generated output (multi-select path)
if "generated_reports" not in st.session_state:
    st.session_state.generated_reports = None

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

    if st.button("Search"):
        if not name.strip() and not county_city.strip():
            st.warning("Enter a system name, OR a county/city.")
        else:
            with st.spinner(f"Searching {state} systems…"):
                matches = fast_search(state, name, county_city or None)
            st.session_state.matches = None if matches.empty else matches.reset_index(drop=True)
            st.session_state.generated_reports = None  # clear old outputs

    # Show table with multi-select
    if st.session_state.matches is not None:
        st.subheader("Matches")
        show_cols = [c for c in ["PWSID", "PWS_NAME", "CITY", "COUNTY_SERVED"] if c in st.session_state.matches.columns]
        df = st.session_state.matches[show_cols].copy()

        # Add checkbox column
        if "Select" not in df.columns:
            df.insert(0, "Select", False)

        # Quick local filter
        st.write("Tip: filter by PWSID, name, city, or county (multiple words = AND).")
        q = st.text_input("Filter rows", key="quick_filter").strip()
        if q:
            tokens = [t for t in re.findall(r"[A-Za-z0-9]+", q) if t]
            if tokens:
                hay = df.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                m = pd.Series(True, index=df.index)
                for t in tokens:
                    m &= hay.str.contains(re.escape(t.lower()), na=False)
                df = df[m]

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
                    help="Tick any number of systems to generate reports for",
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
        st.caption(f"{len(selected_rows)} selected")

        if st.button("Generate report(s) for selected)"):
            if len(selected_rows) == 0:
                st.error("Select at least one system.")
            else:
                gen_files: dict[str, bytes] = {}
                with st.spinner("Generating reports…"):
                    for _, row in selected_rows.iterrows():
                        pid = str(row["PWSID"])
                        data = cached_fetch_all_selected(pid)
                        tmpdir = tempfile.mkdtemp()
                        outpath = os.path.join(tmpdir, f"{pid}_SDWIS_Report.docx")
                        outpath = generate_report(pid, data, out_path=outpath)
                        with open(outpath, "rb") as f:
                            gen_files[os.path.basename(outpath)] = f.read()

                # bundle into a ZIP
                zip_buf = io.BytesIO()
                with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for fname, content in gen_files.items():
                        zf.writestr(fname, content)
                zip_buf.seek(0)

                st.session_state.generated_reports = {
                    "zip_bytes": zip_buf.getvalue(),
                    "files": gen_files,
                }
                st.success(f"Generated {len(gen_files)} report(s). See download options below.")

# ---------------- Single-PWSID path ----------------

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

# ---------------- Multi-downloads ----------------

if st.session_state.generated_reports:
    st.divider()
    st.subheader("Download your reports")
    st.download_button(
        "Download all as ZIP",
        data=st.session_state.generated_reports["zip_bytes"],
        file_name="sdwis_reports.zip",
        mime="application/zip",
        key="zip_dl",
    )
    with st.expander("Download individual files"):
        for fname, content in st.session_state.generated_reports["files"].items():
            st.download_button(
                f"Download {fname}",
                data=content,
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"dl_{fname}",
            )

with st.expander("Developer tools"):
    if st.button("Clear app cache"):
        st.cache_data.clear()
        st.success("Cache cleared. Next search will refetch.")

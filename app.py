# -*- coding: utf-8 -*-
"""
Created on Tue Aug 12 11:21:27 2025

@author: bjennings
"""

import os
import tempfile
import streamlit as st
import pandas as pd

from sdwis_ca_report import (
    looks_like_pwsid,
    search_by_name_ca,
    fetch_all_selected,
    generate_report,
)

st.set_page_config(page_title="SDWIS CA – Report Generator", layout="centered")
st.title("SDWIS California – Report Generator")

st.write("Generate a Word report for a California water system by PWSID or by searching its name.")

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
                    matches = search_by_name_ca(name, county_filter=(county or None))
                if matches is None or matches.empty:
                    st.warning("No matches. Try different words or remove the county filter.")
                    st.session_state.matches = None
                else:
                    st.session_state.matches = matches.reset_index(drop=True)

    if st.session_state.matches is not None:
        st.subheader("Matches")
        # Show a readable table
        show_cols = [c for c in ["PWSID","PWS_NAME","CITY_SERVED","COUNTY_SERVED"] if c in st.session_state.matches.columns]
        st.dataframe(st.session_state.matches[show_cols])
        # Let the user pick one
        idx = st.selectbox(
            "Pick a system",
            options=list(range(len(st.session_state.matches))),
            format_func=lambda i: f"{st.session_state.matches.iloc[i]['PWSID']} — {st.session_state.matches.iloc[i].get('PWS_NAME','')}",
        )
        with col2:
            if st.button("Generate report"):
                pwsid = st.session_state.matches.iloc[idx]["PWSID"]

# If we have a chosen PWSID, build & offer the download
if pwsid:
    with st.spinner(f"Fetching SDWIS data for {pwsid}..."):
        data = fetch_all_selected(pwsid)

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

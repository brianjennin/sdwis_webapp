"""Groundwater map and vulnerability index for one water system.

Reads files produced by the sibling pipeline (brianjennin/gw_vulnerability,
`python run.py export-app`) and dropped into data/gw/ here:

    sgti_by_pwsid.csv          index: which PWSIDs have a result
    <basin>_systems.geojson    service-area boundaries + their index value
    <basin>_wells.geojson      monitoring wells that carry a trend

No GeoPandas, GDAL or SpatiaLite: the pipeline does the spatial work, this
reads plain JSON and CSV. folium is imported inside render() rather than at
module scope, because importing it costs ~1.6 s and most sessions never open
the map.

Coverage is partial by nature. The app is nationwide; these results exist for
whichever California groundwater basins the pipeline has been run on. The
"nothing here for this system" path is therefore the common one and is handled
first.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parent / "data" / "gw"
INDEX_CSV = DATA_DIR / "sgti_by_pwsid.csv"

DECLINE = "#c62f2e"
RISE = "#256abf"
INK = "#1a1a19"


@st.cache_data(show_spinner=False)
def load_index() -> pd.DataFrame:
    """The PWSID -> result table, or empty if no data is bundled."""
    if not INDEX_CSV.exists():
        return pd.DataFrame()
    return pd.read_csv(INDEX_CSV, dtype={"pwsid": str})


@st.cache_data(show_spinner=False)
def load_geojson(name: str) -> dict:
    path = DATA_DIR / name
    if not path.exists():
        return {"type": "FeatureCollection", "features": []}
    return json.loads(path.read_text(encoding="utf-8"))


def lookup(pwsid: str) -> dict | None:
    """Row for this PWSID, or None if the pipeline has not covered it."""
    index = load_index()
    if index.empty:
        return None
    hit = index[index["pwsid"].str.upper() == str(pwsid).upper()]
    return None if hit.empty else hit.iloc[0].to_dict()


def bounds_of(geometry: dict) -> tuple[float, float, float, float]:
    """(min_lon, min_lat, max_lon, max_lat) for any GeoJSON geometry."""
    lons, lats = [], []

    def walk(node):
        if isinstance(node, (list, tuple)):
            if len(node) >= 2 and isinstance(node[0], (int, float)):
                lons.append(float(node[0]))
                lats.append(float(node[1]))
            else:
                for item in node:
                    walk(item)

    walk(geometry.get("coordinates", []))
    if not lons:
        return (0.0, 0.0, 0.0, 0.0)
    return (min(lons), min(lats), max(lons), max(lats))


def render(pwsid: str) -> None:
    """Draw the vulnerability summary and map for one system."""
    record = lookup(pwsid)

    if record is None:
        index = load_index()
        if index.empty:
            st.info(
                "No groundwater analysis is bundled with this app. Results are "
                "produced by the companion pipeline and copied into `data/gw/`."
            )
        else:
            basins = ", ".join(sorted(index["basin_name"].dropna().unique()))
            st.info(
                f"**No groundwater trend available for {pwsid}.** The analysis "
                f"currently covers {basins} — {len(index)} systems. Systems "
                f"outside those basins have no result."
            )
        return

    sgti = float(record["sgti_ft_per_yr"])
    rank = int(record["vuln_rank"])
    total = int(record["systems_ranked"])
    n_wells = int(record["n_wells_used"])
    n_sig = int(record["n_wells_significant"])
    radius_m = int(record["search_radius_m"])
    since = record.get("trend_start_year")

    direction = "declining" if sgti < 0 else "rising"
    left, middle, right = st.columns(3)
    left.metric("Groundwater trend", f"{sgti:+.2f} ft/yr",
                help="System Groundwater Trend Index: inverse-distance-weighted "
                     "mean of nearby well trends. Negative = water levels falling.")
    middle.metric("Rank in basin", f"{rank} of {total}",
                  help="1 = steepest decline in the basin.")
    right.metric("Wells used", f"{n_wells}",
                 help=f"Monitoring wells within {radius_m/1000:.0f} km of the "
                      f"service area. {n_sig} show a statistically significant trend.")

    st.caption(
        f"Water levels beneath this system are **{direction}** at "
        f"{abs(sgti):.2f} ft/yr{f' since {int(since)}' if pd.notna(since) else ''}, "
        f"from {n_wells} monitoring well{'s' if n_wells != 1 else ''} within "
        f"{radius_m/1000:.0f} km ({n_sig} statistically significant). "
        f"{record['basin_name']}."
    )
    if n_wells <= 2:
        st.warning(
            f"This result rests on {n_wells} well"
            f"{'s' if n_wells != 1 else ''}. Treat it as indicative only — "
            "removing a single well can move a system several places in the "
            "ranking."
        )

    _draw_map(pwsid, record)

    st.caption(
        "Screening only. Regional water levels are not a substitute for a "
        "system's own well records: exposure depends on well depth, screen "
        "interval and pump setting, none of which are in this data."
    )


def _draw_map(pwsid: str, record: dict) -> None:
    """Service area plus the wells that fed its index."""
    import folium                                  # ~1.6 s; only when opened
    from streamlit_folium import folium_static

    slug = record["basin_slug"]
    systems = load_geojson(f"{slug}_systems.geojson")
    wells = load_geojson(f"{slug}_wells.geojson")

    target = next((f for f in systems["features"]
                   if str(f["properties"].get("pwsid", "")).upper()
                   == str(pwsid).upper()), None)
    if target is None:
        st.caption("No mapped boundary for this system.")
        return

    min_lon, min_lat, max_lon, max_lat = bounds_of(target["geometry"])
    # Degrees per metre, near enough at this latitude for a display buffer.
    pad = (record["search_radius_m"] / 111_000) * 1.15

    fmap = folium.Map(tiles="CartoDB positron", control_scale=True)
    fmap.fit_bounds([[min_lat - pad, min_lon - pad],
                     [max_lat + pad, max_lon + pad]])

    folium.GeoJson(
        target,
        name="Service area",
        style_function=lambda _: {"fillColor": "#1f3b57", "color": "#1f3b57",
                                  "weight": 2, "fillOpacity": 0.18},
        tooltip=folium.GeoJsonTooltip(
            fields=["pws_name", "pwsid"], aliases=["System", "PWSID"]),
    ).add_to(fmap)

    # Only wells inside the padded box: the rest of the basin is not this
    # system's evidence, and every extra marker is payload on every rerun.
    shown = 0
    for feature in wells["features"]:
        lon, lat = feature["geometry"]["coordinates"][:2]
        if not (min_lon - pad <= lon <= max_lon + pad
                and min_lat - pad <= lat <= max_lat + pad):
            continue
        props = feature["properties"]
        slope = props.get("slope_ft_per_yr")
        if slope is None:
            continue
        significant = (props.get("p_value") or 1.0) <= 0.05
        folium.CircleMarker(
            location=[lat, lon],
            radius=4 + min(abs(slope), 6) * 0.8,
            color=INK if significant else "#ffffff",
            weight=1.2 if significant else 0.8,
            fill=True,
            fill_color=DECLINE if slope < 0 else RISE,
            fill_opacity=0.85,
            tooltip=(f"{props.get('station_id', 'well')}<br>"
                     f"{slope:+.2f} ft/yr"
                     f"{'' if significant else ' (not significant)'}<br>"
                     f"{props.get('n_measurements', '?')} measurements"),
        ).add_to(fmap)
        shown += 1

    # returned_objects=[] via folium_static: no rerun on pan or zoom, which
    # would otherwise re-render the whole page every time the map is touched.
    folium_static(fmap, width=700, height=460)
    st.caption(
        f"Service area in blue; {shown} monitoring well"
        f"{'s' if shown != 1 else ''} shown. Red = water level falling, "
        "blue = rising; larger means faster. A dark outline marks a "
        "statistically significant trend."
    )

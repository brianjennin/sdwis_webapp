"""Groundwater map and vulnerability index for one water system.

Reads files produced by the sibling pipeline (brianjennin/gw_vulnerability,
`python run.py export-app`) and dropped into data/gw/ here:

    sgti_by_pwsid.csv            index: which PWSIDs have a result
    <basin>_systems.geojson      service-area boundaries + their index value
    <basin>_wells.geojson        monitoring wells that carry a trend
    <basin>_system_wells.json    which wells fed which system's index

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
import math
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


@st.cache_data(show_spinner=False)
def load_membership(slug: str) -> dict:
    """{pwsid: [[station_id, distance_m], ...]} as the pipeline computed it.

    Empty for an export made before this file existed; _draw_map falls back to
    measuring distances itself in that case.
    """
    path = DATA_DIR / f"{slug}_system_wells.json"
    if not path.exists():
        return {}
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



def _to_metres(lon, lat, lon0, lat0):
    """Local planar approximation, good to a fraction of a percent at 2 km."""
    return ((lon - lon0) * 111_320.0 * math.cos(math.radians(lat0)),
            (lat - lat0) * 110_540.0)


def _rings(geometry: dict) -> list[list]:
    """Every ring of a Polygon or MultiPolygon, as coordinate lists."""
    kind = geometry.get("type")
    coords = geometry.get("coordinates", [])
    if kind == "Polygon":
        return list(coords)
    if kind == "MultiPolygon":
        return [ring for polygon in coords for ring in polygon]
    if kind == "Point":
        return [[coords]]
    return []


def distance_to_geometry_m(lon: float, lat: float, geometry: dict) -> float:
    """Metres from a point to a service-area geometry; 0 if inside it.

    The index counts wells within `search_radius_m` of the service-area
    BOUNDARY, so the map has to measure the same thing. Filtering on a
    bounding box instead draws wells that never fed the number, which reads
    as a contradiction when the caption says six wells and the map shows ten.
    """
    rings = _rings(geometry)
    if not rings:
        return float("inf")

    lon0, lat0 = lon, lat
    best = float("inf")
    for ring in rings:
        points = [_to_metres(c[0], c[1], lon0, lat0) for c in ring if len(c) >= 2]
        if not points:
            continue
        if len(points) == 1:                       # placeholder point geometry
            best = min(best, math.hypot(*points[0]))
            continue

        # Inside the ring counts as zero distance, matching ST_Distance.
        inside = False
        for i in range(len(points)):
            x1, y1 = points[i]
            x2, y2 = points[(i + 1) % len(points)]
            if (y1 > 0) != (y2 > 0):
                cross = x1 + (0 - y1) * (x2 - x1) / (y2 - y1)
                if cross > 0:
                    inside = not inside
            best = min(best, _point_to_segment(x1, y1, x2, y2))
        if inside:
            return 0.0
    return best


def _point_to_segment(x1: float, y1: float, x2: float, y2: float) -> float:
    """Distance from the origin to the segment (x1,y1)-(x2,y2)."""
    dx, dy = x2 - x1, y2 - y1
    length2 = dx * dx + dy * dy
    if length2 == 0:
        return math.hypot(x1, y1)
    t = max(0.0, min(1.0, -(x1 * dx + y1 * dy) / length2))
    return math.hypot(x1 + t * dx, y1 + t * dy)


ALPHA = 0.05          # matches the pipeline's config.yml


def is_significant(props: dict) -> bool:
    """Whether this well's Mann-Kendall trend clears the significance level.

    Written out rather than `props.get("p_value") or 1.0` because a strongly
    trending well has a p-value that rounds to 0.0 on export, and 0.0 is falsy
    -- the shorthand read it as p = 1.0 and labelled the clearest trends in the
    dataset "not significant".
    """
    p_value = props.get("p_value")
    return p_value is not None and float(p_value) <= ALPHA


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
                      f"service area, weighted by distance. Hover a well on "
                      f"the map for its trend and significance.")

    st.caption(
        f"Water levels beneath this system are **{direction}** at "
        f"{abs(sgti):.2f} ft/yr{f' since {int(since)}' if pd.notna(since) else ''}, "
        f"from {n_wells} monitoring well{'s' if n_wells != 1 else ''} within "
        f"{radius_m/1000:.0f} km. {record['basin_name']}."
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


def select_wells(pwsid: str, record: dict, target: dict,
                 wells: dict) -> tuple[list, list]:
    """Split the basin's wells into (fed this index, shown for context).

    Each entry is (lon, lat, properties, metres_from_service_area).
    """
    # Which wells fed this system's index is read from the pipeline, not
    # re-derived here. Measuring from the exported polygon disagrees with
    # ST_Distance by a metre or two -- the polygon is simplified and this is a
    # planar approximation -- which is enough to add or drop a well sitting on
    # the radius, and the map then contradicts its own caption.
    radius = float(record["search_radius_m"])
    membership = load_membership(record["basin_slug"]).get(pwsid.upper())

    used, nearby = [], []
    if membership is not None:
        distances = {station: metres for station, metres in membership}
        for feature in wells["features"]:
            props = feature["properties"]
            lon, lat = feature["geometry"]["coordinates"][:2]
            if props.get("slope_ft_per_yr") is None:
                continue
            metres = distances.get(props.get("station_id"))
            if metres is not None:
                used.append((lon, lat, props, float(metres)))
            else:
                context = distance_to_geometry_m(lon, lat, target["geometry"])
                if context <= radius * 2.5:
                    nearby.append((lon, lat, props, context))
    else:
        # Export predates the membership file: measure, and accept that a well
        # on the radius may be classified differently than the index did.
        for feature in wells["features"]:
            props = feature["properties"]
            lon, lat = feature["geometry"]["coordinates"][:2]
            if props.get("slope_ft_per_yr") is None:
                continue
            metres = distance_to_geometry_m(lon, lat, target["geometry"])
            if metres <= radius:
                used.append((lon, lat, props, metres))
            elif metres <= radius * 2.5:
                nearby.append((lon, lat, props, metres))
    return used, nearby


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

    radius = float(record["search_radius_m"])
    used, nearby = select_wells(pwsid, record, target, wells)

    # Context first, so contributing wells draw on top of it.
    for lon, lat, props, metres in nearby:
        folium.CircleMarker(
            location=[lat, lon], radius=3,
            color="#9a9a96", weight=0.8, fill=True,
            fill_color="#ffffff", fill_opacity=0.55,
            tooltip=(f"{props.get('station_id', 'well')} — "
                     f"{metres/1000:.1f} km away<br>"
                     f"Outside the {radius/1000:.0f} km radius; "
                     f"not used for this system"),
        ).add_to(fmap)

    for lon, lat, props, metres in used:
        slope = props["slope_ft_per_yr"]
        significant = is_significant(props)
        folium.CircleMarker(
            location=[lat, lon],
            radius=4 + min(abs(slope), 6) * 0.8,
            color=INK if significant else "#ffffff",
            weight=1.4 if significant else 0.8,
            fill=True,
            fill_color=DECLINE if slope < 0 else RISE,
            fill_opacity=0.85,
            tooltip=(f"{props.get('station_id', 'well')}<br>"
                     f"{slope:+.2f} ft/yr"
                     f"{'' if significant else ' (not significant)'}<br>"
                     f"{metres/1000:.1f} km from the service area<br>"
                     f"{props.get('n_measurements', '?')} measurements"),
        ).add_to(fmap)

    folium_static(fmap, width=700, height=460)
    st.caption(
        f"Service area in blue. **{len(used)} well"
        f"{'s' if len(used) != 1 else ''} within "
        f"{radius/1000:.0f} km fed this system's index** — red = falling, "
        "blue = rising, larger means faster, a dark outline marks a "
        "statistically significant trend."
        + (f" {len(nearby)} further well{'s' if len(nearby) != 1 else ''} "
           "shown hollow for context; those are outside the radius and did "
           "not contribute." if nearby else "")
    )

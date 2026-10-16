# SDWIS Report Generator

Streamlit web app that generates Word reports for US public water systems from
EPA's Safe Drinking Water Information System (SDWIS).

## What it does

- Looks up any US public water system by PWSID, or by name + county/city, or
  from a browseable state list.
- Fetches system data live from the EPA Envirofacts API (water system info,
  geographic area, facilities, violations, treatment).
- Generates a formatted Word report per system, or a ZIP of many at once.
- For a limited set of California systems, shows a groundwater vulnerability
  map and index computed by a sibling project.

## Run it

```bash
pip install -r requirements.txt
streamlit run app.py
```

Open the URL Streamlit prints (default http://localhost:8501).

## Searching

Three modes, selected in the UI:

| Mode | Use when |
|---|---|
| **PWSID** | You already know the ID (`CA1010016`, `TX0010043`, etc.). One API call, no state preload. |
| **Name / County or City** | You know roughly what you're looking for. Searches server-side first (fast); falls back to a full-state pull only if that returns nothing. |
| **Browse state** | You want to pick from the whole list for a state. Preloads the state and filters in-memory. |

Multiple words in a name search are ANDed, case-insensitive
(`los angeles water` matches "City of Los Angeles Water System").

## Groundwater Vulnerability (CA, partial)

A tab shown only when the selected system has a precomputed result. Reads
GeoJSON and CSV committed under `data/gw/`, produced by the sibling pipeline
[`gw_vulnerability`](https://github.com/brianjennin/gw_vulnerability). Current
coverage: ~13 small community water systems in the Tule Subbasin. For every
other system, the tab is hidden. No GeoPandas or SpatiaLite dependency in this
app — the pipeline does the spatial work, the app just reads its output.

## Project layout

```
app.py                 Streamlit UI
sdwis_ca_report.py     Envirofacts fetchers + Word report builder
sdwis_code_maps.py     Generated code → description tables
gw_map.py              Groundwater vulnerability tab (lazy folium import)
data/gw/               Pipeline-produced GeoJSON + CSV
tests/, test_search.py Unit tests
TODO.md                Deferred items with enough context to pick up cold
```

## Data sources

- **EPA Envirofacts (SDWIS)** — `https://data.epa.gov/efservice/` — public, no key required.
- **SDWA_REF_CODE_VALUES.csv** — EPA's current code→description reference, bundled.
- **Groundwater layer** — produced offline by
  [`brianjennin/gw_vulnerability`](https://github.com/brianjennin/gw_vulnerability)
  from DWR CASGEM, DWR Bulletin 118, and SWRCB SABL.

## Development

```bash
pytest tests test_search.py
```

See `TODO.md` for deferred work. Open issues or PRs on
[GitHub](https://github.com/brianjennin/sdwis_webapp).

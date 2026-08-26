# TODO

Deferred work, with enough context to pick up cold.

## Add CW and CS to storage facility types

`generate_report` detects storage by facility type, currently:

```python
storage_codes = {"ST", "RS"}   # Storage, Reservoir
```

EPA's `SDWA_REF_CODE_VALUES.csv` has two more that are arguably storage:

- `CW` — Clear Well (treated-water storage basin)
- `CS` — Cistern

A system whose only storage is a clearwell shows no Storage section today,
unless its facility name happens to hit the name fallback (`TANK`, `TOWER`,
`STANDPIPE`, …).

This is a **definition change, not a bug fix** — it changes which facilities
appear in reports, so it needs a deliberate call rather than being folded into
unrelated work. When doing it, extend `test_storage_section_matching_*` and
check a system that actually has a clearwell.

## Groundwater Vulnerability section (sibling repo integration)

Planned but not built. `brianjennin/gw_vulnerability` computes a System
Groundwater Trend Index (SGTI) per PWSID for the Tule Subbasin.

Two integration points:

1. A "Groundwater Vulnerability" section in the Word report, reading SGTI for
   the report's PWSID from a small CSV committed here (`pwsid, sgti_ft_per_yr,
   rank, n_wells_used, radius_m, …`). Coverage is ~13 systems in one subbasin
   against a nationwide app, so the not-assessed path is the common case and
   should be written first.
2. An interactive map tab (folium + streamlit-folium, both pure Python) reading
   a GeoJSON exported by that pipeline. Deliberately keeps GeoPandas and
   SpatiaLite out of this app — only the pipeline's *output* is consumed.

Blocked on the pipeline's data-source URLs, which are unresolved.

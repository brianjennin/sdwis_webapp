# Groundwater data

Empty by design. These files are **outputs of a different repository** and are
copied in, not generated here.

Produce them in `brianjennin/gw_vulnerability`:

```bash
python run.py export-app
```

then copy `data/output/app/*` into this directory:

```bash
# Windows
copy ..\gw_vulnerability\data\output\app\* data\gw\

# macOS / Linux
cp ../gw_vulnerability/data/output/app/* data/gw/
```

Expected contents:

| File | What it is |
|---|---|
| `sgti_by_pwsid.csv` | Index: which PWSIDs have a result, and which basin file to load |
| `<basin>_systems.geojson` | Service-area boundaries and their index value |
| `<basin>_wells.geojson` | Monitoring wells carrying a trend |

Until they are present the map section says so and the rest of the app is
unaffected — `gw_map.render()` handles the empty case first.

**Do not copy fixture output.** The pipeline writes `*_fixture.*` names when
run against its synthetic test data; those are seeded random walks and would
appear here as though they were results.

Coverage grows one basin at a time: re-running the export for another basin
appends to the CSV and adds another pair of GeoJSON files.

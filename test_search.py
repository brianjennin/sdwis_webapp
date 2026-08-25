"""Tests for the Envirofacts search paths.

The live service cannot be assumed reachable (and was not, when this was
written), so these run against a stubbed HTTP layer. They pin the things that
would otherwise fail silently in production:

- the URL shape sent to Envirofacts, including non-overlapping row windows
- that anything the targeted path cannot answer RAISES, so the caller falls
  back to the bulk pull instead of showing an empty result
- that the bulk path REPORTS an incomplete pull instead of returning a prefix
  that looks like the whole table

    python test_search.py
"""
import sys
import types

sys.modules.setdefault("docx", types.ModuleType("docx"))
import sdwis_ca_report as R  # noqa: E402

WS_CSV = (
    "PWSID,PWS_NAME,CITY_NAME,STATE_CODE\n"
    "CA5400001,PORTERVILLE CITY WATER,PORTERVILLE,CA\n"
    "CA5400002,EAST PORTERVILLE MUTUAL,PORTERVILLE,CA\n"
)
GA_CSV = (
    "PWSID,CITY_SERVED,COUNTY_SERVED\n"
    "CA5400001,PORTERVILLE,TULARE\n"
    "CA5400002,PORTERVILLE,TULARE\n"
)


class _Resp:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass

    def json(self):
        raise ValueError("stub serves CSV only")


def _stub(calls, fail=False):
    def get(url, timeout=None):
        calls.append(url)
        if fail:
            raise RuntimeError("service unavailable")
        if not url.endswith("/CSV"):
            raise RuntimeError("stub serves CSV only")
        if "Rows/0:" not in url:
            return _Resp("")  # no second page
        return _Resp(GA_CSV if "GEOGRAPHIC_AREA" in url else WS_CSV)
    return types.SimpleNamespace(get=get)


# ---------------------------------------------------------------- targeted


def test_url_shape():
    path = R._ef_path("WATER_SYSTEM", [("STATE_CODE", "CA"),
                                       ("PWS_NAME", "CONTAINING", "PORTERVILLE")])
    assert path == "WATER_SYSTEM/STATE_CODE/CA/PWS_NAME/CONTAINING/PORTERVILLE", path


def test_name_search_is_one_request():
    calls = []
    R._session = _stub(calls)
    out, stats = R.search_systems_targeted("CA", "porterville", None)
    assert len(calls) == 1, calls
    assert "CONTAINING/PORTERVILLE" in calls[0]
    assert not out.empty
    assert all(s.complete for s in stats)


def test_longest_token_goes_server_side():
    calls = []
    R._session = _stub(calls)
    out, _ = R.search_systems_targeted("CA", "east porterville", None)
    # "porterville" is the selective token; "east" is ANDed locally.
    assert "CONTAINING/PORTERVILLE" in calls[0], calls[0]
    assert out["PWS_NAME"].tolist() == ["EAST PORTERVILLE MUTUAL"], out["PWS_NAME"].tolist()


def test_name_plus_place_narrows_both_sides():
    calls = []
    R._session = _stub(calls)
    out, _ = R.search_systems_targeted("CA", "porterville", "tulare")
    assert any("GEOGRAPHIC_AREA" in c for c in calls), calls
    assert not out.empty


def test_place_only_search_never_pulls_the_whole_state():
    """The regression this guards: a city search returning almost nothing.

    A place-only search must filter server-side on both WATER_SYSTEM.CITY_NAME
    and GEOGRAPHIC_AREA, and must never issue an unfiltered state pull.
    """
    calls = []
    R._session = _stub(calls)
    out, stats = R.search_systems_targeted("CA", None, "roseville")
    assert not out.empty
    assert any("CITY_NAME/CONTAINING/ROSEVILLE" in c for c in calls), calls
    assert any("GEOGRAPHIC_AREA" in c for c in calls), calls
    for c in calls:
        assert "CONTAINING" in c or "/IN/" in c, f"unfiltered pull leaked in: {c}"
    assert all(s.complete for s in stats)


def test_no_criteria_raises_so_caller_falls_back():
    R._session = _stub([])
    try:
        R.search_systems_targeted("CA", "", "")
    except ValueError:
        return
    raise AssertionError("a search with no criteria must raise, not return empty")


def test_service_failure_raises():
    R._session = _stub([], fail=True)
    try:
        R.search_systems_targeted("CA", "porterville", None)
    except Exception:
        return
    raise AssertionError("a failing service must raise so the caller falls back")


def test_targeted_page_failure_raises():
    """Mid-paging failure on the targeted path must raise, not return a prefix."""
    state = {"n": 0}

    def get(url, timeout=None):
        state["n"] += 1
        if state["n"] == 1:
            return _Resp(WS_CSV)
        raise RuntimeError("boom")

    R._session = types.SimpleNamespace(get=get)
    try:
        R.ef_query("WATER_SYSTEM", [("STATE_CODE", "CA")], page_size=2)
    except Exception:
        return
    raise AssertionError("a failed page must raise, never return partial rows")


# -------------------------------------------------------------------- bulk


def _with_fake_json(fake, fn):
    original, R.api_get_json = R.api_get_json, fake
    try:
        return fn()
    finally:
        R.api_get_json = original


def test_bulk_pull_flags_incomplete_instead_of_swallowing():
    """The Roseville regression.

    The bulk path returns whatever it got, so it MUST report when that is only
    a prefix. Previously an errored page was caught, printed to stdout nobody
    reads, and the partial frame returned as if it were the whole table -- then
    inner-joined against GEOGRAPHIC_AREA, silently dropping real matches.
    """
    state = {"n": 0}

    def fake(url):
        state["n"] += 1
        if state["n"] == 1:
            return [{"PWSID": f"CA{i:07d}", "PWS_NAME": "X"} for i in range(2)]
        raise RuntimeError("page 2 timed out")

    stats = []
    df = _with_fake_json(fake, lambda: R.pull_rows_filtered(
        "WATER_SYSTEM", "STATE_CODE", "CA", page_size=2, stats=stats))

    assert len(df) == 2, len(df)
    assert stats and not stats[0].complete, [s.describe() for s in stats]
    assert "failed" in stats[0].reason, stats[0].reason


def test_bulk_pull_flags_page_cap():
    """Hitting the page cap is also incomplete, not a clean finish."""
    stats = []
    _with_fake_json(
        lambda url: [{"PWSID": f"CA{i:07d}"} for i in range(2)],
        lambda: R.pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", "CA",
                                     page_size=2, max_pages=3, stats=stats),
    )
    assert not stats[0].complete and "cap" in stats[0].reason, stats[0].describe()


def test_row_windows_do_not_overlap():
    """Envirofacts row windows are inclusive, so pages must not re-fetch a row."""
    seen = []

    def fake(url):
        seen.append(url.split("/Rows/")[1].split("/")[0])
        return [{"PWSID": f"CA{i:07d}"} for i in range(2)] if len(seen) < 2 else []

    _with_fake_json(fake, lambda: R.pull_rows_filtered(
        "WATER_SYSTEM", "STATE_CODE", "CA", page_size=2))

    assert seen[0] == "0:1", seen
    assert seen[1] == "2:3", seen


def test_complete_pull_is_marked_complete():
    stats = []
    _with_fake_json(
        lambda url: [{"PWSID": "CA0000001"}] if "Rows/0:" in url else [],
        lambda: R.pull_rows_filtered("WATER_SYSTEM", "STATE_CODE", "CA",
                                     page_size=50, stats=stats),
    )
    assert stats[0].complete, stats[0].describe()


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"  ok  {t.__name__}")
    print(f"{len(tests)} passed")

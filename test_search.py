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
    """Stands in for a requests.Response.

    Search queries are served as CSV; the per-PWSID enrichment call goes
    through api_get_json and needs .json(), so both are supported.
    """
    def __init__(self, text, payload=None):
        self.text = text
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        if self._payload is None:
            raise ValueError("stub serves CSV only")
        return self._payload


PER_PWSID_GA = {
    "CA5400001": [{"PWSID": "CA5400001", "CITY_SERVED": "PORTERVILLE",
                   "COUNTY_SERVED": "TULARE"}],
    "CA5400002": [{"PWSID": "CA5400002", "CITY_SERVED": "PORTERVILLE",
                   "COUNTY_SERVED": "TULARE"}],
}


def _stub(calls, fail=False):
    def get(url, timeout=None, **kwargs):
        calls.append(url)
        if fail:
            raise RuntimeError("service unavailable")
        if "/PWSID/" in url:  # per-PWSID enrichment, served as JSON
            pwsid = url.split("/PWSID/")[1].split("/")[0]
            return _Resp("", payload=PER_PWSID_GA.get(pwsid, []))
        if not url.endswith("/CSV"):
            raise RuntimeError("stub serves CSV only")
        if "Rows/0:" not in url:
            return _Resp("")  # no second page
        return _Resp(GA_CSV if "GEOGRAPHIC_AREA" in url else WS_CSV)
    return types.SimpleNamespace(get=get)


def _filters_in(url: str) -> int:
    """How many column filters a request URL carries."""
    path = url.split("/efservice/")[1].split("/Rows/")[0].split("/JSON")[0].split("/CSV")[0]
    parts = [p for p in path.split("/") if p]
    return max(0, (len(parts) - 1 + 1) // 2) if len(parts) > 1 else 0


# ---------------------------------------------------------------- targeted


def test_url_shape():
    path = R._ef_path("WATER_SYSTEM", "PWS_NAME", "PORTERVILLE", "CONTAINING")
    assert path == "WATER_SYSTEM/PWS_NAME/CONTAINING/PORTERVILLE", path


def test_never_chains_two_filters():
    """Envirofacts ignores all but one filter in a chain, so never send two.

    Chaining STATE_CODE/CA with CITY_NAME/CONTAINING/ROSEVILLE returned Arizona
    and Idaho systems from the live service. The state is applied client-side
    instead; this pins that no request regains a second filter.
    """
    calls = []
    R._session = _stub(calls)
    R.search_systems_targeted("CA", None, "porterville")
    searches = [c for c in calls if "/Rows/" in c]
    assert searches, calls
    for c in searches:
        path = c.split("/efservice/")[1].split("/Rows/")[0]
        segments = [s for s in path.split("/") if s]
        # table + column [+ operator] + value  ->  at most 4 segments
        assert len(segments) <= 4, f"chained filter leaked back in: {path}"


def test_name_search_is_one_search_request():
    calls = []
    R._session = _stub(calls)
    out, stats = R.search_systems_targeted("CA", "porterville", None)
    searches = [c for c in calls if "/Rows/" in c]
    assert len(searches) == 1, searches
    assert "PWS_NAME/CONTAINING/PORTERVILLE" in searches[0]
    assert not out.empty


def test_longest_token_goes_server_side():
    calls = []
    R._session = _stub(calls)
    out, _ = R.search_systems_targeted("CA", "east porterville", None)
    # "porterville" is the selective token; "east" is ANDed locally.
    assert "CONTAINING/PORTERVILLE" in calls[0], calls[0]
    assert out["PWS_NAME"].tolist() == ["EAST PORTERVILLE MUTUAL"], out["PWS_NAME"].tolist()


def test_county_comes_from_per_pwsid_lookup():
    """COUNTY_SERVED was blank on every row. The per-PWSID call -- the one the
    Word report uses, and the only one Envirofacts honours -- fills it in."""
    calls = []
    R._session = _stub(calls)
    out, _ = R.search_systems_targeted("CA", "porterville", None)
    assert any("/PWSID/" in c and "GEOGRAPHIC_AREA" in c for c in calls), calls
    assert set(out["COUNTY_SERVED"]) == {"TULARE"}, out["COUNTY_SERVED"].tolist()
    assert set(out["MATCHED_ON"]) == {"system name"}


def test_name_plus_place_keeps_only_real_service_matches():
    calls = []
    R._session = _stub(calls)
    out, _ = R.search_systems_targeted("CA", "porterville", "tulare")
    assert not out.empty
    assert set(out["MATCHED_ON"]) <= {"served city", "county"}, out["MATCHED_ON"].tolist()


def test_place_search_uses_both_candidate_sources():
    """A place search must not depend on a system's mailing address alone:
    GEOGRAPHIC_AREA (serves the place) and WATER_SYSTEM.CITY_NAME are unioned."""
    calls = []
    R._session = _stub(calls)
    R.search_systems_targeted("CA", None, "porterville")
    assert any("GEOGRAPHIC_AREA/CITY_SERVED/CONTAINING" in c for c in calls), calls
    assert any("GEOGRAPHIC_AREA/COUNTY_SERVED/CONTAINING" in c for c in calls), calls
    assert any("WATER_SYSTEM/CITY_NAME/CONTAINING" in c for c in calls), calls


def test_out_of_state_rows_are_dropped():
    """Envirofacts has no state filter applied at all now (single filter only),
    so the state MUST be enforced client-side or AZ/ID systems leak in."""
    leaky = (
        "PWSID,PWS_NAME,CITY_NAME,STATE_CODE\n"
        "CA3110008,CITY OF ROSEVILLE,ROSEVILLE,CA\n"
        "AZ0415033,RV TRADERS,ROSEVILLE,AZ\n"
        "ID5420024,ROCK CREEK MOBILE MANOR,ROSEVILLE,ID\n"
    )

    def get(url, timeout=None, **kwargs):
        if "/PWSID/" in url:
            return _Resp("", payload=[])
        if not url.endswith("/CSV") or "Rows/0:" not in url:
            return _Resp("")
        return _Resp("PWSID,CITY_SERVED,COUNTY_SERVED\n" if "GEOGRAPHIC_AREA" in url else leaky)

    R._session = types.SimpleNamespace(get=get)
    out, stats = R.search_systems_targeted("CA", None, "roseville")
    assert out["PWSID"].tolist() == ["CA3110008"], out["PWSID"].tolist()
    assert any("outside CA" in s.reason for s in stats), [s.describe() for s in stats]


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

    def get(url, timeout=None, **kwargs):
        state["n"] += 1
        if state["n"] == 1:
            return _Resp(WS_CSV)
        raise RuntimeError("boom")

    R._session = types.SimpleNamespace(get=get)
    try:
        R.ef_query("WATER_SYSTEM", "STATE_CODE", "CA", page_size=2)
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

"""Tests for the targeted Envirofacts search path.

The live service cannot be assumed reachable (and was not, when this was
written), so these run against a stubbed HTTP layer. They pin the two things
that would otherwise fail silently in production: the URL shape sent to
Envirofacts, and the guarantee that anything the targeted path cannot answer
raises, so the caller falls back to the bulk pull instead of showing the user
an empty result.

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


def test_url_shape():
    path = R._ef_path("WATER_SYSTEM", [("STATE_CODE", "CA"),
                                       ("PWS_NAME", "CONTAINING", "PORTERVILLE")])
    assert path == "WATER_SYSTEM/STATE_CODE/CA/PWS_NAME/CONTAINING/PORTERVILLE", path


def test_name_search_is_one_request():
    calls = []
    R._session = _stub(calls)
    out = R.search_systems_targeted("CA", "porterville", None)
    assert len(calls) == 1, calls
    assert "CONTAINING/PORTERVILLE" in calls[0]
    assert not out.empty


def test_longest_token_goes_server_side():
    calls = []
    R._session = _stub(calls)
    out = R.search_systems_targeted("CA", "east porterville", None)
    # "porterville" is the selective token; "east" is ANDed locally.
    assert "CONTAINING/PORTERVILLE" in calls[0], calls[0]
    assert out["PWS_NAME"].tolist() == ["EAST PORTERVILLE MUTUAL"], out["PWS_NAME"].tolist()


def test_county_narrows_via_geographic_area():
    calls = []
    R._session = _stub(calls)
    out = R.search_systems_targeted("CA", "porterville", "tulare")
    assert any("GEOGRAPHIC_AREA" in c for c in calls), calls
    assert set(out["COUNTY_SERVED"]) == {"TULARE"}


def test_county_only_raises_so_caller_falls_back():
    R._session = _stub([])
    for bad in ("", "   "):
        try:
            R.search_systems_targeted("CA", bad, "tulare")
        except ValueError:
            continue
        raise AssertionError("county-only search must raise, not return empty")


def test_service_failure_raises():
    calls = []
    R._session = _stub(calls, fail=True)
    try:
        R.search_systems_targeted("CA", "porterville", None)
    except Exception:
        return
    raise AssertionError("a failing service must raise so the caller falls back")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"  ok  {t.__name__}")
    print(f"{len(tests)} passed")

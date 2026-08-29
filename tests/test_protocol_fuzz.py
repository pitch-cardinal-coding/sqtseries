"""Fuzz-lite protocol tests: arbitrary/malformed inputs must never crash.
The wire format is untrusted input from any client, so every parser must
either accept or raise a ProtocolError — never an AttributeError/TypeError
from unexpected shapes.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from sqtseries.messaging.protocol import (
    ProtocolError,
    dumps,
    loads,
    parse_admin,
    parse_ingest,
    parse_query,
)

_SETTINGS = settings(max_examples=50, deadline=2000)

_any_json = st.recursive(
    st.one_of(
        st.none(),
        st.booleans(),
        # orjson only serializes integers within the 64-bit range
        st.integers(min_value=-(2**63), max_value=2**63 - 1),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(),
    ),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=20,
)


def _only_protocol_error(fn, raw):
    try:
        fn(raw)
    except ProtocolError:
        # expected for malformed input
        pass
    # any other exception is a bug
    except Exception as exc:
        raise AssertionError(f"{type(exc).__name__}: {exc} for input {raw!r}") from None


@_SETTINGS
@given(raw=_any_json)
def test_parse_ingest_never_crashes(raw):
    _only_protocol_error(parse_ingest, raw)


@_SETTINGS
@given(raw=_any_json)
def test_parse_query_never_crashes(raw):
    _only_protocol_error(parse_query, raw)


@_SETTINGS
@given(raw=_any_json)
def test_parse_admin_never_crashes(raw):
    _only_protocol_error(parse_admin, raw)


@_SETTINGS
@given(raw=_any_json)
def test_ingest_valid_shape_accepted_or_protocol(raw):
    """Even deeply-nested dicts must not raise anything but ProtocolError."""

    _only_protocol_error(parse_ingest, raw)


@_SETTINGS
@given(data=st.binary(max_size=512))
def test_loads_arbitrary_bytes(data):
    """loads must only ever raise ProtocolError (or succeed)."""
    try:
        loads(data)
    except ProtocolError:
        pass
    except Exception as exc:
        raise AssertionError(f"loads raised {type(exc).__name__}: {exc}") from None


@_SETTINGS
@given(value=_any_json)
def test_dumps_loads_roundtrip(value):
    """Anything dumps() serializes, loads() must decode back."""
    loaded = loads(dumps(value))
    assert loaded == value


@_SETTINGS
@given(raw=_any_json)
def test_query_with_int_bounds(raw):
    """parse_query validates start/end are ints; must not raise non-Protocol."""

    if isinstance(raw, dict):
        _only_protocol_error(parse_query, raw)

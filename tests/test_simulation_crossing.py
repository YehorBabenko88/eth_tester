from decimal import Decimal as D
import pytest
from eth_tester.simulation.crossing import resolve_first_crossing
from test_simulation_execution import BASE


def test_equal_timestamp_stop_first_and_worst_print():
    r = resolve_first_crossing({'ts': BASE, 'low_cross': D(95),
                                'high_cross': D(107)}, 'long', 98, 104)
    assert r['reason'] == 'stop' and r['price'] == D(95)
    assert r['ambiguous_same_timestamp']
    r = resolve_first_crossing({'ts': BASE, 'low_cross': D(91),
                                'high_cross': D(110)}, 'short', 108, 94)
    assert r['reason'] == 'stop' and r['price'] == D(110)
    assert r['ambiguous_same_timestamp']


def test_only_target_is_capped_to_target():
    assert resolve_first_crossing({'ts': BASE, 'low_cross': None,
                                   'high_cross': D(110)}, 'long', 98, 104)['price'] == 104
    assert resolve_first_crossing({'ts': BASE, 'low_cross': D(89),
                                   'high_cross': None}, 'short', 108, 94)['price'] == 94


def test_no_crossing_does_not_invent_fill():
    assert resolve_first_crossing(None, 'long', 98, 104) is None
    assert resolve_first_crossing({'ts': BASE, 'low_cross': None,
                                   'high_cross': None}, 'long', 98, 104) is None
    with pytest.raises(ValueError):
        resolve_first_crossing({'ts': BASE}, 'other', 98, 104)

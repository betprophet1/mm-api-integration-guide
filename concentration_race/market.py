"""Market/line discovery for the concentration-race scenario.

Thin wrapper around test_deduce_race_conditions's market helpers — reused
rather than duplicated, since get_market_for_event/get_opposite_line_id
already handle event lookup and opposite-side resolution correctly.
"""
from test_deduce_race_conditions import get_market_for_event, get_opposite_line_id


def find_market(framework, account_name, event_id=None):
    """Return market_info for event_id, or auto-discover if event_id is None."""
    if event_id:
        return get_market_for_event(framework, account_name, event_id)
    return framework.get_available_market(account_name)


def resolve_opposite_line(framework, account_name, event_id, line_id):
    """Return the opposite line_id for a market, or None if not found."""
    return get_opposite_line_id(framework, account_name, event_id, line_id)

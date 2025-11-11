from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from .leg_matching import MatchedLegLot

PeriodType = Literal["daily", "weekly", "monthly", "total"]


def _group_date_to_period_key(activity_date: date, period_type: PeriodType) -> tuple[str, str]:
    """Map an activity date to the requested period key/label pair."""
    if period_type == "daily":
        return activity_date.isoformat(), activity_date.strftime("%Y-%m-%d")

    if period_type == "weekly":
        iso_year, iso_week, _ = activity_date.isocalendar()
        period_key = f"{iso_year}-W{iso_week:02d}"
        period_label = f"Week {iso_week}, {iso_year}"
        return period_key, period_label

    if period_type == "monthly":
        return activity_date.strftime("%Y-%m"), activity_date.strftime("%B %Y")

    return "total", "Total"


def _date_in_range(
    check_date: date, since: Optional[date] = None, until: Optional[date] = None
) -> bool:
    """Return True if `check_date` falls within the optional bounds."""
    if since is not None and check_date < since:
        return False
    if until is not None and check_date > until:
        return False
    return True


def _lot_overlaps_date_range(opened_at: date, until: Optional[date] = None) -> bool:
    """Determine whether an open lot was started before or during the requested range."""
    if until is not None and opened_at > until:
        return False
    return True


def _lot_was_open_during_period(lot: MatchedLegLot, until: Optional[date] = None) -> bool:
    """Return True if the lot was open (or closing after the period) so we include unrealized exposure."""
    if lot.is_open:
        return True
    if until is None:
        return False
    if lot.closed_at and lot.closed_at > until:
        return True
    return False


def _lot_closed_by_assignment(lot: MatchedLegLot) -> bool:
    """Return True if every closing portion for the lot comes from an assignment."""
    if not lot.is_closed or not lot.close_portions:
        return False
    return all(portion.fill.is_assignment for portion in lot.close_portions)


def _parse_period_key_to_date(period_key: str, period_type: PeriodType) -> Optional[date]:
    """Convert a period key back into a representative date when possible."""
    try:
        if period_type == "daily":
            return date.fromisoformat(period_key)
        if period_type == "weekly":
            year, week = period_key.split("-W")
            return date.fromisocalendar(int(year), int(week), 1)
        if period_type == "monthly":
            year, month = period_key.split("-")
            return date(int(year), int(month), 1)
    except (ValueError, AttributeError):
        return None
    return None


def _clamp_period_to_range(period_key: str, period_type: PeriodType, since: Optional[date]) -> str:
    """
    Clamp a period key so that periods before the `since` date map to the first period inside the range.
    """
    if since is None or period_type == "total":
        return period_key

    first_period_key, _ = _group_date_to_period_key(since, period_type)
    period_date = _parse_period_key_to_date(period_key, period_type)
    first_date = _parse_period_key_to_date(first_period_key, period_type)

    if period_date is None or first_date is None:
        return first_period_key if period_key < first_period_key else period_key

    return first_period_key if period_date < first_date else period_key

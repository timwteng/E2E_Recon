"""Decide which business date a run reconciles.

Databricks reports job times in UTC, so a date taken straight from the job clock
can be a day off from the bank's calendar. Scheduled runs therefore derive the
date in the business timezone; any other run must say which date it means.
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from e2e_recon.config import BusinessCalendar

SCHEDULED_TRIGGER = "periodic"  # value of {{job.trigger.type}} for cron schedules


class BusinessDateError(ValueError):
    pass


def resolve_business_date(
    explicit: str | None,
    trigger_type: str | None,
    trigger_time: str | None,
    calendar: BusinessCalendar,
) -> date:
    """An explicit date always wins. Otherwise only a scheduled run may derive one."""
    if explicit:
        return date.fromisoformat(explicit)
    if trigger_type != SCHEDULED_TRIGGER:
        raise BusinessDateError(
            f"No business date given for a '{trigger_type or 'manual'}' run. Pass one explicitly, "
            "e.g. databricks bundle run e2e_recon_daily --params business_date=2026-10-30"
        )
    if not trigger_time:
        raise BusinessDateError("Scheduled run is missing its trigger time")
    try:
        triggered = datetime.fromisoformat(trigger_time)
    except ValueError as exc:
        raise BusinessDateError(f"Unrecognised trigger time '{trigger_time}'") from exc
    if triggered.tzinfo is None:
        triggered = triggered.replace(tzinfo=timezone.utc)
    local_day = triggered.astimezone(ZoneInfo(calendar.timezone)).date()
    return local_day - timedelta(days=calendar.lag_days)

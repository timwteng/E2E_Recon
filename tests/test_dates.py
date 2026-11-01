from datetime import date

import pytest

from e2e_recon.cli import EXIT_BAD_DATE, main
from e2e_recon.config import BusinessCalendar
from e2e_recon.dates import BusinessDateError, resolve_business_date

SGT = BusinessCalendar("Asia/Singapore", lag_days=1)


@pytest.mark.parametrize(
    "trigger_time, expected",
    [
        ("2026-11-01T22:00:00Z", date(2026, 11, 1)),  # 06:00 SGT on 2 Nov -> reconcile 1 Nov
        ("2026-11-01T22:00:00", date(2026, 11, 1)),  # no offset: treated as UTC
        ("2026-11-01T22:00:00.000+00:00", date(2026, 11, 1)),
        ("2026-11-01T15:59:00Z", date(2026, 10, 31)),  # 23:59 SGT on 1 Nov -> reconcile 31 Oct
    ],
)
def test_scheduled_run_reconciles_previous_business_day_in_local_time(trigger_time, expected):
    assert resolve_business_date("", "periodic", trigger_time, SGT) == expected


def test_explicit_date_always_wins():
    assert resolve_business_date("2026-10-30", "periodic", "2026-11-01T22:00:00Z", SGT) == date(2026, 10, 30)
    assert resolve_business_date("2026-10-30", "one_time", "", SGT) == date(2026, 10, 30)


@pytest.mark.parametrize("trigger_type", ["one_time", "run_job_task", "file_arrival", "", None])
def test_non_scheduled_run_without_date_is_refused(trigger_type):
    with pytest.raises(BusinessDateError, match="Pass one explicitly"):
        resolve_business_date("", trigger_type, "2026-11-02T02:00:00Z", SGT)


def test_unreadable_trigger_time_is_refused():
    with pytest.raises(BusinessDateError, match="Unrecognised trigger time"):
        resolve_business_date("", "periodic", "yesterday-ish", SGT)


def test_lag_days_is_configurable():
    same_day = BusinessCalendar("Asia/Singapore", lag_days=0)
    assert resolve_business_date("", "periodic", "2026-11-01T22:00:00Z", same_day) == date(2026, 11, 2)


def test_unknown_timezone_rejected():
    with pytest.raises(Exception):
        BusinessCalendar("Mars/Olympus_Mons")


def test_cli_manual_run_without_date_exits_before_touching_spark():
    assert main(["check", "--catalog", "workspace", "--schema", "e2e_recon", "--trigger-type", "one_time"]) == EXIT_BAD_DATE

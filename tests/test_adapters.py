from decimal import Decimal

import pytest

from conftest import BUSINESS_DATE
from e2e_recon.adapters import SystemAdapter
from e2e_recon.synthetic import STANDARD_SCHEMA

ROWS = [
    ("T1", BUSINESS_DATE, "OUT", "PURCHASE", "SGD", Decimal("12.34")),
    ("T2", BUSINESS_DATE, "IN", "REPAYMENT", "USD", Decimal("1500.00")),
    ("T3", BUSINESS_DATE, "OUT", "FEE", "TWD", Decimal("0.05")),
]


@pytest.mark.parametrize("system_index", [0, 1, 2, 3])
def test_encode_then_standardize_round_trips(spark, config, tmp_path, system_index):
    adapter = SystemAdapter(config.systems[system_index])
    path = adapter.landing_path(str(tmp_path), BUSINESS_DATE)
    adapter.write_raw(adapter.encode(spark.createDataFrame(ROWS, STANDARD_SCHEMA)), path)

    valid, rejected = adapter.standardize(adapter.read_raw(spark, path, BUSINESS_DATE))

    got = sorted(tuple(r) for r in valid.select(*STANDARD_SCHEMA.fieldNames()).collect())
    assert got == sorted(ROWS)
    assert rejected.count() == 0
    assert {r["source_file"] for r in valid.collect()} != {None}


def test_bad_rows_are_rejected_with_reason(spark, config, tmp_path):
    adapter = SystemAdapter(config.systems[0])  # card_switch: yyyyMMdd, minor units, coded values
    path = str(tmp_path / "bad")
    spark.createDataFrame(
        [
            ("T1", "20261030", "D", "05", "SGD", "1234"),  # valid
            ("", "20261030", "D", "05", "SGD", "100"),
            ("T3", "2026-10-30", "D", "05", "SGD", "100"),
            ("T4", "20261029", "D", "05", "SGD", "100"),
            ("T5", "20261030", "X", "05", "SGD", "100"),
            ("T6", "20261030", "D", "99", "SGD", "100"),
            ("T7", "20261030", "D", "05", "SG", "100"),
            ("T8", "20261030", "D", "05", "SGD", "12x"),
        ],
        adapter.raw_columns,
    ).write.option("header", True).csv(path)

    valid, rejected = adapter.standardize(adapter.read_raw(spark, path, BUSINESS_DATE))

    assert [(r["txn_id"], r["amount"]) for r in valid.collect()] == [("T1", Decimal("12.34"))]
    reasons = {r["reject_reason"] for r in rejected.collect()}
    assert reasons == {
        "MISSING_TXN_ID",
        "INVALID_BUSINESS_DATE",
        "BUSINESS_DATE_MISMATCH",
        "INVALID_DIRECTION",
        "INVALID_TXN_TYPE",
        "INVALID_CURRENCY",
        "INVALID_AMOUNT",
    }


def test_missing_raw_column_fails_fast(spark, config, tmp_path):
    adapter = SystemAdapter(config.systems[0])
    path = str(tmp_path / "wrong_layout")
    spark.createDataFrame([("T1",)], ["SOMETHING_ELSE"]).write.option("header", True).csv(path)
    with pytest.raises(ValueError, match="missing columns"):
        adapter.read_raw(spark, path, BUSINESS_DATE)

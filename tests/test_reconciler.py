from decimal import Decimal

from conftest import BUSINESS_DATE
from e2e_recon.reconciler import HopReconciler

COLUMNS = ["system", "stage", "txn_id", "business_date", "direction", "txn_type", "currency", "amount"]


def _txns(spark, rows):
    return spark.createDataFrame(
        [(s, h, t, BUSINESS_DATE, d, ty, c, Decimal(a)) for s, h, t, d, ty, c, a in rows],
        "system string, stage int, txn_id string, business_date date, direction string, "
        "txn_type string, currency string, amount decimal(18,2)",
    )


def test_control_and_record_breaks_are_classified(spark, config):
    pair = config.adjacent_pairs[0]
    src, tgt = pair.source.name, pair.target.name
    txns = _txns(
        spark,
        [
            (src, 1, "OK", "OUT", "PURCHASE", "SGD", "10.00"),
            (tgt, 2, "OK", "OUT", "PURCHASE", "SGD", "10.00"),
            (src, 1, "LOST", "OUT", "PURCHASE", "SGD", "5.00"),
            (src, 1, "DUP", "IN", "REFUND", "SGD", "7.00"),
            (tgt, 2, "DUP", "IN", "REFUND", "SGD", "7.00"),
            (tgt, 2, "DUP", "IN", "REFUND", "SGD", "7.00"),
            (src, 1, "FX", "OUT", "FEE", "USD", "3.00"),
            (tgt, 2, "FX", "OUT", "FEE", "USD", "3.01"),
            (tgt, 2, "NEW", "IN", "REPAYMENT", "TWD", "100.00"),
        ],
    )
    reconciler = HopReconciler(config.rules)

    totals = {
        (r["direction"], r["txn_type"]): (r["status"], r["count_diff"], r["amount_diff"])
        for r in reconciler.compare_totals(reconciler.control_totals(txns), pair).collect()
    }
    assert totals == {
        ("OUT", "PURCHASE"): ("COUNT_AND_AMOUNT_BREAK", -1, Decimal("-5.00")),
        ("IN", "REFUND"): ("COUNT_AND_AMOUNT_BREAK", 1, Decimal("7.00")),
        ("OUT", "FEE"): ("AMOUNT_BREAK", 0, Decimal("0.01")),
        ("IN", "REPAYMENT"): ("MISSING_IN_SOURCE", 1, Decimal("100.00")),
    }

    breaks = {r["txn_id"]: r["break_type"] for r in reconciler.compare_records(txns, pair).collect()}
    assert breaks == {
        "LOST": "MISSING_IN_TARGET",
        "DUP": "DUPLICATE_IN_TARGET",
        "FX": "AMOUNT_MISMATCH",
        "NEW": "UNEXPECTED_IN_TARGET",
    }


def test_matched_flow_has_no_breaks(spark, config):
    pair = config.adjacent_pairs[0]
    txns = _txns(
        spark,
        [
            (pair.source.name, 1, "A", "OUT", "PURCHASE", "SGD", "10.00"),
            (pair.target.name, 2, "A", "OUT", "PURCHASE", "SGD", "10.00"),
        ],
    )
    reconciler = HopReconciler(config.rules)
    statuses = {r["status"] for r in reconciler.compare_totals(reconciler.control_totals(txns), pair).collect()}
    assert statuses == {"MATCHED"}
    assert reconciler.compare_records(txns, pair).count() == 0

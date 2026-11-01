"""Synthetic credit-card flow through every configured system, with known breaks injected.

Breaks are injected on the way into each hop and carried downstream, so each one
shows up at exactly one ADJACENT pair (plus END_TO_END). The injected breaks are
returned as an answer key so the recon can be checked against ground truth.
"""

import random
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from e2e_recon.adapters import SystemAdapter
from e2e_recon.config import ReconConfig

STANDARD_SCHEMA = T.StructType(
    [
        T.StructField("txn_id", T.StringType(), False),
        T.StructField("business_date", T.DateType(), False),
        T.StructField("direction", T.StringType(), False),
        T.StructField("txn_type", T.StringType(), False),
        T.StructField("currency", T.StringType(), False),
        T.StructField("amount", T.DecimalType(18, 2), False),
    ]
)
ANSWER_KEY_SCHEMA = T.StructType(
    [
        T.StructField("business_date", T.DateType(), False),
        T.StructField("hop_label", T.StringType(), False),
        T.StructField("txn_id", T.StringType(), False),
        T.StructField("break_type", T.StringType(), False),
    ]
)

# txn_type -> (direction, min amount, max amount)
_TXN_TYPES = {
    "PURCHASE": ("OUT", 5, 1_500),
    "CASH_ADVANCE": ("OUT", 50, 2_000),
    "FEE": ("OUT", 1, 80),
    "REFUND": ("IN", 5, 800),
    "REPAYMENT": ("IN", 100, 10_000),
}
_TXN_WEIGHTS = (70, 3, 7, 5, 15)
_CURRENCIES, _CCY_WEIGHTS = ("SGD", "USD", "TWD"), (70, 20, 10)
_AMOUNT_NUDGES = tuple(Decimal(x) for x in ("0.01", "-0.01", "0.05", "1.00", "-2.50"))


@dataclass(frozen=True)
class BreakProfile:
    """Probability, per transaction per hop, of each kind of break."""

    missing: float = 0.002
    amount: float = 0.001
    duplicate: float = 0.001


@dataclass(frozen=True)
class InjectedBreak:
    hop_label: str
    txn_id: str
    break_type: str


@dataclass(frozen=True)
class SyntheticFlow:
    business_date: date
    records: dict[str, list[tuple]]  # system name -> standard rows
    injected: tuple[InjectedBreak, ...]

    def frame(self, spark: SparkSession, system: str) -> DataFrame:
        return spark.createDataFrame(self.records[system], STANDARD_SCHEMA)

    def answer_key(self, spark: SparkSession) -> DataFrame:
        rows = [(self.business_date, b.hop_label, b.txn_id, b.break_type) for b in self.injected]
        return spark.createDataFrame(rows, ANSWER_KEY_SCHEMA)


class SyntheticFlowGenerator:
    def __init__(
        self, config: ReconConfig, n_txns: int = 5_000, profile: BreakProfile = BreakProfile(), seed: int = 42
    ) -> None:
        if n_txns <= 0:
            raise ValueError("n_txns must be positive")
        self.config = config
        self.n_txns = n_txns
        self.profile = profile
        self.seed = seed

    def generate(self, business_date: date) -> SyntheticFlow:
        rng = random.Random(f"{self.seed}-{business_date.isoformat()}")
        # txn_id -> (row, copies). Copies let a duplicate travel downstream as one unit.
        flow = {row[0]: (row, 1) for row in (self._new_txn(rng, business_date, i) for i in range(self.n_txns))}
        records = {self.config.systems[0].name: self._expand(flow)}
        injected: list[InjectedBreak] = []

        for pair in self.config.adjacent_pairs:
            next_flow = {}
            for txn_id, (row, copies) in flow.items():
                draw = rng.random()
                if draw < self.profile.missing:
                    injected.append(InjectedBreak(pair.label, txn_id, "MISSING_IN_TARGET"))
                    continue
                draw -= self.profile.missing
                if draw < self.profile.amount:
                    row = self._nudge_amount(rng, row)
                    injected.append(InjectedBreak(pair.label, txn_id, "AMOUNT_MISMATCH"))
                elif draw - self.profile.amount < self.profile.duplicate:
                    copies += 1
                    injected.append(InjectedBreak(pair.label, txn_id, "DUPLICATE_IN_TARGET"))
                next_flow[txn_id] = (row, copies)
            flow = next_flow
            records[pair.target.name] = self._expand(flow)

        return SyntheticFlow(business_date, records, tuple(injected))

    def land(self, spark: SparkSession, flow: SyntheticFlow, landing_root: str) -> dict[str, str]:
        """Write each system's extract in its own raw layout. Returns system -> path."""
        paths = {}
        for spec in self.config.systems:
            adapter = SystemAdapter(spec)
            path = adapter.landing_path(landing_root, flow.business_date)
            adapter.write_raw(adapter.encode(flow.frame(spark, spec.name)), path)
            paths[spec.name] = path
        return paths

    @staticmethod
    def _new_txn(rng: random.Random, business_date: date, seq: int) -> tuple:
        txn_type = rng.choices(tuple(_TXN_TYPES), weights=_TXN_WEIGHTS)[0]
        direction, low, high = _TXN_TYPES[txn_type]
        currency = rng.choices(_CURRENCIES, weights=_CCY_WEIGHTS)[0]
        amount = Decimal(rng.randint(low * 100, high * 100)) / 100
        return (f"CC{business_date:%Y%m%d}{seq:07d}", business_date, direction, txn_type, currency, amount)

    @staticmethod
    def _nudge_amount(rng: random.Random, row: tuple) -> tuple:
        nudged = row[5] + rng.choice(_AMOUNT_NUDGES)
        return (*row[:5], nudged if nudged > 0 else row[5] + Decimal("0.01"))

    @staticmethod
    def _expand(flow: dict[str, tuple[tuple, int]]) -> list[tuple]:
        return [row for row, copies in flow.values() for _ in range(copies)]

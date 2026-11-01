"""Control-total and record-level comparison between two systems in the flow."""

import operator
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from e2e_recon.config import HopPair, ReconRules

TOTAL_AMOUNT_TYPE = "decimal(20,2)"
_ATTRIBUTES = ("direction", "txn_type", "currency")


class HopReconciler:
    def __init__(self, rules: ReconRules) -> None:
        self.rules = rules
        self._tolerance = F.lit(rules.amount_tolerance).cast(TOTAL_AMOUNT_TYPE)

    # ------------------------------------------------------------ control level
    def control_totals(self, txns: DataFrame) -> DataFrame:
        """Transaction count and amount per system and control key."""
        return txns.groupBy("system", "hop", *self.rules.control_keys).agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum("amount").cast(TOTAL_AMOUNT_TYPE).alias("txn_amount"),
        )

    def compare_totals(self, totals: DataFrame, pair: HopPair) -> DataFrame:
        keys = list(self.rules.control_keys)
        side = lambda system, prefix: totals.filter(F.col("system") == system).select(  # noqa: E731
            *keys, F.col("txn_count").alias(f"{prefix}_count"), F.col("txn_amount").alias(f"{prefix}_amount")
        )
        joined = side(pair.source.name, "source").join(side(pair.target.name, "target"), on=keys, how="full")

        src_present, tgt_present = F.col("source_count").isNotNull(), F.col("target_count").isNotNull()
        filled = joined.select(
            *keys,
            src_present.alias("_src"),
            tgt_present.alias("_tgt"),
            *[F.coalesce(F.col(c), F.lit(0)).alias(c) for c in ("source_count", "target_count")],
            *[
                F.coalesce(F.col(c), F.lit(0)).cast(TOTAL_AMOUNT_TYPE).alias(c)
                for c in ("source_amount", "target_amount")
            ],
        )
        count_break = F.col("source_count") != F.col("target_count")
        amount_break = F.abs(F.col("target_amount") - F.col("source_amount")) > self._tolerance
        status = (
            F.when(~F.col("_src"), "MISSING_IN_SOURCE")
            .when(~F.col("_tgt"), "MISSING_IN_TARGET")
            .when(count_break & amount_break, "COUNT_AND_AMOUNT_BREAK")
            .when(count_break, "COUNT_BREAK")
            .when(amount_break, "AMOUNT_BREAK")
            .otherwise("MATCHED")
        )
        return filled.select(
            *self._pair_columns(pair),
            *keys,
            "source_count",
            "target_count",
            (F.col("target_count") - F.col("source_count")).alias("count_diff"),
            "source_amount",
            "target_amount",
            (F.col("target_amount") - F.col("source_amount")).cast(TOTAL_AMOUNT_TYPE).alias("amount_diff"),
            status.alias("status"),
        )

    # ------------------------------------------------------------- record level
    def compare_records(self, txns: DataFrame, pair: HopPair) -> DataFrame:
        """Explain control breaks: which transactions are missing, duplicated or altered between two systems."""
        key = self.rules.record_key
        source = self._per_record(txns, pair.source.name, "source")
        target = self._per_record(txns, pair.target.name, "target")
        joined = source.join(target, on=[key, "business_date"], how="full")

        attrs_differ = reduce(
            operator.or_, [~F.col(f"source_{a}").eqNullSafe(F.col(f"target_{a}")) for a in _ATTRIBUTES]
        )
        amount_diff = F.col("target_amount") - F.col("source_amount")
        break_type = (
            F.when(F.col("source_count").isNull(), "UNEXPECTED_IN_TARGET")
            .when(F.col("target_count").isNull(), "MISSING_IN_TARGET")
            .when(F.col("target_count") > F.col("source_count"), "DUPLICATE_IN_TARGET")
            .when(F.col("target_count") < F.col("source_count"), "DUPLICATE_IN_SOURCE")
            .when(attrs_differ, "ATTRIBUTE_MISMATCH")
            .when(F.abs(amount_diff) > self._tolerance, "AMOUNT_MISMATCH")
        )
        return (
            joined.withColumn("break_type", break_type)
            .filter(F.col("break_type").isNotNull())
            .select(
                *self._pair_columns(pair),
                "business_date",
                key,
                "break_type",
                *[F.coalesce(F.col(f"source_{a}"), F.col(f"target_{a}")).alias(a) for a in _ATTRIBUTES],
                F.coalesce(F.col("source_count"), F.lit(0)).alias("source_count"),
                F.coalesce(F.col("target_count"), F.lit(0)).alias("target_count"),
                "source_amount",
                "target_amount",
                amount_diff.cast(TOTAL_AMOUNT_TYPE).alias("amount_diff"),
            )
        )

    def _per_record(self, txns: DataFrame, system: str, prefix: str) -> DataFrame:
        return (
            txns.filter(F.col("system") == system)
            .groupBy(self.rules.record_key, "business_date")
            .agg(
                F.count(F.lit(1)).alias(f"{prefix}_count"),
                F.sum("amount").cast(TOTAL_AMOUNT_TYPE).alias(f"{prefix}_amount"),
                *[F.min(a).alias(f"{prefix}_{a}") for a in _ATTRIBUTES],
            )
        )

    @staticmethod
    def _pair_columns(pair: HopPair) -> list[Column]:
        return [
            F.lit(pair.label).alias("hop_label"),
            F.lit(pair.scope).alias("pair_scope"),
            F.lit(pair.source.name).alias("source_system"),
            F.lit(pair.target.name).alias("target_system"),
        ]

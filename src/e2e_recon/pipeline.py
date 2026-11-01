"""Orchestrates bronze -> silver -> gold for one business date."""

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from e2e_recon.adapters import BRONZE_DATE_COL, SystemAdapter
from e2e_recon.config import ReconConfig, TableNames
from e2e_recon.reconciler import HopReconciler
from e2e_recon.storage import TableStore

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestSummary:
    business_date: date
    loaded: dict[str, int]
    rejected: dict[str, int]


@dataclass(frozen=True)
class ReconSummary:
    business_date: date
    run_id: str
    pair_status: dict[str, dict[str, int]]  # hop_label -> status -> number of control keys
    record_breaks: dict[str, dict[str, int]]  # hop_label -> break_type -> number of transactions

    @property
    def has_breaks(self) -> bool:
        return any(s != "MATCHED" for counts in self.pair_status.values() for s in counts)


class ReconPipeline:
    def __init__(
        self,
        spark: SparkSession,
        config: ReconConfig,
        tables: TableNames,
        store: TableStore,
        landing_root: str | None = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.tables = tables
        self.store = store
        self.landing_root = landing_root or tables.landing_root
        self.adapters = tuple(SystemAdapter(s) for s in config.systems)
        self.reconciler = HopReconciler(config.rules)

    def ingest(self, business_date: date) -> IngestSummary:
        valid_frames, reject_frames = [], []
        for adapter in self.adapters:
            table = self.tables.bronze(adapter.spec.name)
            path = adapter.landing_path(self.landing_root, business_date)
            log.info("Loading %s from %s", adapter.spec.name, path)
            self.store.overwrite_date(adapter.read_raw(self.spark, path, business_date), table, BRONZE_DATE_COL, business_date)

            bronze = self.store.read(table).filter(F.col(BRONZE_DATE_COL) == F.lit(business_date))
            valid, rejected = adapter.standardize(bronze)
            valid_frames.append(valid)
            reject_frames.append(rejected)

        self.store.overwrite_date(_union(valid_frames), self.tables.silver_txn, "business_date", business_date)
        self.store.overwrite_date(_union(reject_frames), self.tables.silver_rejects, "business_date", business_date)
        return IngestSummary(
            business_date,
            loaded=self._count_by_system(self.tables.silver_txn, business_date),
            rejected=self._count_by_system(self.tables.silver_rejects, business_date),
        )

    def reconcile(self, business_date: date, run_id: str) -> ReconSummary:
        txns = self.store.read(self.tables.silver_txn).filter(F.col("business_date") == F.lit(business_date))
        run_cols = [F.lit(run_id).alias("run_id"), F.lit(datetime.now(timezone.utc)).alias("recon_ts")]

        totals = self.reconciler.control_totals(txns).select("*", *run_cols)
        self.store.overwrite_date(totals, self.tables.gold_control_totals, "business_date", business_date)
        totals = self.store.read(self.tables.gold_control_totals).filter(F.col("business_date") == F.lit(business_date))

        pairs = self.config.hop_pairs
        results = _union([self.reconciler.compare_totals(totals, p) for p in pairs]).select("*", *run_cols)
        details = _union([self.reconciler.compare_records(txns, p) for p in pairs]).select("*", *run_cols)
        self.store.overwrite_date(results, self.tables.gold_recon_results, "business_date", business_date)
        self.store.overwrite_date(details, self.tables.gold_break_details, "business_date", business_date)

        return ReconSummary(
            business_date,
            run_id,
            pair_status=self._tally(self.tables.gold_recon_results, business_date, "status"),
            record_breaks=self._tally(self.tables.gold_break_details, business_date, "break_type"),
        )

    def _count_by_system(self, table: str, business_date: date) -> dict[str, int]:
        rows = (
            self.store.read(table)
            .filter(F.col("business_date") == F.lit(business_date))
            .groupBy("system")
            .count()
            .collect()
        )
        counts = {r["system"]: r["count"] for r in rows}
        return {a.spec.name: counts.get(a.spec.name, 0) for a in self.adapters}

    def _tally(self, table: str, business_date: date, column: str) -> dict[str, dict[str, int]]:
        rows = (
            self.store.read(table)
            .filter(F.col("business_date") == F.lit(business_date))
            .groupBy("hop_label", column)
            .count()
            .collect()
        )
        tally: dict[str, dict[str, int]] = {p.label: {} for p in self.config.hop_pairs}
        for r in rows:
            tally[r["hop_label"]][r[column]] = r["count"]
        return tally


def _union(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b), frames)

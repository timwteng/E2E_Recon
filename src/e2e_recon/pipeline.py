"""Orchestrates bronze -> silver -> gold for one business date."""

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from e2e_recon.adapters import BRONZE_DATE_COL, MissingExtractError, SystemAdapter
from e2e_recon.config import ReconConfig, TableNames
from e2e_recon.reconciler import HopReconciler
from e2e_recon.storage import TableStore

log = logging.getLogger(__name__)

LOADED, MISSING = "LOADED", "MISSING"
EXTRACT_MISSING = "EXTRACT_MISSING"

EXTRACT_STATUS_SCHEMA = T.StructType(
    [
        T.StructField("business_date", T.DateType(), False),
        T.StructField("system", T.StringType(), False),
        T.StructField("stage", T.IntegerType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("file_count", T.LongType(), False),
        T.StructField("rows_loaded", T.LongType(), False),
        T.StructField("rows_rejected", T.LongType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("checked_at", T.TimestampType(), False),
    ]
)
RUN_STATUS_SCHEMA = T.StructType(
    [
        T.StructField("business_date", T.DateType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("completed_at", T.TimestampType(), False),
        T.StructField("missing_extracts", T.ArrayType(T.StringType()), False),
        T.StructField("control_break_keys", T.LongType(), False),
        T.StructField("record_breaks", T.LongType(), False),
    ]
)


@dataclass(frozen=True)
class IngestSummary:
    business_date: date
    loaded: dict[str, int]
    rejected: dict[str, int]
    missing: tuple[str, ...]


@dataclass(frozen=True)
class ReconSummary:
    business_date: date
    run_id: str
    pair_status: dict[str, dict[str, int]]  # hop_label -> status -> number of control keys
    record_breaks: dict[str, dict[str, int]]  # hop_label -> break_type -> number of transactions
    missing_extracts: tuple[str, ...]

    @property
    def control_break_keys(self) -> int:
        return sum(n for counts in self.pair_status.values() for s, n in counts.items() if s != "MATCHED")

    @property
    def record_break_count(self) -> int:
        return sum(n for counts in self.record_breaks.values() for n in counts.values())

    @property
    def has_breaks(self) -> bool:
        return bool(self.missing_extracts) or self.control_break_keys > 0


@dataclass(frozen=True)
class RunRecord:
    """The latest completed recon for a business date, as written to gold_run_status."""

    business_date: date
    run_id: str
    completed_at: datetime
    missing_extracts: tuple[str, ...]
    control_break_keys: int
    record_breaks: int

    @property
    def is_clean(self) -> bool:
        return not self.missing_extracts and self.control_break_keys == 0


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

    # ------------------------------------------------------------------ ingest
    def ingest(self, business_date: date, run_id: str) -> IngestSummary:
        """Load every system's extract. A missing extract is recorded, not fatal; a changed layout is fatal."""
        valid_frames, reject_frames, arrived = [], [], {}
        for adapter in self.adapters:
            name, table = adapter.spec.name, self.tables.bronze(adapter.spec.name)
            path = adapter.landing_path(self.landing_root, business_date)
            try:
                raw = adapter.read_raw(self.spark, path, business_date)
                arrived[name] = True
            except MissingExtractError as exc:
                log.warning("%s", exc)
                raw = adapter.empty_bronze(self.spark)  # clears any stale rows from an earlier run
                arrived[name] = False
            self.store.overwrite_date(raw, table, BRONZE_DATE_COL, business_date)

            bronze = self.store.read(table).filter(F.col(BRONZE_DATE_COL) == F.lit(business_date))
            valid, rejected = adapter.standardize(bronze)
            valid_frames.append(valid)
            reject_frames.append(rejected)

        self.store.overwrite_date(_union(valid_frames), self.tables.silver_txn, "business_date", business_date)
        self.store.overwrite_date(_union(reject_frames), self.tables.silver_rejects, "business_date", business_date)

        loaded = self._count_by_system(self.tables.silver_txn, business_date)
        rejected = self._count_by_system(self.tables.silver_rejects, business_date)
        self._write_extract_status(business_date, run_id, arrived, loaded, rejected)
        return IngestSummary(
            business_date,
            loaded=loaded,
            rejected=rejected,
            missing=tuple(name for name, ok in arrived.items() if not ok),
        )

    # --------------------------------------------------------------- reconcile
    def reconcile(self, business_date: date, run_id: str) -> ReconSummary:
        txns = self.store.read(self.tables.silver_txn).filter(F.col("business_date") == F.lit(business_date))
        missing = self._missing_extracts(business_date)
        run_cols = [F.lit(run_id).alias("run_id"), F.lit(_utc_now()).alias("recon_ts")]

        totals = self.reconciler.control_totals(txns).select("*", *run_cols)
        self.store.overwrite_date(totals, self.tables.gold_control_totals, "business_date", business_date)
        totals = self.store.read(self.tables.gold_control_totals).filter(F.col("business_date") == F.lit(business_date))

        results, details = [], []
        for pair in self.config.hop_pairs:
            compared = self.reconciler.compare_totals(totals, pair)
            records = self.reconciler.compare_records(txns, pair)
            if {pair.source.name, pair.target.name} & set(missing):
                # Not a data break: one side never arrived, so transaction-level detail would only be noise.
                compared, records = compared.withColumn("status", F.lit(EXTRACT_MISSING)), records.limit(0)
            results.append(compared)
            details.append(records)

        self.store.overwrite_date(
            _union(results).select("*", *run_cols), self.tables.gold_recon_results, "business_date", business_date
        )
        self.store.overwrite_date(
            _union(details).select("*", *run_cols), self.tables.gold_break_details, "business_date", business_date
        )

        summary = ReconSummary(
            business_date,
            run_id,
            pair_status=self._tally(self.tables.gold_recon_results, business_date, "status"),
            record_breaks=self._tally(self.tables.gold_break_details, business_date, "break_type"),
            missing_extracts=missing,
        )
        self._write_run_status(summary)  # last: marks the gold tables for this run as complete
        return summary

    # ------------------------------------------------------------------- check
    def latest_completed_run(self, business_date: date) -> RunRecord | None:
        if not self.store.exists(self.tables.gold_run_status):  # no recon has ever completed
            return None
        rows = (
            self.store.read(self.tables.gold_run_status)
            .filter(F.col("business_date") == F.lit(business_date))
            .orderBy(F.col("completed_at").desc())
            .limit(1)
            .collect()
        )
        if not rows:
            return None
        r = rows[0]
        return RunRecord(
            r["business_date"],
            r["run_id"],
            r["completed_at"],
            tuple(r["missing_extracts"]),
            r["control_break_keys"],
            r["record_breaks"],
        )

    # ----------------------------------------------------------------- helpers
    def _write_extract_status(
        self, business_date: date, run_id: str, arrived: dict[str, bool], loaded: dict[str, int], rejected: dict[str, int]
    ) -> None:
        now = _utc_now()
        rows = []
        for adapter in self.adapters:
            name = adapter.spec.name
            files = (
                self.store.read(self.tables.bronze(name))
                .filter(F.col(BRONZE_DATE_COL) == F.lit(business_date))
                .select(F.countDistinct("_source_file"))
                .first()[0]
            )
            rows.append(
                (
                    business_date,
                    name,
                    adapter.spec.stage,
                    LOADED if arrived[name] else MISSING,
                    files,
                    loaded[name],
                    rejected[name],
                    run_id,
                    now,
                )
            )
        df = self.spark.createDataFrame(rows, EXTRACT_STATUS_SCHEMA)
        self.store.overwrite_date(df, self.tables.silver_extract_status, "business_date", business_date)

    def _missing_extracts(self, business_date: date) -> tuple[str, ...]:
        rows = (
            self.store.read(self.tables.silver_extract_status)
            .filter((F.col("business_date") == F.lit(business_date)) & (F.col("status") == MISSING))
            .select("system")
            .collect()
        )
        found = {r["system"] for r in rows}
        return tuple(a.spec.name for a in self.adapters if a.spec.name in found)

    def _write_run_status(self, summary: ReconSummary) -> None:
        row = (
            summary.business_date,
            summary.run_id,
            _utc_now(),
            list(summary.missing_extracts),
            summary.control_break_keys,
            summary.record_break_count,
        )
        self.store.append(self.spark.createDataFrame([row], RUN_STATUS_SCHEMA), self.tables.gold_run_status)

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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

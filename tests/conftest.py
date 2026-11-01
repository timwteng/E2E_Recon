import os
import sys
from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from e2e_recon.config import ReconConfig
from e2e_recon.storage import TableStore

BUSINESS_DATE = date(2026, 10, 30)

# Spark workers must run the same Python minor version as the driver (the 3.11 venv).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


class InMemoryTableStore(TableStore):
    """Same overwrite-one-date semantics as DeltaTableStore, without needing Delta locally."""

    def __init__(self) -> None:
        self.tables: dict[str, DataFrame] = {}

    def exists(self, table: str) -> bool:
        return table in self.tables

    def read(self, table: str) -> DataFrame:
        return self.tables[table]

    def overwrite_date(self, df: DataFrame, table: str, date_column: str, business_date: date) -> None:
        existing = self.tables.get(table)
        kept = existing.filter(F.col(date_column) != F.lit(business_date)) if existing is not None else None
        self._save(table, df if kept is None else kept.unionByName(df))

    def append(self, df: DataFrame, table: str) -> None:
        existing = self.tables.get(table)
        self._save(table, df if existing is None else existing.unionByName(df))

    def _save(self, table: str, df: DataFrame) -> None:
        self.tables[table] = df.localCheckpoint()  # materialise, like a real table write


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("e2e-recon-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")  # match Databricks serverless
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def config() -> ReconConfig:
    return ReconConfig.load()


@pytest.fixture
def store() -> InMemoryTableStore:
    return InMemoryTableStore()

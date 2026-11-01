"""Table persistence. Every write replaces exactly one business date, so reruns are idempotent."""

from abc import ABC, abstractmethod
from datetime import date

from pyspark.sql import DataFrame, SparkSession


class TableStore(ABC):
    @abstractmethod
    def read(self, table: str) -> DataFrame: ...

    @abstractmethod
    def overwrite_date(self, df: DataFrame, table: str, date_column: str, business_date: date) -> None: ...


class DeltaTableStore(TableStore):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, table: str) -> DataFrame:
        return self.spark.read.table(table)

    def overwrite_date(self, df: DataFrame, table: str, date_column: str, business_date: date) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"{date_column} = DATE'{business_date.isoformat()}'")
            .option("mergeSchema", True)
            .saveAsTable(table)
        )

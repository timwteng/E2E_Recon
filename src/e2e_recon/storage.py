"""Table persistence.

Layer tables replace exactly one business date per write, so reruns are idempotent.
The run log is append-only, so every attempt stays on record.
"""

from abc import ABC, abstractmethod
from datetime import date

from pyspark.sql import DataFrame, SparkSession


class TableStore(ABC):
    @abstractmethod
    def exists(self, table: str) -> bool: ...

    @abstractmethod
    def read(self, table: str) -> DataFrame: ...

    @abstractmethod
    def overwrite_date(self, df: DataFrame, table: str, date_column: str, business_date: date) -> None: ...

    @abstractmethod
    def append(self, df: DataFrame, table: str) -> None: ...


class DeltaTableStore(TableStore):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(table)

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

    def append(self, df: DataFrame, table: str) -> None:
        df.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable(table)

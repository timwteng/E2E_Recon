"""Translate each system's raw extract to and from the standard transaction schema.

All parsing uses try_* SQL functions so a bad row becomes a reject with a reason
instead of failing the job (Databricks serverless runs with ANSI mode on).
"""

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from e2e_recon.config import DIRECTIONS, STANDARD_FIELDS, SystemSpec

AMOUNT_TYPE = "decimal(18,2)"
BRONZE_DATE_COL = "_business_date"
STANDARD_COLUMNS = ("system", "hop", *STANDARD_FIELDS, "source_file", "ingested_at")


class SystemAdapter:
    def __init__(self, spec: SystemSpec) -> None:
        self.spec = spec

    @property
    def raw_columns(self) -> list[str]:
        return [self.spec.raw_column(f) for f in STANDARD_FIELDS]

    def landing_path(self, landing_root: str, business_date: date) -> str:
        return f"{landing_root.rstrip('/')}/{self.spec.name}/business_date={business_date.isoformat()}"

    # ---------------------------------------------------------------- read side
    def read_raw(self, spark: SparkSession, path: str, business_date: date) -> DataFrame:
        """Load one day's extract as strings, plus lineage columns. This is the bronze record."""
        reader = spark.read.format(self.spec.file_format)
        if self.spec.file_format == "csv":
            reader = reader.option("header", True).option("inferSchema", False)
        else:
            reader = reader.option("primitivesAsString", True)
        df = reader.load(path)

        missing = [c for c in self.raw_columns if c not in df.columns]
        if missing:
            raise ValueError(f"{self.spec.name}: extract at {path} is missing columns {missing}")

        return df.select(
            *[F.col(c).cast("string").alias(c) for c in self.raw_columns],
            F.lit(business_date).alias(BRONZE_DATE_COL),
            F.col("_metadata.file_name").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )

    def standardize(self, bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Split bronze rows into (valid standard rows, rejected rows with a reason)."""
        parsed = bronze.select(
            F.lit(self.spec.name).alias("system"),
            F.lit(self.spec.hop).alias("hop"),
            F.trim(self._raw("txn_id")).alias("txn_id"),
            F.to_date(F.expr(f"try_to_timestamp(trim({self._quoted('business_date')}), '{self.spec.date_format}')"))
            .alias("business_date"),
            self._decode("direction").alias("direction"),
            self._decode("txn_type").alias("txn_type"),
            F.upper(F.trim(self._raw("currency"))).alias("currency"),
            F.expr(
                f"try_divide(try_cast(trim({self._quoted('amount')}) AS DECIMAL(24,6)), {self.spec.amount_scale})"
            ).cast(AMOUNT_TYPE).alias("amount"),
            F.col("_source_file").alias("source_file"),
            F.col("_ingested_at").alias("ingested_at"),
            F.col(BRONZE_DATE_COL).alias("_landing_date"),
            F.to_json(F.struct(*[F.col(c) for c in self.raw_columns])).alias("raw_record"),
        ).withColumn("reject_reason", self._reject_reason())

        valid = parsed.filter(F.col("reject_reason").isNull()).select(*STANDARD_COLUMNS)
        rejected = parsed.filter(F.col("reject_reason").isNotNull()).select(
            "system",
            "hop",
            F.col("_landing_date").alias("business_date"),
            "reject_reason",
            "raw_record",
            "source_file",
            "ingested_at",
        )
        return valid, rejected

    # --------------------------------------------------------------- write side
    def encode(self, standard: DataFrame) -> DataFrame:
        """Inverse of standardize: render standard rows in this system's raw layout (used for synthetic data)."""
        rendered = {
            "txn_id": F.col("txn_id"),
            "business_date": F.date_format("business_date", self.spec.date_format),
            "direction": self._encode_value("direction"),
            "txn_type": self._encode_value("txn_type"),
            "currency": F.col("currency"),
            "amount": (
                (F.col("amount") * F.lit(self.spec.amount_scale)).cast("long")
                if self.spec.amount_scale > 1
                else F.col("amount")
            ),
        }
        return standard.select(*[rendered[f].cast("string").alias(self.spec.raw_column(f)) for f in STANDARD_FIELDS])

    def write_raw(self, raw: DataFrame, path: str) -> None:
        raw.coalesce(1).write.mode("overwrite").option("header", True).format(self.spec.file_format).save(path)

    # ------------------------------------------------------------------ helpers
    def _raw(self, standard_field: str) -> Column:
        return F.col(self.spec.raw_column(standard_field))

    def _quoted(self, standard_field: str) -> str:
        return f"`{self.spec.raw_column(standard_field)}`"

    def _decode(self, standard_field: str) -> Column:
        value = F.upper(F.trim(self._raw(standard_field)))
        mapping = self.spec.value_maps.get(standard_field)
        if not mapping:
            return value
        expr = None
        for raw_value, std_value in mapping.items():
            cond = value == F.lit(raw_value.upper())
            expr = F.when(cond, F.lit(std_value)) if expr is None else expr.when(cond, F.lit(std_value))
        return expr  # unmapped codes become null and are rejected

    def _encode_value(self, standard_field: str) -> Column:
        mapping = self.spec.value_maps.get(standard_field)
        if not mapping:
            return F.col(standard_field)
        expr = None
        for raw_value, std_value in mapping.items():
            cond = F.col(standard_field) == F.lit(std_value)
            expr = F.when(cond, F.lit(raw_value)) if expr is None else expr.when(cond, F.lit(raw_value))
        return expr

    @staticmethod
    def _reject_reason() -> Column:
        return (
            F.when(F.col("txn_id").isNull() | (F.col("txn_id") == ""), "MISSING_TXN_ID")
            .when(F.col("business_date").isNull(), "INVALID_BUSINESS_DATE")
            .when(F.col("business_date") != F.col("_landing_date"), "BUSINESS_DATE_MISMATCH")
            .when(~F.coalesce(F.col("direction").isin(*DIRECTIONS), F.lit(False)), "INVALID_DIRECTION")
            .when(F.col("txn_type").isNull() | (F.col("txn_type") == ""), "INVALID_TXN_TYPE")
            .when(F.coalesce(F.length("currency"), F.lit(0)) != 3, "INVALID_CURRENCY")
            .when(F.col("amount").isNull(), "INVALID_AMOUNT")
        )

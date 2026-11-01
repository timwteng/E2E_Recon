# Databricks notebook source
# MAGIC %md
# MAGIC # E2E recon: review breaks
# MAGIC Control totals by hop, then the transactions behind each break. Set the widgets, then Run all.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "e2e_recon")
dbutils.widgets.text("business_date", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
business_date = dbutils.widgets.get("business_date") or spark.sql(
    f"SELECT max(business_date) FROM {catalog}.{schema}.gold_recon_results"
).first()[0]
print(f"Reviewing {catalog}.{schema} for {business_date}")

# COMMAND ----------

# MAGIC %md ## 1. Status by hop

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT hop_label, pair_scope,
               count_if(status = 'MATCHED')  AS matched_keys,
               count_if(status != 'MATCHED') AS break_keys,
               sum(count_diff)               AS net_count_diff,
               sum(amount_diff)              AS net_amount_diff
        FROM {catalog}.{schema}.gold_recon_results
        WHERE business_date = '{business_date}'
        GROUP BY ALL
        ORDER BY pair_scope, hop_label
        """
    )
)

# COMMAND ----------

# MAGIC %md ## 2. Control keys that broke

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT hop_label, direction, currency, txn_type, status,
               source_count, target_count, count_diff,
               source_amount, target_amount, amount_diff
        FROM {catalog}.{schema}.gold_recon_results
        WHERE business_date = '{business_date}' AND status != 'MATCHED' AND pair_scope = 'ADJACENT'
        ORDER BY hop_label, abs(amount_diff) DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md ## 3. Transactions behind the breaks

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT hop_label, break_type, txn_id, direction, currency, txn_type,
               source_count, target_count, source_amount, target_amount, amount_diff
        FROM {catalog}.{schema}.gold_break_details
        WHERE business_date = '{business_date}' AND pair_scope = 'ADJACENT'
        ORDER BY hop_label, break_type, txn_id
        """
    )
)

# COMMAND ----------

# MAGIC %md ## 4. Synthetic data only: did the recon find every injected break?

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT coalesce(k.hop_label, d.hop_label) AS hop_label,
               count_if(k.txn_id IS NOT NULL AND d.txn_id IS NOT NULL) AS found,
               count_if(d.txn_id IS NULL)                              AS missed,
               count_if(k.txn_id IS NULL)                              AS false_positive
        FROM (SELECT * FROM {catalog}.{schema}.synthetic_answer_key WHERE business_date = '{business_date}') k
        FULL OUTER JOIN (
            SELECT * FROM {catalog}.{schema}.gold_break_details
            WHERE business_date = '{business_date}' AND pair_scope = 'ADJACENT'
        ) d
          ON k.hop_label = d.hop_label AND k.txn_id = d.txn_id AND k.break_type = d.break_type
        GROUP BY ALL
        ORDER BY hop_label
        """
    )
)

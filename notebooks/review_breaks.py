# Databricks notebook source
# MAGIC %md
# MAGIC # E2E recon: review breaks
# MAGIC Shows the latest **completed** recon for a business date: which extracts arrived, control totals
# MAGIC for each hop (stage to stage), then the transactions behind each break. Set the widgets, then Run all.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "e2e_recon")
dbutils.widgets.text("business_date", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = lambda table: f"{catalog}.{schema}.{table}"  # noqa: E731

business_date = dbutils.widgets.get("business_date") or spark.sql(
    f"SELECT max(business_date) FROM {fq('gold_run_status')}"
).first()[0]

# Only trust gold rows written by a run that finished: gold_run_status is written last.
latest = spark.sql(
    f"""
    SELECT run_id, completed_at, missing_extracts, control_break_keys, record_breaks
    FROM {fq('gold_run_status')}
    WHERE business_date = '{business_date}'
    ORDER BY completed_at DESC
    LIMIT 1
    """
).first()
if latest is None:
    dbutils.notebook.exit(f"No completed recon for {business_date}")
run_id = latest["run_id"]
print(
    f"{business_date}: run {run_id} completed {latest['completed_at']} | "
    f"missing extracts: {latest['missing_extracts'] or 'none'} | "
    f"control keys with breaks: {latest['control_break_keys']} | transactions in breaks: {latest['record_breaks']}"
)

# COMMAND ----------

# MAGIC %md ## 1. Did every extract arrive?

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT stage, system, status, file_count, rows_loaded, rows_rejected
        FROM {fq('silver_extract_status')}
        WHERE business_date = '{business_date}'
        ORDER BY stage
        """
    )
)

# COMMAND ----------

# MAGIC %md ## 2. Status by hop

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT hop_label, pair_scope,
               count_if(status = 'MATCHED')  AS matched_keys,
               count_if(status != 'MATCHED') AS break_keys,
               sum(count_diff)               AS net_count_diff,
               sum(amount_diff)              AS net_amount_diff
        FROM {fq('gold_recon_results')}
        WHERE business_date = '{business_date}' AND run_id = '{run_id}'
        GROUP BY ALL
        ORDER BY pair_scope, hop_label
        """
    )
)

# COMMAND ----------

# MAGIC %md ## 3. Control keys that broke

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT hop_label, direction, currency, txn_type, status,
               source_count, target_count, count_diff,
               source_amount, target_amount, amount_diff
        FROM {fq('gold_recon_results')}
        WHERE business_date = '{business_date}' AND run_id = '{run_id}'
          AND status != 'MATCHED' AND pair_scope = 'ADJACENT'
        ORDER BY hop_label, abs(amount_diff) DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md ## 4. Transactions behind the breaks

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT hop_label, break_type, txn_id, direction, currency, txn_type,
               source_count, target_count, source_amount, target_amount, amount_diff
        FROM {fq('gold_break_details')}
        WHERE business_date = '{business_date}' AND run_id = '{run_id}' AND pair_scope = 'ADJACENT'
        ORDER BY hop_label, break_type, txn_id
        """
    )
)

# COMMAND ----------

# MAGIC %md ## 5. Synthetic data only: did the recon find every injected break?

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT coalesce(k.hop_label, d.hop_label) AS hop_label,
               count_if(k.txn_id IS NOT NULL AND d.txn_id IS NOT NULL) AS found,
               count_if(d.txn_id IS NULL)                              AS missed,
               count_if(k.txn_id IS NULL)                              AS false_positive
        FROM (SELECT * FROM {fq('synthetic_answer_key')} WHERE business_date = '{business_date}') k
        FULL OUTER JOIN (
            SELECT * FROM {fq('gold_break_details')}
            WHERE business_date = '{business_date}' AND run_id = '{run_id}' AND pair_scope = 'ADJACENT'
        ) d
          ON k.hop_label = d.hop_label AND k.txn_id = d.txn_id AND k.break_type = d.break_type
        GROUP BY ALL
        ORDER BY hop_label
        """
    )
)

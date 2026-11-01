# E2E Recon

End-to-end reconciliation of **transaction count and amount** across a multi-system flow, deployed to
Databricks with an Asset Bundle.

The example flow is a credit card payment, where every incoming (refunds, repayments) and outgoing
(purchases, cash advances, fees) transaction passes through four systems:

```
card_switch  ->  card_processing  ->  payment_hub  ->  general_ledger
  (hop 1)           (hop 2)             (hop 3)           (hop 4)
```

For each business date the recon proves that what entered hop 1 arrived at every later hop with the
same count and amount. It runs three kinds of check:

- **Adjacent hops** (1→2, 2→3, 3→4) show *where* a break happened.
- **End to end** (1→4) shows whether the flow as a whole balances.
- **Record-level detail** explains each control break: the transactions that are missing, unexpected,
  duplicated, or have a changed amount or attributes.

## How it works

| Layer | Table | What's in it |
|---|---|---|
| Landing | UC volume `landing/<system>/business_date=YYYY-MM-DD/` | Raw extracts, each in its own layout |
| Bronze | `bronze_<system>` | Raw rows as strings, plus file name and load time |
| Silver | `silver_txn` | All systems mapped to one standard schema |
| Silver | `silver_rejects` | Rows that failed parsing, with a reason |
| Gold | `gold_control_totals` | Count and amount per system × date × direction × currency × txn type |
| Gold | `gold_recon_results` | Source vs target totals per hop pair, with diff and status |
| Gold | `gold_break_details` | Transactions behind each break |

Each system's layout is declared in [`src/e2e_recon/conf/systems.yml`](src/e2e_recon/conf/systems.yml):
column names, date format, minor-unit amounts and code mappings (`D`→`OUT`, `05`→`PURCHASE`, and so
on). To add a system or change a layout, you edit the YAML, not the code.

Every write replaces exactly one business date, so a rerun of the same date is idempotent.

**Control statuses:** `MATCHED`, `COUNT_BREAK`, `AMOUNT_BREAK`, `COUNT_AND_AMOUNT_BREAK`,
`MISSING_IN_TARGET`, `MISSING_IN_SOURCE`.

**Record break types:** `MISSING_IN_TARGET`, `UNEXPECTED_IN_TARGET`, `DUPLICATE_IN_TARGET`,
`DUPLICATE_IN_SOURCE`, `ATTRIBUTE_MISMATCH`, `AMOUNT_MISMATCH`.

### Synthetic data

`recon generate` creates a day of card transactions, pushes them through the four systems, and
injects known breaks along the way (dropped, duplicated and amount-nudged transactions). The injected
breaks are saved to `synthetic_answer_key`, so the last cell of `notebooks/review_breaks.py` can show
whether the recon found every one of them.

## Project layout

```
databricks.yml                 bundle definition: targets free / azure_dev
resources/
  e2e_recon.schema.yml         UC schema + landing volume
  e2e_recon.job.yml            daily job: generate (optional) -> ingest -> reconcile
src/e2e_recon/
  conf/systems.yml             flow definition and per-system layouts
  config.py                    typed config, hop pairs, table names
  adapters.py                  raw <-> standard mapping per system
  reconciler.py                control totals and record-level comparison
  pipeline.py                  bronze -> silver -> gold for one date
  storage.py                   Delta writes, one date at a time
  synthetic.py                 synthetic flow with an answer key
  cli.py                       `recon` entry point used by job tasks
notebooks/review_breaks.py     review results in the workspace
tests/                         pytest, including a full pipeline run vs the answer key
.github/workflows/ci.yml       GitHub Actions: tests + wheel build on Python 3.11
```

## 1. Repository and CI

The code lives at <https://github.com/timwteng/E2E_Recon>. To work on another machine:

```powershell
git clone https://github.com/timwteng/E2E_Recon.git
```

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs on every push and pull
request to `main`. It sets up Python 3.11 and Java 17, runs `pytest`, and builds the wheel. Results
show up in the repo's **Actions** tab.

> This folder is inside OneDrive. Git works there, but if OneDrive ever reports sync conflicts on
> `.git`, move the project to a non-synced path such as `C:\dev\E2E_Recon`.

## 2. Run locally (Python 3.11)

You'll need Python 3.11 and Java 17 for local PySpark.

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest -q
```

## 3. Deploy to Databricks Free Edition

1. Sign up at <https://www.databricks.com/learn/free-edition> and copy your workspace URL.
2. Install the Databricks CLI (v0.250 or newer) and log in:

```powershell
winget install Databricks.DatabricksCLI
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile free
```

3. Validate, deploy and run. The CLI builds the wheel, uploads it, and creates the schema, volume
   and job:

```powershell
databricks bundle validate -t free
databricks bundle deploy -t free
databricks bundle run e2e_recon_daily -t free
```

4. In the workspace, open **Jobs & Pipelines → [dev …] e2e_recon_daily** to see the run. Then open
   `notebooks/review_breaks.py` under the bundle folder in **Workspace**.

Because `mode: development` is set, the deployed schema name gets a prefix like
`dev_<your_user>_e2e_recon`. The job resolves this automatically. For the notebook, check the actual
name in **Catalog → workspace** or with `databricks bundle summary -t free`, and enter it in the
`schema` widget.

To run for a specific date: `databricks bundle run e2e_recon_daily -t free --params business_date=2026-10-30`.

**Optional:** to edit in the Databricks UI as well, go to **Workspace → Create → Git folder** and
paste `https://github.com/timwteng/E2E_Recon.git`. Deploy from your machine with the CLI either way.

## 4. Move to Azure Databricks later

The code doesn't change. Only the target does:

1. `databricks auth login --host https://adb-<id>.<n>.azuredatabricks.net --profile azure`
2. In `databricks.yml` under `azure_dev`, set `catalog` to a Unity Catalog catalog you can create
   schemas in.
3. Serverless jobs must be enabled on the workspace. If they aren't, replace `environment_key` in the
   job with a job cluster (DBR 15.4 LTS, Python 3.11) and add the wheel as a `libraries: - whl:` entry.
4. Run `databricks bundle deploy -t azure_dev`.

For a shared, scheduled setup, add a `prod` target with `mode: production`, a service principal in
`run_as`, and `use_synthetic_data: "false"`.

## 5. Plug in real extracts

1. Set `use_synthetic_data` to `"false"` for the target. The generate task is then skipped.
2. Have each upstream system drop its daily file at
   `/Volumes/<catalog>/<schema>/landing/<system>/business_date=YYYY-MM-DD/`.
3. Update `systems.yml` with the real column names, date formats, amount scales and code mappings.
4. To fail the job, and trigger alerts, when breaks are found, add `--fail-on-break` to the
   reconcile task.

Things a production version would add: timing tolerance for transactions that legitimately post at
the next hop on T+1, a GL that receives aggregated journals instead of individual transactions, and
break ageing and ownership workflow.

"""Generate a synthetic day, run the full pipeline, and check the recon finds exactly the injected breaks."""

from conftest import BUSINESS_DATE
from e2e_recon.config import TableNames
from e2e_recon.pipeline import ReconPipeline
from e2e_recon.synthetic import BreakProfile, SyntheticFlowGenerator


def test_recon_detects_exactly_the_injected_breaks(spark, config, store, tmp_path):
    generator = SyntheticFlowGenerator(
        config, n_txns=2_000, profile=BreakProfile(missing=0.01, amount=0.01, duplicate=0.01), seed=7
    )
    flow = generator.generate(BUSINESS_DATE)
    generator.land(spark, flow, str(tmp_path))
    assert flow.injected, "profile should inject breaks"

    tables = TableNames("workspace", "e2e_recon")
    pipeline = ReconPipeline(spark, config, tables, store, landing_root=str(tmp_path))

    ingest = pipeline.ingest(BUSINESS_DATE, run_id="test-run")
    assert ingest.loaded == {name: len(rows) for name, rows in flow.records.items()}
    assert set(ingest.rejected.values()) == {0}

    summary = pipeline.reconcile(BUSINESS_DATE, run_id="test-run")
    assert summary.has_breaks

    details = store.read(tables.gold_break_details)
    adjacent = {
        (r["hop_label"], r["txn_id"], r["break_type"])
        for r in details.filter("pair_scope = 'ADJACENT'").collect()
    }
    assert adjacent == {(b.hop_label, b.txn_id, b.break_type) for b in flow.injected}

    # Every injected break is also visible end to end.
    e2e_ids = {r["txn_id"] for r in details.filter("pair_scope = 'END_TO_END'").collect()}
    assert {b.txn_id for b in flow.injected} == e2e_ids

    # Control totals agree with the record-level view: the source of each count diff is explained.
    results = store.read(tables.gold_recon_results)
    for label in summary.pair_status:
        count_diff = sum(r["count_diff"] for r in results.filter(f"hop_label = '{label}'").collect())
        record_delta = sum(
            r["target_count"] - r["source_count"] for r in details.filter(f"hop_label = '{label}'").collect()
        )
        assert count_diff == record_delta


def test_rerun_is_idempotent(spark, config, store, tmp_path):
    generator = SyntheticFlowGenerator(config, n_txns=300, seed=1)
    generator.land(spark, generator.generate(BUSINESS_DATE), str(tmp_path))
    pipeline = ReconPipeline(spark, config, TableNames("workspace", "e2e_recon"), store, landing_root=str(tmp_path))

    first = (pipeline.ingest(BUSINESS_DATE, "run-1"), pipeline.reconcile(BUSINESS_DATE, "run-1"))
    second = (pipeline.ingest(BUSINESS_DATE, "run-2"), pipeline.reconcile(BUSINESS_DATE, "run-2"))

    assert first[0] == second[0]
    assert first[1].pair_status == second[1].pair_status
    assert store.read("workspace.e2e_recon.gold_recon_results").select("run_id").distinct().count() == 1

    # The run log keeps every attempt; readers take the latest completed one.
    assert store.read("workspace.e2e_recon.gold_run_status").count() == 2
    assert pipeline.latest_completed_run(BUSINESS_DATE).run_id == "run-2"

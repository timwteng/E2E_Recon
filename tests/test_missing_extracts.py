"""A system that delivers nothing must be reported as such, not crash the run or look like 5,000 data breaks."""

import shutil

import pytest

from conftest import BUSINESS_DATE
from e2e_recon.adapters import MissingExtractError, SystemAdapter
from e2e_recon.config import TableNames
from e2e_recon.pipeline import EXTRACT_MISSING, ReconPipeline
from e2e_recon.synthetic import BreakProfile, SyntheticFlowGenerator

TABLES = TableNames("workspace", "e2e_recon")


@pytest.mark.parametrize("delivered", ["no folder", "empty folder", "zero-byte file"])
def test_nothing_delivered_raises_missing_extract(spark, config, tmp_path, delivered):
    adapter = SystemAdapter(config.systems[0])
    path = tmp_path / "extract"
    if delivered != "no folder":
        path.mkdir()
    if delivered == "zero-byte file":
        (path / "part.csv").write_bytes(b"")
    with pytest.raises(MissingExtractError):
        adapter.read_raw(spark, str(path), BUSINESS_DATE)


def _land(spark, config, tmp_path, profile=BreakProfile()):
    generator = SyntheticFlowGenerator(config, n_txns=500, profile=profile, seed=3)
    flow = generator.generate(BUSINESS_DATE)
    generator.land(spark, flow, str(tmp_path))
    return flow


def test_missing_stage_is_reported_and_other_hops_still_reconcile(spark, config, store, tmp_path):
    flow = _land(spark, config, tmp_path, BreakProfile(missing=0.02, amount=0.02, duplicate=0.02))
    missing_system = config.systems[2]  # payment_hub
    shutil.rmtree(SystemAdapter(missing_system).landing_path(str(tmp_path), BUSINESS_DATE))
    pipeline = ReconPipeline(spark, config, TABLES, store, landing_root=str(tmp_path))

    ingest = pipeline.ingest(BUSINESS_DATE, "run-1")
    assert ingest.missing == (missing_system.name,)
    assert ingest.loaded[missing_system.name] == 0
    statuses = {r["system"]: r["status"] for r in store.read(TABLES.silver_extract_status).collect()}
    assert statuses == {s.name: ("MISSING" if s is missing_system else "LOADED") for s in config.systems}

    summary = pipeline.reconcile(BUSINESS_DATE, "run-1")
    assert summary.missing_extracts == (missing_system.name,)
    for pair in config.hop_pairs:
        touches_missing = missing_system.name in (pair.source.name, pair.target.name)
        statuses = set(summary.pair_status[pair.label])
        if touches_missing:
            assert statuses == {EXTRACT_MISSING}, pair.label
            assert summary.record_breaks[pair.label] == {}, "no transaction-level noise for a missing extract"
        else:
            assert EXTRACT_MISSING not in statuses, pair.label

    # The hop that did arrive still finds exactly its injected breaks.
    first_hop = config.adjacent_pairs[0].label
    details = store.read(TABLES.gold_break_details).filter(f"hop_label = '{first_hop}'").collect()
    assert {(r["txn_id"], r["break_type"]) for r in details} == {
        (b.txn_id, b.break_type) for b in flow.injected if b.hop_label == first_hop
    }

    record = pipeline.latest_completed_run(BUSINESS_DATE)
    assert record.missing_extracts == (missing_system.name,)
    assert not record.is_clean


def test_stale_bronze_is_cleared_when_extract_goes_missing(spark, config, store, tmp_path):
    _land(spark, config, tmp_path)
    pipeline = ReconPipeline(spark, config, TABLES, store, landing_root=str(tmp_path))
    pipeline.ingest(BUSINESS_DATE, "run-1")
    target = config.systems[1]
    assert store.read(TABLES.bronze(target.name)).count() > 0

    shutil.rmtree(SystemAdapter(target).landing_path(str(tmp_path), BUSINESS_DATE))
    pipeline.ingest(BUSINESS_DATE, "run-2")
    assert store.read(TABLES.bronze(target.name)).count() == 0


def test_clean_day_passes_check(spark, config, store, tmp_path):
    _land(spark, config, tmp_path, BreakProfile(missing=0, amount=0, duplicate=0))
    pipeline = ReconPipeline(spark, config, TABLES, store, landing_root=str(tmp_path))
    assert pipeline.latest_completed_run(BUSINESS_DATE) is None  # nothing completed yet

    pipeline.ingest(BUSINESS_DATE, "run-1")
    summary = pipeline.reconcile(BUSINESS_DATE, "run-1")

    assert not summary.has_breaks
    record = pipeline.latest_completed_run(BUSINESS_DATE)
    assert record.is_clean and record.run_id == "run-1"

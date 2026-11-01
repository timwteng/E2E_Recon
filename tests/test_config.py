import pytest

from e2e_recon.config import ReconConfig, SystemSpec, TableNames


def test_packaged_config_defines_four_stage_flow(config):
    assert [s.name for s in config.systems] == ["card_switch", "card_processing", "payment_hub", "general_ledger"]
    assert [p.label for p in config.adjacent_pairs] == [
        "card_switch->card_processing",
        "card_processing->payment_hub",
        "payment_hub->general_ledger",
    ]
    assert config.hop_pairs[-1].scope == "END_TO_END"
    assert config.hop_pairs[-1].label == "card_switch->general_ledger"


def test_system_spec_rejects_incomplete_mapping():
    with pytest.raises(ValueError, match="missing column mappings"):
        SystemSpec(name="x", stage=1, file_format="csv", date_format="yyyy-MM-dd", columns={"txn_id": "id"})


def test_duplicate_stages_rejected(config):
    first = config.systems[0]
    with pytest.raises(ValueError, match="unique"):
        ReconConfig(rules=config.rules, systems=(first, first))


def test_table_names_reject_unsafe_identifiers():
    with pytest.raises(ValueError):
        TableNames("workspace", "recon; DROP TABLE x")
    assert TableNames("workspace", "e2e_recon").silver_txn == "workspace.e2e_recon.silver_txn"

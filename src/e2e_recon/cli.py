"""Job entry point: `recon <generate|ingest|reconcile|check> --catalog ... --schema ... [--business-date YYYY-MM-DD]`.

Exit codes: 0 = success, 1 = check found breaks or missing extracts, 2 = no completed recon to check,
3 = the business date could not be resolved.
"""

import argparse
import logging
import sys
import uuid

from pyspark.sql import SparkSession

from e2e_recon.config import ReconConfig, TableNames
from e2e_recon.dates import BusinessDateError, resolve_business_date
from e2e_recon.pipeline import ReconPipeline
from e2e_recon.storage import DeltaTableStore
from e2e_recon.synthetic import SyntheticFlowGenerator

log = logging.getLogger("e2e_recon")

EXIT_OK, EXIT_BREAKS, EXIT_NO_RUN, EXIT_BAD_DATE = 0, 1, 2, 3


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--catalog", required=True)
    common.add_argument("--schema", required=True)
    common.add_argument("--business-date", default="", help="YYYY-MM-DD. Required unless the run is scheduled.")
    common.add_argument("--trigger-type", default="", help="{{job.trigger.type}}; 'periodic' = scheduled run")
    common.add_argument("--trigger-time", default="", help="{{job.trigger.time.iso_datetime}} (UTC)")
    common.add_argument("--run-id", default="", help="{{job.run_id}}; a random id is used when omitted")
    common.add_argument("--landing-root", help="Override the landing location (default: the bundle's UC volume)")
    common.add_argument("--config", help="Path to a systems.yml (default: the packaged one)")

    parser = argparse.ArgumentParser(prog="recon", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser("generate", parents=[common], help="Land synthetic extracts with known breaks")
    gen.add_argument("--n-txns", type=int, default=5_000)
    gen.add_argument("--seed", type=int, default=42)

    commands.add_parser("ingest", parents=[common], help="Load extracts to bronze and standardise to silver")
    commands.add_parser("reconcile", parents=[common], help="Compute control totals and breaks to gold")
    commands.add_parser("check", parents=[common], help="Fail if the latest completed recon has breaks")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = _parser().parse_args(argv)
    config = ReconConfig.load(args.config)

    try:
        business_date = resolve_business_date(args.business_date, args.trigger_type, args.trigger_time, config.calendar)
    except BusinessDateError as exc:
        log.error("%s", exc)
        return EXIT_BAD_DATE
    run_id = args.run_id or uuid.uuid4().hex
    log.info("%s for business date %s (run %s)", args.command, business_date, run_id)

    spark = SparkSession.builder.getOrCreate()
    tables = TableNames(args.catalog, args.schema)
    store = DeltaTableStore(spark)
    landing_root = args.landing_root or tables.landing_root

    if args.command == "generate":
        generator = SyntheticFlowGenerator(config, n_txns=args.n_txns, seed=args.seed)
        flow = generator.generate(business_date)
        generator.land(spark, flow, landing_root)
        store.overwrite_date(flow.answer_key(spark), tables.synthetic_answer_key, "business_date", business_date)
        log.info("Landed %d txns with %d injected breaks", args.n_txns, len(flow.injected))
        return EXIT_OK

    pipeline = ReconPipeline(spark, config, tables, store, landing_root)

    if args.command == "ingest":
        summary = pipeline.ingest(business_date, run_id)
        log.info("Loaded per system: %s | rejected: %s", summary.loaded, summary.rejected)
        if summary.missing:
            log.warning("Missing extracts: %s (recon will continue with the stages that arrived)", summary.missing)
        return EXIT_OK

    if args.command == "reconcile":
        summary = pipeline.reconcile(business_date, run_id)
        for label, statuses in summary.pair_status.items():
            log.info("%-40s control: %s | records: %s", label, statuses, summary.record_breaks.get(label, {}))
        return EXIT_OK

    record = pipeline.latest_completed_run(business_date)
    if record is None:
        log.error("No completed recon found for %s", business_date)
        return EXIT_NO_RUN
    if record.is_clean:
        log.info("Recon %s for %s is clean", record.run_id, business_date)
        return EXIT_OK
    log.error(
        "Recon %s for %s needs attention: missing extracts=%s, control keys with breaks=%d, transactions in breaks=%d",
        record.run_id,
        business_date,
        list(record.missing_extracts) or "none",
        record.control_break_keys,
        record.record_breaks,
    )
    return EXIT_BREAKS


def entrypoint() -> None:
    sys.exit(main())


if __name__ == "__main__":
    entrypoint()

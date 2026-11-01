"""Job entry point: `recon <generate|ingest|reconcile> --catalog ... --schema ... --business-date YYYY-MM-DD`."""

import argparse
import logging
import sys
import uuid
from datetime import date

from pyspark.sql import SparkSession

from e2e_recon.config import ReconConfig, TableNames
from e2e_recon.pipeline import ReconPipeline
from e2e_recon.storage import DeltaTableStore
from e2e_recon.synthetic import SyntheticFlowGenerator

log = logging.getLogger("e2e_recon")


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--catalog", required=True)
    common.add_argument("--schema", required=True)
    common.add_argument("--business-date", required=True, type=date.fromisoformat)
    common.add_argument("--landing-root", help="Override the landing location (default: the bundle's UC volume)")
    common.add_argument("--config", help="Path to a systems.yml (default: the packaged one)")

    parser = argparse.ArgumentParser(prog="recon", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser("generate", parents=[common], help="Land synthetic extracts with known breaks")
    gen.add_argument("--n-txns", type=int, default=5_000)
    gen.add_argument("--seed", type=int, default=42)

    commands.add_parser("ingest", parents=[common], help="Load extracts to bronze and standardise to silver")

    rec = commands.add_parser("reconcile", parents=[common], help="Compute control totals and breaks to gold")
    rec.add_argument("--run-id", default=None)
    rec.add_argument("--fail-on-break", action="store_true", help="Exit non-zero when any break is found")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = _parser().parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    config = ReconConfig.load(args.config)
    tables = TableNames(args.catalog, args.schema)
    store = DeltaTableStore(spark)
    landing_root = args.landing_root or tables.landing_root

    if args.command == "generate":
        generator = SyntheticFlowGenerator(config, n_txns=args.n_txns, seed=args.seed)
        flow = generator.generate(args.business_date)
        generator.land(spark, flow, landing_root)
        store.overwrite_date(flow.answer_key(spark), tables.synthetic_answer_key, "business_date", args.business_date)
        log.info("Landed %d txns for %s with %d injected breaks", args.n_txns, args.business_date, len(flow.injected))
        return 0

    pipeline = ReconPipeline(spark, config, tables, store, landing_root)
    if args.command == "ingest":
        summary = pipeline.ingest(args.business_date)
        log.info("Loaded per system: %s | rejected: %s", summary.loaded, summary.rejected)
        return 0

    summary = pipeline.reconcile(args.business_date, args.run_id or uuid.uuid4().hex)
    for label, statuses in summary.pair_status.items():
        log.info("%-40s control: %s | records: %s", label, statuses, summary.record_breaks.get(label, {}))
    if summary.has_breaks and args.fail_on_break:
        log.error("Breaks found for %s", args.business_date)
        return 1
    return 0


def entrypoint() -> None:
    sys.exit(main())


if __name__ == "__main__":
    entrypoint()

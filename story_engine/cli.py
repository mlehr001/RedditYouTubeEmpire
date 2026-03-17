"""
Story Engine CLI.
Entry point for all pipeline operations.

Usage:
    python -m story_engine.cli ingest              # fetch + queue new stories
    python -m story_engine.cli worker normalize    # run normalize worker
    python -m story_engine.cli worker clean
    python -m story_engine.cli worker score
    python -m story_engine.cli worker hook
    python -m story_engine.cli worker validate
    python -m story_engine.cli worker censor
    python -m story_engine.cli worker format
    python -m story_engine.cli worker store
    python -m story_engine.cli worker video_build
    python -m story_engine.cli worker publish
    python -m story_engine.cli run-all             # run all workers (multiprocess)
    python -m story_engine.cli status              # pipeline health report
    python -m story_engine.cli analytics           # collect analytics snapshots
    python -m story_engine.cli queue-status        # show queue depths
    python -m story_engine.cli schema-init         # initialize DB schema
    python -m story_engine.cli scheduler           # start full automated scheduler
    python -m story_engine.cli trends              # show current trending topics
"""

import argparse
import logging
import multiprocessing
import sys
from pathlib import Path

from story_engine.config.settings import cfg
from story_engine.utils.logging_setup import setup_logging

logger = logging.getLogger(__name__)


WORKER_MAP = {
    "normalize": "story_engine.pipeline.normalize._NormalizeDispatcher",
    "clean": "story_engine.pipeline.clean.CleanWorker",
    "score": "story_engine.pipeline.score.ScoreWorker",
    "hook": "story_engine.pipeline.hook.HookWorker",
    "validate": "story_engine.pipeline.validate.ValidateWorker",
    "censor": "story_engine.pipeline.censor.CensorWorker",
    "format": "story_engine.pipeline.format_stage.FormatWorker",
    "store": "story_engine.pipeline.store.StoreWorker",
    "video_build": "story_engine.pipeline.video_build.VideoBuildWorker",
    "publish": "story_engine.pipeline.publish.PublishWorker",
}


def cmd_ingest(args):
    from story_engine.adapters.ingestion import Ingestion
    ingestion = Ingestion()
    stats = ingestion.run()
    print(f"Ingestion complete: {stats}")


def cmd_worker(args):
    stage = args.stage
    if stage not in WORKER_MAP:
        print(f"Unknown stage: {stage}. Valid: {list(WORKER_MAP.keys())}")
        sys.exit(1)

    module_path, class_name = WORKER_MAP[stage].rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    WorkerClass = getattr(module, class_name)
    worker = WorkerClass()
    logger.info("Starting worker: %s", stage)
    worker.run()


def _run_worker_process(stage: str):
    """Target function for multiprocess worker."""
    setup_logging(cfg.log_level)
    cmd_worker(argparse.Namespace(stage=stage))


def cmd_run_all(args):
    """Spawn all workers as separate processes."""
    processes = []
    stages = list(WORKER_MAP.keys())
    logger.info("Spawning %d worker processes: %s", len(stages), stages)

    for stage in stages:
        p = multiprocessing.Process(
            target=_run_worker_process,
            args=(stage,),
            name=f"worker-{stage}",
            daemon=True,
        )
        p.start()
        processes.append(p)
        logger.info("Started worker process: %s (pid=%d)", stage, p.pid)

    print(f"All {len(processes)} workers started. Press Ctrl+C to stop.")
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nShutting down workers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=5)
        print("Done.")


def cmd_status(args):
    from story_engine.pipeline.analyze import AnalyticsCollector
    collector = AnalyticsCollector()
    report = collector.pipeline_health_report()
    print("\n── Pipeline Health ─────────────────────────────")
    for status, count in sorted(report.items(), key=lambda x: -x[1]):
        bar = "█" * min(count, 40)
        print(f"  {status:<20} {count:>6}  {bar}")

    top = collector.get_top_performers(limit=5)
    if top:
        print("\n── Top Performers (last 30 days) ────────────────")
        for row in top:
            print(f"  [{row.get('platform')}] {row.get('ai_title', row.get('title', ''))[:50]}"
                  f"  views={row.get('max_views', 0):,}  score={row.get('score', '-')}")


def cmd_analytics(args):
    from story_engine.pipeline.analyze import AnalyticsCollector
    collector = AnalyticsCollector()
    stats = collector.run()
    print(f"Analytics collected: {stats}")


def cmd_queue_status(args):
    from story_engine.queue.job_queue import get_queue
    queue = get_queue()
    health = queue.health_check()
    print(f"\n── Queue Status ({health['status']}) ───────────────────────────")
    for stage, count in health["queues"].items():
        indicator = "!" if count > 100 else "·"
        print(f"  {indicator} {stage:<20} {count:>6} jobs")
    print(f"\n  DEAD LETTER: {health['dead_letter']}")


def cmd_schema_init(args):
    from story_engine.db.database import get_db
    db = get_db()
    schema_path = Path(__file__).parent / "db" / "schema.sql"
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
    print("Schema initialized")


def cmd_scheduler(args):
    """Start the full automated scheduler + all workers."""
    from story_engine.scheduler import main as scheduler_main
    scheduler_main()


def cmd_trends(args):
    """Show current trending topics from RSS/NewsAPI signals."""
    from story_engine.adapters.trend_signals import TrendScanner
    scanner = TrendScanner()
    print("\n── Trending Topics ──────────────────────────────────────")
    for i, topic in enumerate(scanner.get_trending_topics()[:20], 1):
        print(f"  {i:2d}. {topic}")
    print("\n── Hot Keywords ─────────────────────────────────────────")
    keywords = scanner.get_hot_keywords(top_n=15)
    print("  " + ", ".join(keywords))


def main():
    setup_logging(cfg.log_level)

    parser = argparse.ArgumentParser(
        description="Story Engine — AI content pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("ingest", help="Fetch new stories from all sources")

    wp = subparsers.add_parser("worker", help="Run a pipeline stage worker")
    wp.add_argument("stage", choices=list(WORKER_MAP.keys()))

    subparsers.add_parser("run-all", help="Run all workers (multiprocess)")
    subparsers.add_parser("status", help="Show pipeline health report")
    subparsers.add_parser("analytics", help="Collect analytics snapshots")
    subparsers.add_parser("queue-status", help="Show Redis queue depths")
    subparsers.add_parser("schema-init", help="Initialize PostgreSQL schema")
    subparsers.add_parser("scheduler", help="Start full automated scheduler + workers")
    subparsers.add_parser("trends", help="Show current trending topics")

    args = parser.parse_args()
    dispatch = {
        "ingest": cmd_ingest,
        "worker": cmd_worker,
        "run-all": cmd_run_all,
        "status": cmd_status,
        "analytics": cmd_analytics,
        "queue-status": cmd_queue_status,
        "schema-init": cmd_schema_init,
        "scheduler": cmd_scheduler,
        "trends": cmd_trends,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
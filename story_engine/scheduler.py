"""
Story Engine Scheduler.
Runs the full pipeline automatically on configurable intervals.
Uses APScheduler for reliable job scheduling.

Schedule (defaults, all configurable via env):
  - Ingest:    every 3 hours (pulls new stories from all sources)
  - Analytics: every 6 hours (snapshots view/like counts)
  - Cleanup:   daily at 3am  (purge rejected stories older than 30 days)

Workers run continuously as separate processes — the scheduler
only fires the INGESTION trigger. Workers self-loop via Redis queue.

Run: python -m story_engine.scheduler
"""

import logging
import multiprocessing
import os
import signal
import sys
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from story_engine.config.settings import cfg
from story_engine.utils.logging_setup import setup_logging

logger = logging.getLogger(__name__)


# ── Schedule config (override with env vars) ──────────────────────────────────
INGEST_INTERVAL_HOURS = int(os.environ.get("INGEST_INTERVAL_HOURS", "3"))
ANALYTICS_INTERVAL_HOURS = int(os.environ.get("ANALYTICS_INTERVAL_HOURS", "6"))
CLEANUP_CRON_HOUR = int(os.environ.get("CLEANUP_CRON_HOUR", "3"))  # 3am daily

# ── Worker map ────────────────────────────────────────────────────────────────
WORKER_STAGES = [
    "normalize", "clean", "score", "hook",
    "validate", "censor", "format", "store",
    "video_build", "publish",
]


def job_ingest():
    """Scheduled job: fetch new stories from all sources."""
    logger.info("[scheduler] ingest job started at %s", datetime.utcnow().isoformat())
    try:
        from story_engine.adapters.ingestion import Ingestion
        ingestion = Ingestion()
        stats = ingestion.run()
        logger.info("[scheduler] ingest complete: %s", stats["total"])
    except Exception as e:
        logger.exception("[scheduler] ingest job failed: %s", e)


def job_analytics():
    """Scheduled job: collect analytics snapshots for published videos."""
    logger.info("[scheduler] analytics job started")
    try:
        from story_engine.pipeline.analyze import AnalyticsCollector
        collector = AnalyticsCollector()
        stats = collector.run()
        logger.info("[scheduler] analytics complete: %s", stats)
    except Exception as e:
        logger.exception("[scheduler] analytics job failed: %s", e)


def job_cleanup():
    """Scheduled job: purge old rejected/failed stories to keep DB lean."""
    logger.info("[scheduler] cleanup job started")
    try:
        from story_engine.db.database import get_db
        db = get_db()

        # Delete rejected/failed stories older than 30 days
        result = db.execute(
            """
            DELETE FROM stories
            WHERE pipeline_status IN ('rejected', 'failed')
              AND updated_at < NOW() - INTERVAL '30 days'
            RETURNING id
            """
        )
        deleted = len(result)

        # Archive pipeline_jobs older than 14 days
        db.execute(
            """
            DELETE FROM pipeline_jobs
            WHERE finished_at < NOW() - INTERVAL '14 days'
              AND status IN ('done', 'failed')
            """
        )

        logger.info("[scheduler] cleanup complete | stories_deleted=%d", deleted)
    except Exception as e:
        logger.exception("[scheduler] cleanup job failed: %s", e)


def job_health_report():
    """Log a pipeline health snapshot every hour."""
    try:
        from story_engine.pipeline.analyze import AnalyticsCollector
        from story_engine.queue.job_queue import get_queue
        collector = AnalyticsCollector()
        report = collector.pipeline_health_report()
        queue = get_queue()
        health = queue.health_check()
        logger.info("[scheduler] health | pipeline=%s | queues=%s",
                    report, health["queues"])
    except Exception as e:
        logger.debug("[scheduler] health report failed: %s", e)


def _run_worker(stage: str):
    """Worker subprocess entry point."""
    setup_logging(cfg.log_level)
    import importlib
    worker_map = {
        "normalize": ("story_engine.pipeline.normalize", "_NormalizeDispatcher"),
        "clean":     ("story_engine.pipeline.clean", "CleanWorker"),
        "score":     ("story_engine.pipeline.score", "ScoreWorker"),
        "hook":      ("story_engine.pipeline.hook", "HookWorker"),
        "validate":  ("story_engine.pipeline.validate", "ValidateWorker"),
        "censor":    ("story_engine.pipeline.censor", "CensorWorker"),
        "format":    ("story_engine.pipeline.format_stage", "FormatWorker"),
        "store":     ("story_engine.pipeline.store", "StoreWorker"),
        "video_build": ("story_engine.pipeline.video_build", "VideoBuildWorker"),
        "publish":   ("story_engine.pipeline.publish", "PublishWorker"),
    }
    module_path, class_name = worker_map[stage]
    mod = importlib.import_module(module_path)
    worker = getattr(mod, class_name)()
    worker.run()


def start_workers() -> list:
    """Start all pipeline workers as daemon processes."""
    processes = []
    for stage in WORKER_STAGES:
        p = multiprocessing.Process(
            target=_run_worker,
            args=(stage,),
            name=f"worker-{stage}",
            daemon=True,
        )
        p.start()
        logger.info("[scheduler] started worker: %s (pid=%d)", stage, p.pid)
        processes.append(p)
    return processes


def main():
    setup_logging(cfg.log_level)
    logger.info("=" * 60)
    logger.info("Story Engine Scheduler starting")
    logger.info("  Ingest interval:    every %dh", INGEST_INTERVAL_HOURS)
    logger.info("  Analytics interval: every %dh", ANALYTICS_INTERVAL_HOURS)
    logger.info("  Cleanup:            daily at %d:00", CLEANUP_CRON_HOUR)
    logger.info("=" * 60)

    # Start all pipeline workers
    worker_procs = start_workers()
    logger.info("[scheduler] %d workers started", len(worker_procs))

    # Brief pause to let workers initialize
    time.sleep(3)

    # Run ingest immediately on startup
    logger.info("[scheduler] running initial ingest...")
    job_ingest()

    # Build scheduler
    scheduler = BlockingScheduler(timezone="UTC")

    # Ingest every N hours
    scheduler.add_job(
        job_ingest,
        trigger=IntervalTrigger(hours=INGEST_INTERVAL_HOURS),
        id="ingest",
        name="Ingest new stories",
        max_instances=1,
        coalesce=True,
    )

    # Analytics every N hours
    scheduler.add_job(
        job_analytics,
        trigger=IntervalTrigger(hours=ANALYTICS_INTERVAL_HOURS),
        id="analytics",
        name="Collect analytics snapshots",
        max_instances=1,
        coalesce=True,
    )

    # Cleanup daily at configured hour
    scheduler.add_job(
        job_cleanup,
        trigger=CronTrigger(hour=CLEANUP_CRON_HOUR, minute=0),
        id="cleanup",
        name="Purge old rejected stories",
        max_instances=1,
    )

    # Health report every hour
    scheduler.add_job(
        job_health_report,
        trigger=IntervalTrigger(hours=1),
        id="health",
        name="Pipeline health report",
        max_instances=1,
    )

    # Graceful shutdown
    def _shutdown(signum, frame):
        logger.info("[scheduler] shutdown signal — stopping...")
        scheduler.shutdown(wait=False)
        for p in worker_procs:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("[scheduler] running — Ctrl+C to stop")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] stopped")


if __name__ == "__main__":
    main()
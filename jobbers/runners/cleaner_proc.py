import argparse
import asyncio
import datetime as dt
import logging
import sys

parser = argparse.ArgumentParser(description="Jobbers Cleaner")
parser.add_argument(
    "--rate-limit-age",
    type=lambda x: dt.timedelta(seconds=int(x)),
    default=None,
    help="Rate limit age in seconds (default: 7 days)",
)
parser.add_argument(
    "--min-queue-age",
    type=lambda x: dt.datetime.fromtimestamp(int(x), tz=dt.UTC),
    default=None,
    help="Minimum queue age in seconds (default: 7 days)",
)
parser.add_argument(
    "--max-queue-age",
    type=lambda x: dt.datetime.fromtimestamp(int(x), tz=dt.UTC),
    default=None,
    help="Maximum queue age in seconds (default: 30 days)",
)
parser.add_argument(
    "--stale-time",
    type=lambda x: dt.timedelta(seconds=int(x)),
    default=None,
    help="Mark tasks as stalled if their heartbeat is older than this many seconds",
)
parser.add_argument(
    "--dlq-age",
    type=lambda x: dt.timedelta(seconds=int(x)),
    default=None,
    help="Remove dead-letter queue entries older than this many seconds",
)
parser.add_argument(
    "--completed-task-age",
    type=lambda x: dt.timedelta(seconds=int(x)),
    default=None,
    help="Delete task blobs and heartbeat entries for terminal tasks older than this many seconds",
)
parser.add_argument(
    "--rebuild-status-index",
    action="store_true",
    default=False,
    help="Backfill task-status-idx sorted sets from existing task blobs",
)


def run() -> None:
    from jobbers.state_manager import build_sm

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]

    from jobbers.utils.otel import enable_otel
    enable_otel(handlers, service_name="jobbers-cleaner")

    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    args = parser.parse_args()
    state_manager = build_sm()

    loop = asyncio.get_event_loop()
    if args.rebuild_status_index:
        loop.run_until_complete(state_manager.ta.rebuild_status_indexes())
    loop.run_until_complete(
        state_manager.clean(
            args.rate_limit_age,
            args.min_queue_age,
            args.max_queue_age,
            stale_time=args.stale_time,
            dlq_age=args.dlq_age,
            completed_task_age=args.completed_task_age,
        )
    )

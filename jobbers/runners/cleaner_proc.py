import argparse
import asyncio
import datetime as dt
import logging
import sys

from jobbers import db
from jobbers.adapters.static import StaticRoutingBackend
from jobbers.utils.otel import enable_otel

parser = argparse.ArgumentParser(description="Jobbers Cleaner")
parser.add_argument(
    "--static-config",
    metavar="FILE",
    default=None,
    help="Path to a JSON/YAML static routing config file. Implies ROUTING_BACKEND=static.",
)
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
    "--recover-orphaned-scheduled",
    action="store_true",
    default=False,
    help="Re-add scheduled tasks orphaned by a crash between acquisition and dispatch.",
)
parser.add_argument(
    "--drop-stale-indexes",
    action="store_true",
    default=False,
    help="Drop RediSearch indexes left behind by older schema versions (RedisJSON backends only).",
)


async def cleaner(args: argparse.Namespace) -> None:
    if args.static_config:
        db.register_routing_backend(StaticRoutingBackend.from_file(args.static_config))

    state_manager = await db.init_state_manager()
    await state_manager.clean(
        rate_limit_age=args.rate_limit_age,
        min_queue_age=args.min_queue_age,
        max_queue_age=args.max_queue_age,
        stale_time=args.stale_time,
        dlq_age=args.dlq_age,
        completed_task_age=args.completed_task_age,
        recover_orphaned_scheduled=args.recover_orphaned_scheduled,
        drop_stale_indexes=args.drop_stale_indexes,
    )


def run() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]
    enable_otel(handlers, service_name="jobbers-cleaner")

    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(cleaner(args))

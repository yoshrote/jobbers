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
    type=lambda x: dt.datetime.fromtimestamp(int(x), tz=dt.timezone.utc),
    default=None,
    help="Minimum queue age in seconds (default: 7 days)",
)
parser.add_argument(
    "--max-queue-age",
    type=lambda x: dt.datetime.fromtimestamp(int(x), tz=dt.timezone.utc),
    default=None,
    help="Maximum queue age in seconds (default: 30 days)",
)


def run():
    from .state_manager import build_sm

    handlers = [logging.StreamHandler(stream=sys.stdout)]

    from jobbers.otel import enable_otel
    enable_otel(handlers, service_name="jobbers-cleaner")

    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    args = parser.parse_args()
    state_manager = build_sm()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        state_manager.clean(args.rate_limit_age, args.min_queue_age, args.max_queue_age)
    )

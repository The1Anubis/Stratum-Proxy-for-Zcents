"""Command line interface for the Zcents Stratum proxy."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from contextlib import suppress

from .config import load_config
from .proxy import StratumProxy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Zcents Stratum proxy")
    parser.add_argument(
        "--config",
        "-c",
        default="config.json",
        help="Path to the proxy configuration file (JSON or TOML).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    return parser.parse_args()


async def _run_proxy(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    config = load_config(args.config)
    proxy = StratumProxy(config)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        if not stop_event.is_set():
            logging.getLogger("stratum_proxy").info("Received shutdown signal")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal_handler)

    server_task = asyncio.create_task(proxy.serve())

    await stop_event.wait()
    await proxy.stop()
    with suppress(asyncio.CancelledError):
        await server_task


def main() -> None:
    args = parse_args()
    asyncio.run(_run_proxy(args))


if __name__ == "__main__":  # pragma: no cover - manual execution entry point
    main()

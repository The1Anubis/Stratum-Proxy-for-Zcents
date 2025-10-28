"""Async Stratum proxy implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from .config import ProxyConfig

_LOGGER = logging.getLogger("stratum_proxy")


@dataclass(slots=True)
class ConnectionContext:
    """State tracked for each connected miner."""

    miner_id: int
    miner_address: str
    worker_name: str | None = None


class StratumProxy:
    """A lightweight Stratum proxy that forwards miners to a Zcents node."""

    def __init__(self, config: ProxyConfig) -> None:
        self._config = config
        self._server: asyncio.base_events.Server | None = None
        self._shutdown = asyncio.Event()
        self._connection_id = 0
        self._active_tasks: set[asyncio.Task[Any]] = set()

    async def serve(self) -> None:
        """Start the proxy server and run until :meth:`stop` is called."""

        if self._server is not None:
            raise RuntimeError("Proxy server already running")

        _LOGGER.info(
            "Starting Stratum proxy on %s:%s → %s:%s",
            self._config.listen_host,
            self._config.listen_port,
            self._config.upstream.host,
            self._config.upstream.port,
        )

        self._server = await asyncio.start_server(
            self._handle_miner,
            host=self._config.listen_host,
            port=self._config.listen_port,
        )

        async with self._server:
            await self._shutdown.wait()

        _LOGGER.info("Stratum proxy stopped")

    async def stop(self) -> None:
        """Stop the proxy server and wait for all client tasks to exit."""

        if self._server is None:
            return

        self._server.close()
        await self._server.wait_closed()
        self._shutdown.set()

        for task in list(self._active_tasks):
            task.cancel()
        for task in list(self._active_tasks):
            with suppress(asyncio.CancelledError):
                await task

    async def _handle_miner(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._connection_id += 1
        context = ConnectionContext(
            miner_id=self._connection_id,
            miner_address=self._format_address(writer),
        )
        _LOGGER.info("Miner #%s connected from %s", context.miner_id, context.miner_address)

        try:
            upstream_reader, upstream_writer = await self._connect_upstream(context)
        except Exception as exc:  # pragma: no cover - network failure is runtime behaviour
            _LOGGER.error("Failed to connect upstream for miner #%s: %s", context.miner_id, exc)
            writer.close()
            await writer.wait_closed()
            return

        pump_tasks = {
            asyncio.create_task(
                self._pump_miner_to_upstream(context, reader, upstream_writer),
                name=f"miner→upstream#{context.miner_id}",
            ),
            asyncio.create_task(
                self._pump_upstream_to_miner(context, upstream_reader, writer),
                name=f"upstream→miner#{context.miner_id}",
            ),
        }

        self._active_tasks.update(pump_tasks)

        done, pending = await asyncio.wait(
            pump_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        for task in pump_tasks:
            self._active_tasks.discard(task)
            with suppress(asyncio.CancelledError):
                await task

        writer.close()
        upstream_writer.close()
        with suppress(Exception):
            await writer.wait_closed()
            await upstream_writer.wait_closed()

        _LOGGER.info("Miner #%s disconnected", context.miner_id)

    async def _connect_upstream(
        self, context: ConnectionContext
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish a connection to the upstream Zcents node."""

        connector = asyncio.open_connection
        ssl_ctx: ssl.SSLContext | None = None
        if self._config.upstream.ssl:
            ssl_ctx = ssl.create_default_context()

        attempt = 0
        while True:
            try:
                reader, writer = await connector(
                    self._config.upstream.host,
                    self._config.upstream.port,
                    ssl=ssl_ctx,
                )
                _LOGGER.info(
                    "Miner #%s linked to upstream at %s:%s",
                    context.miner_id,
                    self._config.upstream.host,
                    self._config.upstream.port,
                )
                return reader, writer
            except OSError as exc:
                attempt += 1
                _LOGGER.warning(
                    "Attempt %s failed to reach upstream for miner #%s: %s",
                    attempt,
                    context.miner_id,
                    exc,
                )
                await asyncio.sleep(self._config.upstream_reconnect)

    async def _pump_miner_to_upstream(
        self,
        context: ConnectionContext,
        reader: asyncio.StreamReader,
        upstream_writer: asyncio.StreamWriter,
    ) -> None:
        """Forward messages from the miner to the upstream node."""

        try:
            while True:
                try:
                    line = await asyncio.wait_for(
                        reader.readline(),
                        timeout=self._config.miner_timeout,
                    )
                except asyncio.TimeoutError:
                    _LOGGER.warning(
                        "Miner #%s timed out after %.0fs",
                        context.miner_id,
                        self._config.miner_timeout,
                    )
                    return

                if not line:
                    return

                if len(line) > self._config.max_message_size:
                    _LOGGER.warning(
                        "Miner #%s sent oversized message (%s bytes) – dropping connection",
                        context.miner_id,
                        len(line),
                    )
                    return

                try:
                    line = self._rewrite_outbound(context, line)
                except ValueError as exc:
                    _LOGGER.error(
                        "Failed to parse miner #%s payload: %s", context.miner_id, exc
                    )
                    continue

                upstream_writer.write(line)
                await upstream_writer.drain()
        finally:
            upstream_writer.close()

    async def _pump_upstream_to_miner(
        self,
        context: ConnectionContext,
        upstream_reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Forward messages from the upstream node to the miner."""

        keepalive_task: asyncio.Task[None] | None = None
        if self._config.upstream.keepalive_interval > 0:
            keepalive_task = asyncio.create_task(
                self._send_keepalives(
                    context,
                    writer=writer,
                    interval=self._config.upstream.keepalive_interval,
                ),
                name=f"keepalive#{context.miner_id}",
            )

        try:
            while True:
                line = await upstream_reader.readline()
                if not line:
                    return

                if len(line) > self._config.max_message_size:
                    _LOGGER.debug(
                        "Truncating oversized upstream message for miner #%s (%s bytes)",
                        context.miner_id,
                        len(line),
                    )
                    line = line[: self._config.max_message_size]

                try:
                    line = self._rewrite_inbound(context, line)
                except ValueError as exc:
                    _LOGGER.error(
                        "Failed to parse upstream payload for miner #%s: %s",
                        context.miner_id,
                        exc,
                    )
                    continue

                writer.write(line)
                await writer.drain()
        finally:
            if keepalive_task:
                keepalive_task.cancel()
                with suppress(asyncio.CancelledError):
                    await keepalive_task
            writer.close()

    async def _send_keepalives(
        self,
        context: ConnectionContext,
        writer: asyncio.StreamWriter,
        interval: float,
    ) -> None:
        """Send passive keepalive notifications to the miner."""

        payload = json.dumps({"id": None, "method": "mining.keepalive", "params": []}) + "\n"
        data = payload.encode()

        while not writer.is_closing():
            await asyncio.sleep(interval)
            writer.write(data)
            with suppress(Exception):
                await writer.drain()
            _LOGGER.debug("Sent keepalive to miner #%s", context.miner_id)

    def _rewrite_outbound(self, context: ConnectionContext, raw: bytes) -> bytes:
        """Rewrite messages from miners before they go upstream."""

        try:
            message = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON: {exc}") from exc

        if isinstance(message, dict) and message.get("method") == "mining.authorize":
            params = list(message.get("params", []))
            username = self._config.upstream.username
            password = self._config.upstream.password

            if params:
                worker_param = str(params[0])
                if username:
                    suffix = (
                        worker_param.split(".")[-1]
                        if self._config.upstream.worker_prefix
                        else worker_param.split(".")[-1]
                    )
                    worker_name = (
                        f"{self._config.upstream.worker_prefix}.{suffix}" if self._config.upstream.worker_prefix else f"{username}.{suffix}" if suffix else username
                    )
                else:
                    worker_name = worker_param
            else:
                worker_name = username or ""

            context.worker_name = worker_name
            new_params = [worker_name]

            if password is not None:
                new_params.append(password)
            elif len(params) > 1:
                new_params.append(params[1])

            if len(params) > 2:
                new_params.extend(params[2:])

            message["params"] = new_params

        encoded = (json.dumps(message) + "\n").encode("utf-8")
        _LOGGER.debug("Miner #%s → upstream: %s", context.miner_id, encoded.decode().strip())
        return encoded

    def _rewrite_inbound(self, context: ConnectionContext, raw: bytes) -> bytes:
        """Rewrite messages from the upstream node before delivering to the miner."""

        try:
            message = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON: {exc}") from exc

        # Update result IDs in logs for easier debugging.
        if isinstance(message, dict) and "result" in message and message.get("id") is not None:
            _LOGGER.debug(
                "Upstream → miner #%s response id=%s", context.miner_id, message["id"]
            )

        encoded = (json.dumps(message) + "\n").encode("utf-8")
        return encoded

    @staticmethod
    def _format_address(writer: asyncio.StreamWriter) -> str:
        sock = writer.get_extra_info("peername")
        if not sock:
            return "?"
        if isinstance(sock, tuple):
            host, port, *_ = sock
            return f"{host}:{port}"
        return str(sock)


__all__ = ["StratumProxy"]

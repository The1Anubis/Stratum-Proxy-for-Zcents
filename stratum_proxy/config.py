"""Configuration helpers for the Zcents Stratum proxy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json

try:  # pragma: no cover - tomllib is not available in <3.11
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore


@dataclass(slots=True)
class UpstreamConfig:
    """Configuration describing how the proxy talks to the Zcents node."""

    host: str
    port: int
    ssl: bool = False
    username: str | None = None
    password: str | None = None
    worker_prefix: str | None = None
    keepalive_interval: float = 0.0


@dataclass(slots=True)
class ProxyConfig:
    """Top-level configuration for the proxy server."""

    listen_host: str
    listen_port: int
    upstream: UpstreamConfig
    miner_timeout: float = 300.0
    upstream_reconnect: float = 5.0
    max_message_size: int = 64 * 1024


def load_config(path: str | Path) -> ProxyConfig:
    """Load a :class:`ProxyConfig` from a JSON or TOML file."""

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file does not exist: {path}")

    data: dict[str, Any]
    if path.suffix in {".json", ""}:  # default to JSON when no suffix
        data = json.loads(path.read_text())
    elif path.suffix in {".toml", ".tml"}:
        if tomllib is None:  # pragma: no cover - python <3.11
            raise RuntimeError(
                "tomllib is unavailable. Install tomli or use a JSON config file."
            )
        data = tomllib.loads(path.read_text())
    else:
        raise ValueError(
            "Unsupported config format. Use .json or .toml files for configuration."
        )

    return _config_from_dict(data)


def _config_from_dict(data: dict[str, Any]) -> ProxyConfig:
    try:
        upstream_data = data["upstream"]
    except KeyError as exc:  # pragma: no cover - validated by tests
        raise ValueError("Missing 'upstream' section in configuration") from exc

    upstream = UpstreamConfig(
        host=_require(upstream_data, "host"),
        port=int(_require(upstream_data, "port")),
        ssl=bool(upstream_data.get("ssl", False)),
        username=upstream_data.get("username"),
        password=upstream_data.get("password"),
        worker_prefix=upstream_data.get("worker_prefix"),
        keepalive_interval=float(upstream_data.get("keepalive_interval", 0.0)),
    )

    return ProxyConfig(
        listen_host=data.get("listen_host", "0.0.0.0"),
        listen_port=int(data.get("listen_port", 3333)),
        upstream=upstream,
        miner_timeout=float(data.get("miner_timeout", 300.0)),
        upstream_reconnect=float(data.get("upstream_reconnect", 5.0)),
        max_message_size=int(data.get("max_message_size", 64 * 1024)),
    )


def _require(data: dict[str, Any], key: str) -> Any:
    if key not in data:
        raise ValueError(f"Missing required key '{key}' in configuration")
    return data[key]


__all__ = ["ProxyConfig", "UpstreamConfig", "load_config"]

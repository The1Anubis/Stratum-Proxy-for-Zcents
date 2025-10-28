from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from stratum_proxy.config import ProxyConfig, UpstreamConfig, load_config


class LoadConfigTests(unittest.TestCase):
    def test_loads_json_config(self) -> None:
        data = {
            "listen_host": "127.0.0.1",
            "listen_port": 9999,
            "miner_timeout": 100,
            "upstream_reconnect": 2,
            "max_message_size": 128,
            "upstream": {
                "host": "upstream.local",
                "port": 8444,
                "username": "user",
                "password": "pass",
                "worker_prefix": "zcents",
                "keepalive_interval": 10,
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps(data))
            config = load_config(path)

        self.assertIsInstance(config, ProxyConfig)
        self.assertEqual(config.listen_host, "127.0.0.1")
        self.assertEqual(config.listen_port, 9999)
        self.assertEqual(config.upstream.host, "upstream.local")
        self.assertEqual(config.upstream.username, "user")
        self.assertEqual(config.upstream.worker_prefix, "zcents")
        self.assertEqual(config.upstream.keepalive_interval, 10)


class ProxyConfigEqualityTests(unittest.TestCase):
    def test_dataclasses_can_be_instantiated(self) -> None:
        upstream = UpstreamConfig(host="node", port=1234)
        config = ProxyConfig(listen_host="0.0.0.0", listen_port=3333, upstream=upstream)
        self.assertEqual(config.upstream.host, "node")
        self.assertFalse(config.upstream.ssl)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

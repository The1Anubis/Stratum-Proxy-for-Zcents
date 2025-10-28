from __future__ import annotations

import json
import unittest

from stratum_proxy.config import ProxyConfig, UpstreamConfig
from stratum_proxy.proxy import ConnectionContext, StratumProxy


class RewriteTests(unittest.TestCase):
    def setUp(self) -> None:
        upstream = UpstreamConfig(
            host="node",
            port=1234,
            username="zcents.pool",
            password="secret",
            worker_prefix="zcents",
        )
        self.proxy = StratumProxy(
            ProxyConfig(
                listen_host="0.0.0.0",
                listen_port=3333,
                upstream=upstream,
                miner_timeout=30,
                upstream_reconnect=1,
            )
        )
        self.context = ConnectionContext(miner_id=1, miner_address="127.0.0.1:5000")

    def test_authorize_rewrite_sets_credentials(self) -> None:
        payload = {
            "id": 1,
            "method": "mining.authorize",
            "params": ["worker-a.test", "x"],
        }
        raw = (json.dumps(payload) + "\n").encode()
        rewritten = self.proxy._rewrite_outbound(self.context, raw)
        message = json.loads(rewritten.decode())
        self.assertEqual(message["params"][0], "zcents.test")
        self.assertEqual(message["params"][1], "secret")

    def test_passthrough_for_other_methods(self) -> None:
        payload = {"id": 2, "method": "mining.submit", "params": ["foo"]}
        raw = (json.dumps(payload) + "\n").encode()
        rewritten = self.proxy._rewrite_outbound(self.context, raw)
        self.assertEqual(json.loads(rewritten.decode()), payload)

    def test_inbound_rewrite_maintains_structure(self) -> None:
        payload = {"id": 1, "result": True}
        raw = (json.dumps(payload) + "\n").encode()
        rewritten = self.proxy._rewrite_inbound(self.context, raw)
        self.assertEqual(json.loads(rewritten.decode()), payload)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

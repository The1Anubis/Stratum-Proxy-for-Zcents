# Stratum Proxy for Zcents

A lightweight Stratum V1 proxy that lets ASIC miners such as the Antminer Z9 Mini connect to a Zcents node. The proxy accepts connections from miners on a configurable TCP port and forwards traffic to your Zcents stratum endpoint while normalising worker credentials and keeping connections alive.

## Features

- Asyncio-based server capable of handling multiple miners concurrently
- Automatic reconnection to the upstream Zcents node
- Optional worker name rewriting so every miner shares the same Zcents credential
- Optional keepalive pings to prevent idle disconnects
- Simple JSON or TOML configuration file

## Getting started

### 1. Create a configuration file

Copy the example file and update it with the address of your Zcents node:

```bash
cp config.example.json config.json
```

Then edit `config.json` and replace the placeholder host, port and credentials with values that match your deployment. If you are using TLS on the upstream node set `"ssl": true`. Set `keepalive_interval` to a value greater than zero only if your miners require periodic notifications.

### 2. Run the proxy

Install the package locally (optional, the project can also be executed in-place) and start the proxy:

```bash
pip install -e .
zcents-stratum-proxy --config config.json --log-level INFO
```

The server will listen for miners on the configured `listen_host`/`listen_port` and automatically forward traffic to the upstream Zcents node.

### 3. Point your miners to the proxy

Update each ASIC to use the machine running the proxy as its pool address. For example:

```
pool address: stratum+tcp://your-proxy-host:3333
worker name: anything-you-like
password: x
```

The proxy replaces the worker name and password with the credentials defined in the configuration so that the upstream node only sees a single authorised identity.

## Running in production

- Use a process manager such as `systemd`, `supervisord` or Docker to keep the proxy alive.
- Enable TLS (`ssl = true`) if your Zcents node is exposed over a secure connection.
- Tune `miner_timeout` and `keepalive_interval` to match your miners' behaviour.
- For better observability run with `--log-level DEBUG` to view proxied JSON-RPC messages.

## License

This project is released under the MIT License.

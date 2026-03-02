# nats-ewebsock

[NATS](https://nats.io/) WebSocket client for Rust that compiles to both WebAssembly and native.

Based on the [ewebsock](https://github.com/rerun-io/ewebsock) crate for WebSocket support in WebAssembly.

## Status

This crate is in early development. The API may change significantly in future releases.

Currently, it supports basic connection and messaging functionality:

- Connect to a NATS server over WebSocket
- Subscribe to subjects
- Receive messages
- Publish messages
- Unsubscribe from subjects

Planned features:

- Message Headers
- Request-Reply pattern
- Advanced connection options

## Usage

See [simple.rs](./examples/simple.rs) for a simple example.

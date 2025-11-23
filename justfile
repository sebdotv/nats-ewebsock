build:
    cargo build --all-targets
    cargo build --all-targets --all-features
    cargo build --target wasm32-unknown-unknown
    cargo build --target wasm32-unknown-unknown --all-features

doc:
    rm -rf target/doc
    cargo doc --lib --all-features --no-deps --open

test:
    cargo nextest run

ci:
    cargo sort --workspace
    cargo +nightly fmt --all
    just _clippy --all-targets --all-features
    cargo test --all-targets --all-features

# runs clippy twice: first time tries to fix issues, second time fails if there are still warnings
_clippy *args:
    cargo clippy --allow-dirty --fix {{ args }}
    cargo clippy {{ args }} -- -D warnings

start-nats-server:
    docker run -it --rm  -v {{ justfile_directory() }}/tests/resources/nats.conf:/container/nats.conf -p 4222:4222 -p 4223:4223 nats -c /container/nats.conf

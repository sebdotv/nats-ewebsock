use std::fmt::{Display, Formatter};

/// Result type for NATS over WebSocket client.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for NATS over WebSocket client.
#[derive(Debug)]
pub enum Error {
    /// WebSocket related errors.
    WebSocket(String),
    /// Message channel related errors.
    MessageChannel(String),
    /// NATS protocol related errors.
    NatsProtocol(String),
    /// Invalid state errors.
    State(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::WebSocket(msg) => write!(f, "WebSocket error: {}", msg),
            Error::MessageChannel(msg) => write!(f, "Message channel error: {}", msg),
            Error::NatsProtocol(msg) => write!(f, "NATS protocol error: {}", msg),
            Error::State(msg) => write!(f, "Invalid state error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

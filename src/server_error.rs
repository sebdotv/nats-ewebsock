/// Result type for NATS server operations.
pub type ServerResult = Result<(), ServerError>;

/// Errors returned by the NATS server.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ServerError {
    InvalidSubject,
    Other(String),
}
impl From<&str> for ServerError {
    fn from(s: &str) -> Self {
        match s {
            "Invalid Subject" => ServerError::InvalidSubject,
            other => ServerError::Other(other.to_owned()),
        }
    }
}

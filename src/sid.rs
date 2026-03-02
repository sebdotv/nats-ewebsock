use std::str::FromStr;

/// NATS subscription ID.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubscriptionId {
    inner: String,
}
impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionId {
    pub fn new() -> Self {
        Self {
            inner: uuid::Uuid::new_v4().to_string(),
        }
    }
    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }
}
impl FromStr for SubscriptionId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // todo validate sid
        Ok(Self {
            inner: s.to_owned(),
        })
    }
}

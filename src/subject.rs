use std::str::FromStr;

/// NATS subject.
#[derive(Debug)]
pub struct Subject {
    inner: String,
}
impl Subject {
    pub fn as_str(&self) -> &str {
        &self.inner
    }
}
impl FromStr for Subject {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // todo validate subject name
        Ok(Self {
            inner: s.to_owned(),
        })
    }
}

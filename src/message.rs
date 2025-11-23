use crate::sid::SubscriptionId;
use crate::subject::Subject;
use std::fmt::Debug;

/// NATS message received from a subscription.
pub struct Message {
    pub subject: Subject,
    pub sid: SubscriptionId,
    pub reply_to: Option<Subject>,
    pub payload: Vec<u8>,
}
impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("subject", &self.subject)
            .field("sid", &self.sid)
            .field("reply_to", &self.reply_to)
            .field("payload len", &self.payload.len())
            .finish()
    }
}

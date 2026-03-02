use crate::sid::SubscriptionId;
use crate::subject::Subject;
use std::fmt::Debug;

/// NATS message received from a subscription.
pub type MessageWithSid = (SubscriptionId, Message);

/// NATS message, either published or received.
pub struct Message {
    pub subject: Subject,
    pub reply_to: Option<Subject>,
    pub payload: Vec<u8>,
}
impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("subject", &self.subject)
            .field("reply_to", &self.reply_to)
            .field("payload len", &self.payload.len())
            .finish()
    }
}

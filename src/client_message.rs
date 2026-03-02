use crate::{Message, SubscriptionId};

#[derive(Debug)]
pub enum ClientMessage {
    Pub(Message),
    Sub {
        subject: String,
        queue_group: Option<String>,
        sid: SubscriptionId,
    },
    Unsub {
        sid: SubscriptionId,
        max_msgs: Option<u32>,
    },
    Pong,
}
impl ClientMessage {
    pub fn to_nats_protocol(&self) -> Vec<u8> {
        match self {
            ClientMessage::Pub(message) => {
                let mut fields = vec![message.subject.as_str()];
                if let Some(reply_to) = &message.reply_to {
                    fields.push(reply_to.as_str());
                }
                let payload_len = message.payload.len().to_string();
                fields.push(&payload_len);
                let control_line = Self::control_line("PUB", &fields);

                let mut result = Vec::with_capacity(control_line.len() + message.payload.len() + 2);
                result.extend_from_slice(control_line.as_bytes());
                result.extend_from_slice(&message.payload);
                result.extend_from_slice(b"\r\n");
                result
            }
            ClientMessage::Sub {
                subject,
                queue_group,
                sid,
            } => {
                let mut fields = vec![subject.as_str()];
                if let Some(qg) = queue_group {
                    fields.push(qg);
                }
                fields.push(sid.as_str());
                Self::control_line("SUB", &fields).into_bytes()
            }
            ClientMessage::Unsub { sid, max_msgs } => {
                let mut fields = vec![sid.as_str()];
                let max_msgs = max_msgs.map(|m| m.to_string());
                if let Some(max_msgs) = &max_msgs {
                    fields.push(max_msgs);
                }
                Self::control_line("UNSUB", &fields).into_bytes()
            }
            ClientMessage::Pong => Self::control_line("PONG", &[]).into_bytes(),
        }
    }
    fn control_line(op_name: &str, fields: &[&str]) -> String {
        let mut control_line = String::new();
        control_line.push_str(op_name);
        for field in fields {
            control_line.push(' ');
            control_line.push_str(field);
        }
        control_line.push_str("\r\n");
        control_line
    }
}

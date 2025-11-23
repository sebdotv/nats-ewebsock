use crate::message::Message;
use crate::server_error::{ServerError, ServerResult};
use crate::sid::SubscriptionId;
use anyhow::{Context, Result, bail, ensure};

pub enum ServerMessage {
    #[cfg(feature = "server-info")]
    Info(crate::server_info::ServerInfo),
    #[cfg(not(feature = "server-info"))]
    Info(String),
    Msg(Message),
    HMsg,
    /// +OK or -ERR
    ServerResult(ServerResult),
    Ping,
    Pong,
}

type IResult<'a, T> = Result<(&'a [u8], T)>;

impl ServerMessage {
    fn control_line(input: &[u8]) -> IResult<'_, &str> {
        let newline_idx = newline_idx(input).context("missing CRLF")?;
        let line = std::str::from_utf8(&input[..newline_idx])
            .context("control line is not valid UTF-8")?;
        let input = &input[newline_idx + 2..];
        Ok((input, line))
    }

    pub(crate) fn parse(data: &[u8]) -> IResult<'_, Self> {
        let (input, line) = Self::control_line(data)?;

        let op_name_end_idx = line.find(|c: char| c.is_whitespace()).unwrap_or(line.len());
        let (op_name, remainder) = line.split_at(op_name_end_idx);
        let remainder = remainder.trim_start();
        match op_name {
            "INFO" => {
                #[cfg(feature = "server-info")]
                {
                    let server_info: crate::server_info::ServerInfo =
                        serde_json::from_str(remainder).context("failed to parse server info")?;
                    ensure!(server_info.proto == 1);
                    Ok((input, Self::Info(server_info)))
                }
                #[cfg(not(feature = "server-info"))]
                {
                    ensure!(remainder.contains(r#""proto":1"#));
                    Ok((input, Self::Info(remainder.to_owned())))
                }
            }
            "MSG" => {
                let fields = remainder.split_whitespace().collect::<Vec<&str>>();
                let (subject, sid, reply_to, payload_size) = match fields.as_slice() {
                    [subject, sid, payload_size] => (subject, sid, None, payload_size),
                    [subject, sid, reply_to, payload_size] => {
                        (subject, sid, Some(reply_to), payload_size)
                    }
                    _ => bail!("malformed MSG control line"),
                };
                let subject = subject.parse()?;
                let sid = sid.parse()?;
                let reply_to = reply_to.map(|s| s.parse()).transpose()?;
                let payload_size: usize = payload_size.parse()?;

                // extract payload
                let (payload, input) = input
                    .split_at_checked(payload_size)
                    .context("insufficient data for MSG payload")?;
                ensure!(
                    input.starts_with(b"\r\n"),
                    "expected CRLF after MSG payload"
                );
                let input = &input[2..];

                Ok((
                    input,
                    Self::Msg(Message {
                        subject,
                        sid,
                        reply_to,
                        payload: payload.to_owned(),
                    }),
                ))
            }
            "+OK" => {
                ensure!(remainder.is_empty());
                Ok((input, Self::ServerResult(Ok(()))))
            }
            "-ERR" => {
                let error_message = remainder
                    .strip_prefix("'")
                    .and_then(|s| s.strip_suffix("'"))
                    .context("malformed -ERR message")?;
                Ok((
                    input,
                    Self::ServerResult(Err(ServerError::from(error_message))),
                ))
            }
            "PING" => {
                ensure!(remainder.is_empty());
                Ok((input, Self::Ping))
            }
            other => todo!("server message {:?}", other),
        }
    }
}

#[derive(Debug)]
pub enum ClientMessage {
    Sub {
        subject: String,
        queue_group: Option<String>,
        sid: SubscriptionId,
    },
    Pong,
}
impl ClientMessage {
    pub fn to_nats_protocol(&self) -> String {
        match self {
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
                Self::control_line("SUB", &fields)
            }
            ClientMessage::Pong => Self::control_line("PONG", &[]),
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

fn newline_idx(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

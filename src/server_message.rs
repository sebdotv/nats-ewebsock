use crate::error::Error;
use crate::error::Result;
use crate::message::{Message, MessageWithSid};
use crate::server_error::{ServerError, ServerResult};

pub enum ServerMessage {
    #[cfg(feature = "server-info")]
    Info(Box<crate::server_info::ServerInfo>),
    #[cfg(not(feature = "server-info"))]
    Info(String),
    Msg(MessageWithSid),
    #[expect(unused)]
    HMsg,
    /// +OK or -ERR
    ServerResult(ServerResult),
    Ping,
    Pong,
}

type IResult<'a, T> = Result<(&'a [u8], T)>;

impl ServerMessage {
    pub fn parse(data: &[u8]) -> IResult<'_, Self> {
        let (input, line) = control_line(data)?;

        let op_name_end_idx = line.find(|c: char| c.is_whitespace()).unwrap_or(line.len());
        let (op_name, remainder) = line.split_at(op_name_end_idx);
        let remainder = remainder.trim_start();

        let check_no_remainder = || {
            remainder
                .is_empty()
                .then_some(())
                .ok_or_else(|| Error::NatsProtocol(format!("malformed {} message", op_name)))
        };

        match op_name {
            "INFO" => Self::parse_info(input, remainder),
            "MSG" => Self::parse_msg(input, remainder),
            "HMSG" => todo!("HMSG parsing not implemented"),
            "+OK" => {
                check_no_remainder()?;
                Ok((input, Self::ServerResult(Ok(()))))
            }
            "-ERR" => {
                let error_message = remainder
                    .strip_prefix("'")
                    .and_then(|s| s.strip_suffix("'"))
                    .ok_or_else(|| Error::NatsProtocol("malformed -ERR message".to_owned()))?;
                Ok((
                    input,
                    Self::ServerResult(Err(ServerError::from(error_message))),
                ))
            }
            "PING" => {
                check_no_remainder()?;
                Ok((input, Self::Ping))
            }
            "PONG" => {
                check_no_remainder()?;
                Ok((input, Self::Pong))
            }
            other => Err(Error::NatsProtocol(format!(
                "unknown server message op: {}",
                other
            ))),
        }
    }

    #[cfg(feature = "server-info")]
    fn parse_info<'a>(input: &'a [u8], remainder: &'a str) -> IResult<'a, Self> {
        let server_info: crate::server_info::ServerInfo = serde_json::from_str(remainder)
            .map_err(|e| Error::NatsProtocol(format!("failed to parse server info: {}", e)))?;
        let version = server_info.proto;
        if version != 1 {
            return Err(Error::NatsProtocol(format!(
                "unsupported NATS protocol version: {}",
                version
            )));
        }
        Ok((input, Self::Info(Box::new(server_info))))
    }
    #[cfg(not(feature = "server-info"))]
    fn parse_info<'a>(input: &'a [u8], remainder: &'a str) -> IResult<'a, Self> {
        if !remainder.contains(r#""proto":1"#) {
            return Err(Error::NatsProtocol(
                "unsupported NATS protocol version".to_owned(),
            ));
        }
        Ok((input, Self::Info(remainder.to_owned())))
    }

    fn parse_msg<'a>(input: &'a [u8], remainder: &'a str) -> IResult<'a, Self> {
        let fields = remainder.split_whitespace().collect::<Vec<&str>>();
        let (subject, sid, reply_to, payload_size) = match fields.as_slice() {
            [subject, sid, payload_size] => (subject, sid, None, payload_size),
            [subject, sid, reply_to, payload_size] => (subject, sid, Some(reply_to), payload_size),
            _ => {
                return Err(Error::NatsProtocol("malformed MSG control line".to_owned()));
            }
        };
        let subject = subject
            .parse()
            .map_err(|e| Error::NatsProtocol(format!("invalid subject in MSG: {}", e)))?;
        let sid = sid
            .parse()
            .map_err(|e| Error::NatsProtocol(format!("invalid SID in MSG: {}", e)))?;
        let reply_to = reply_to
            .map(|s| {
                s.parse()
                    .map_err(|e| Error::NatsProtocol(format!("invalid reply-to in MSG: {}", e)))
            })
            .transpose()?;
        let payload_size: usize = payload_size
            .parse()
            .map_err(|e| Error::NatsProtocol(format!("invalid payload size in MSG: {}", e)))?;

        // extract payload
        let (payload, input) = input
            .split_at_checked(payload_size)
            .ok_or_else(|| Error::NatsProtocol("insufficient data for MSG payload".to_owned()))?;
        if !input.starts_with(b"\r\n") {
            return Err(Error::NatsProtocol(
                "expected CRLF after MSG payload".to_owned(),
            ));
        }
        let input = &input[2..];

        Ok((
            input,
            Self::Msg((
                sid,
                Message {
                    subject,
                    reply_to,
                    payload: payload.to_owned(),
                },
            )),
        ))
    }
}

fn control_line(input: &[u8]) -> IResult<'_, &str> {
    let newline_idx = newline_idx(input).ok_or(Error::NatsProtocol("missing CRLF".to_owned()))?;
    let line = std::str::from_utf8(&input[..newline_idx])
        .map_err(|_e| Error::NatsProtocol("control line is not valid UTF-8".to_owned()))?;
    let input = &input[newline_idx + 2..];
    Ok((input, line))
}

fn newline_idx(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

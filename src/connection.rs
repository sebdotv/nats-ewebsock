use crate::client_message::ClientMessage;
use crate::error::{Error, Result};
use crate::message::MessageWithSid;
use crate::server_error::ServerResult;
use crate::server_message::ServerMessage;
use crate::sid::SubscriptionId;
use crate::{Message, WsOptions};
use ewebsock::{WsEvent, WsMessage};
use std::collections::VecDeque;
use tokio::sync::mpsc;

#[derive(Debug)]
enum State {
    Initial,
    WsOpen,
    #[cfg(feature = "server-info")]
    Connected(Box<crate::server_info::ServerInfo>),
    #[cfg(not(feature = "server-info"))]
    Connected(String),
    WsClosed,
}

/// Connection to a NATS server over WebSocket.
pub struct Connection {
    ws_sender: ewebsock::WsSender,
    ws_receiver: ewebsock::WsReceiver,
    state: State,
    server_result_callbacks: VecDeque<Box<dyn FnOnce(ServerResult)>>,
    msg_sender: mpsc::Sender<MessageWithSid>,
}
impl Connection {
    /// Connects to a NATS server at the given URL with default options.
    /// # Errors
    /// Returns an error if the WebSocket connection could not be established.
    pub fn connect<S: Into<String>>(url: S) -> Result<(Self, mpsc::Receiver<MessageWithSid>)> {
        let options = WsOptions::default();
        let message_buffer = 1024;
        Self::connect_options(url, options, message_buffer)
    }

    /// Connects to a NATS server at the given URL with specified options.
    /// # Errors
    /// Returns an error if the WebSocket connection could not be established.
    pub fn connect_options<S: Into<String>>(
        url: S,
        options: WsOptions,
        message_buffer: usize,
    ) -> Result<(Self, mpsc::Receiver<MessageWithSid>)> {
        let (ws_sender, ws_receiver) = ewebsock::connect(url, options).map_err(Error::WebSocket)?;
        let (msg_sender, msg_receiver) = mpsc::channel(message_buffer);
        Ok((
            Self {
                ws_sender,
                ws_receiver,
                state: State::Initial,
                server_result_callbacks: VecDeque::new(),
                msg_sender,
            },
            msg_receiver,
        ))
    }

    /// Returns true if the connection is open and server info has been received.
    pub fn is_connected(&self) -> bool {
        matches!(self.state, State::Connected(_))
    }
    fn check_connected(&self) -> Result<()> {
        self.is_connected()
            .then_some(())
            .ok_or_else(|| Error::State("not connected".to_owned()))
    }

    #[cfg(feature = "server-info")]
    /// Returns the server info if connected.
    pub fn get_server_info(&self) -> Option<&crate::server_info::ServerInfo> {
        match &self.state {
            State::Connected(info) => Some(info),
            _ => None,
        }
    }
    #[cfg(not(feature = "server-info"))]
    /// Returns the server info string if connected.
    pub fn get_server_info(&self) -> Option<&String> {
        match &self.state {
            State::Connected(info) => Some(info),
            _ => None,
        }
    }

    /// Publishes a message to the server.
    /// # Errors
    /// Returns an error if the connection is not established.
    pub fn publish<F: FnOnce(ServerResult) + 'static>(
        &mut self,
        message: Message,
        on_response: F,
    ) -> Result<()> {
        let msg = ClientMessage::Pub(message);
        self.send_with_callback(msg, on_response)
    }

    /// Subscribes to a subject pattern with an optional queue group.
    /// # Errors
    /// Returns an error if the connection is not established.
    pub fn subscribe<S: Into<String>, F: FnOnce(ServerResult) + 'static>(
        &mut self,
        subject: S,
        queue_group: Option<String>,
        on_response: F,
    ) -> Result<SubscriptionId> {
        let sid = SubscriptionId::new();
        let msg = ClientMessage::Sub {
            subject: subject.into(),
            queue_group,
            sid: sid.clone(),
        };
        self.send_with_callback(msg, on_response)?;
        Ok(sid)
    }

    /// Unsubscribes from a subscription with an optional maximum number of messages.
    /// # Errors
    /// Returns an error if the connection is not established.
    pub fn unsubscribe<F: FnOnce(ServerResult) + 'static>(
        &mut self,
        sid: SubscriptionId,
        max_msgs: Option<u32>,
        on_response: F,
    ) -> Result<()> {
        let msg = ClientMessage::Unsub { sid, max_msgs };
        self.send_with_callback(msg, on_response)
    }

    /// Sends a custom client message to the server.
    /// # Errors
    /// Returns an error if the connection is not established.
    fn send_with_callback<F: FnOnce(ServerResult) + 'static>(
        &mut self,
        msg: ClientMessage,
        on_response: F,
    ) -> Result<()> {
        self.check_connected()?;
        self.server_result_callbacks
            .push_back(Box::new(on_response));
        self.send(msg);
        Ok(())
    }

    /// Tries to receive and process incoming WebSocket events.
    /// Needs to be called periodically to handle messages and server responses.
    /// # Errors
    /// Returns an error if the connection is closed or a protocol error occurs.
    pub fn try_receive(&mut self) -> Result<()> {
        if matches!(self.state, State::WsClosed) {
            return Err(Error::State("closed".to_owned()));
        }

        while let Some(event) = self.ws_receiver.try_recv() {
            match event {
                WsEvent::Opened => {
                    if !matches!(self.state, State::Initial) {
                        return Err(Error::State(
                            "WebSocket opened event in invalid state".to_owned(),
                        ));
                    }
                    self.state = State::WsOpen;
                }
                WsEvent::Message(msg) => match msg {
                    WsMessage::Binary(data) => {
                        let mut input = data.as_slice();

                        while !input.is_empty() {
                            use ServerMessage::{HMsg, Info, Msg, Ping, Pong, ServerResult};

                            let (i, control_line) = ServerMessage::parse(input)?;
                            input = i;

                            match control_line {
                                Info(info) => {
                                    self.state = State::Connected(info);
                                }
                                Ping => {
                                    self.send(ClientMessage::Pong);
                                }
                                ServerResult(r) => {
                                    if let Some(on_result) =
                                        self.server_result_callbacks.pop_front()
                                    {
                                        on_result(r);
                                    } else {
                                        return Err(Error::NatsProtocol(
                                            "no callback for server result".to_owned(),
                                        ));
                                    }
                                }
                                Msg((sid, msg)) => {
                                    self.msg_sender.try_send((sid, msg)).map_err(|e| match e {
                                        mpsc::error::TrySendError::Full(_) => {
                                            Error::MessageChannel("message channel full".to_owned())
                                        }
                                        mpsc::error::TrySendError::Closed(_) => {
                                            Error::MessageChannel(
                                                "receiver has been closed".to_owned(),
                                            )
                                        }
                                    })?;
                                }
                                HMsg => {
                                    todo!("handle HMsg");
                                }
                                Pong => {
                                    todo!("handle Pong");
                                }
                            }
                        }
                    }
                    other => {
                        return Err(Error::NatsProtocol(format!(
                            "unexpected message type: {:?}",
                            other
                        )));
                    }
                },
                WsEvent::Error(e) => {
                    return Err(Error::WebSocket(e));
                }
                WsEvent::Closed => {
                    self.state = State::WsClosed;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    fn send(&mut self, msg: ClientMessage) {
        let bytes = msg.to_nats_protocol();
        self.ws_sender.send(WsMessage::Binary(bytes));
    }
}

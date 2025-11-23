use crate::WsOptions;
use crate::message::Message;
use crate::nats::{ClientMessage, ServerMessage};
use crate::server_error::ServerResult;
use crate::sid::SubscriptionId;
use anyhow::{Result, anyhow};
use anyhow::{bail, ensure};
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
    Connected(Box<String>),
    WsClosed,
}

/// Connection to a NATS server over WebSocket.
pub struct Connection {
    ws_sender: ewebsock::WsSender,
    ws_receiver: ewebsock::WsReceiver,
    state: State,
    server_result_callbacks: VecDeque<Box<dyn Fn(ServerResult)>>,
    msg_sender: mpsc::Sender<Message>,
}
impl Connection {
    /// Connects to a NATS server at the given URL with default options.
    pub fn connect<S: Into<String>>(url: S) -> Result<(Self, mpsc::Receiver<Message>)> {
        let options = WsOptions::default();
        let message_buffer = 1024;
        Self::connect_options(url, options, message_buffer)
    }

    /// Connects to a NATS server at the given URL with specified options.
    pub fn connect_options<S: Into<String>>(
        url: S,
        options: WsOptions,
        message_buffer: usize,
    ) -> Result<(Self, mpsc::Receiver<Message>)> {
        let (ws_sender, ws_receiver) = ewebsock::connect(url, options)
            .map_err(|e| anyhow!("WebSocket connection error: {}", e))?;
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

    /// Subscribes to a subject pattern with an optional queue group.
    pub fn subscribe<S: Into<String>, F: Fn(ServerResult) + 'static>(
        &mut self,
        subject: S,
        queue_group: Option<String>,
        on_response: F,
    ) -> Result<()> {
        ensure!(self.is_connected());
        let msg = ClientMessage::Sub {
            subject: subject.into(),
            queue_group,
            sid: SubscriptionId::new(),
        };

        self.server_result_callbacks
            .push_back(Box::new(on_response));
        self.send(msg);
        Ok(())
    }

    /// Tries to receive and process incoming WebSocket events.
    /// Needs to be called periodically to handle messages and server responses.
    pub fn try_receive(&mut self) -> Result<()> {
        ensure!(!matches!(self.state, State::WsClosed));

        while let Some(event) = self.ws_receiver.try_recv() {
            match event {
                WsEvent::Opened => {
                    ensure!(matches!(self.state, State::Initial));
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
                                    self.state = State::Connected(Box::new(info));
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
                                        bail!("No callback for server result: {:?}", r);
                                    }
                                }
                                Msg(msg) => {
                                    self.msg_sender.try_send(msg)?;
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
                    WsMessage::Text(text) => {
                        bail!("Unexpected text message: {}", text);
                    }
                    _ => {
                        bail!("Unexpected message type: {:?}", msg);
                    }
                },
                WsEvent::Error(e) => {
                    bail!("WebSocket error: {}", e);
                }
                WsEvent::Closed => {
                    self.state = State::WsClosed;
                }
            }
        }
        Ok(())
    }

    fn send(&mut self, msg: ClientMessage) {
        let protocol_msg = msg.to_nats_protocol();
        self.ws_sender
            .send(WsMessage::Binary(protocol_msg.into_bytes()));
    }
}

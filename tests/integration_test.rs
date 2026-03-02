#![expect(clippy::expect_used)]
#![expect(clippy::panic)]
#![expect(clippy::unwrap_used)]

use bytes::Bytes;
use futures_util::StreamExt;
use nats_ewebsock::{Connection, Message, ServerError, ServerResult, SubscriptionId};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

#[tokio::test]
async fn publish_single_message() {
    let nats_server = NatsServer::from_env().await;
    let ports = nats_server.ports;

    let messages_fut = subscribe_using_other_crate(ports.nats, "test", 1).await;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();

    let msg = Message {
        subject: "test".parse().unwrap(),
        reply_to: None,
        payload: b"hello".to_vec(),
    };
    h.publish_and_wait(msg).unwrap();

    let messages = messages_fut.await.unwrap();
    assert_eq!(messages.len(), 1);
    let received_msg = &messages[0];
    assert_eq!(received_msg.subject.as_str(), "test");
    assert!(received_msg.reply.is_none());
    assert_eq!(received_msg.payload.as_ref(), b"hello");
}

#[tokio::test]
async fn receive_single_message() {
    let nats_server = NatsServer::from_env().await;
    let ports = nats_server.ports;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();
    let _sid = h.subscribe_and_wait(">").unwrap();

    publish_using_other_crate(ports.nats, "test", "hello", 1).await;
    let (sid, msg) = h.receive_message();
    assert!(!sid.as_str().is_empty());
    assert_eq!(msg.subject.as_str(), "test");
    assert!(msg.reply_to.is_none());
    assert_eq!(msg.payload, b"hello");
}

#[tokio::test]
async fn receive_message_batch() {
    let nats_server = NatsServer::from_env().await;
    let ports = nats_server.ports;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();
    let _sid = h.subscribe_and_wait(">").unwrap();

    let n = 1000;
    publish_using_other_crate(ports.nats, "test", "hello", n).await;
    for _ in 0..n {
        let (sid, msg) = h.receive_message();
        assert!(!sid.as_str().is_empty());
        assert_eq!(msg.subject.as_str(), "test");
        assert!(msg.reply_to.is_none());
        assert_eq!(msg.payload, b"hello");
    }
}

#[tokio::test]
async fn multi_subscribe() {
    let nats_server = NatsServer::from_env().await;
    let ports = nats_server.ports;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();

    let sid1 = h.conn.subscribe(">", None, assert_ok).unwrap();
    let sid2 = h.conn.subscribe(">", None, assert_ok).unwrap();

    // invalid subject
    let sid3 = h
        .conn
        .subscribe(">.*", None, assert_err(ServerError::InvalidSubject))
        .unwrap();

    assert_ne!(sid1, sid2);
    assert_ne!(sid1, sid3);
}

#[tokio::test]
async fn unsubscribe() {
    let nats_server = NatsServer::from_env().await;
    let ports = nats_server.ports;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();

    let sid = h.subscribe_and_wait("test").unwrap();

    publish_using_other_crate(ports.nats, "test", "hello", 1).await;
    let (msg_sid, msg) = h.receive_message();
    assert_eq!(msg_sid, sid);
    assert_eq!(msg.subject.as_str(), "test");
    assert_eq!(msg.payload, b"hello");

    h.unsubscribe_and_wait(sid.clone(), None).unwrap();

    let msgs_fut = subscribe_using_other_crate(ports.nats, "test", 1).await;
    publish_using_other_crate(ports.nats, "test", "hello", 1).await;

    let messages = msgs_fut.await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].subject.as_str(), "test");
    assert_eq!(messages[0].payload.as_ref(), b"hello");

    // make sure no message is received on the unsubscribed connection
    h.wait_duration(Duration::from_millis(100));
    assert_eq!(
        h.msg_rx.try_recv().err(),
        Some(mpsc::error::TryRecvError::Empty)
    );

    // note: no need to test duplicate unsubscription, as the server does not error on that
}

async fn publish_using_other_crate(nats_port: u16, subject: &str, msg: &str, n: usize) {
    let nats_url = format!("nats://localhost:{}", nats_port);
    let client = async_nats::connect(nats_url).await.unwrap();
    let data = Bytes::from(msg.to_owned());
    for _ in 0..n {
        client
            .publish(subject.to_owned(), data.clone())
            .await
            .unwrap();
    }
    client.flush().await.unwrap();
}
async fn subscribe_using_other_crate(
    nats_port: u16,
    subject: &str,
    expected_n: usize,
) -> JoinHandle<Vec<async_nats::Message>> {
    let nats_url = format!("nats://localhost:{}", nats_port);
    let subject = subject.to_owned();

    let (ready_tx, ready_rx) = oneshot::channel();
    let messages_fut = tokio::spawn(async move {
        let client = async_nats::connect(nats_url).await.unwrap();
        let mut sub = client.subscribe(subject).await.unwrap();
        ready_tx.send(()).unwrap();
        let mut messages = Vec::with_capacity(expected_n);
        for _ in 0..expected_n {
            let msg = sub.next().await.unwrap();
            messages.push(msg);
        }
        messages
    });
    ready_rx.await.unwrap();
    messages_fut
}

struct NatsServerPorts {
    nats: u16,
    ws: u16,
}

enum NatsServerInstance {
    #[allow(unused)]
    Managed(Box<testcontainers::ContainerAsync<testcontainers::GenericImage>>),
    Existing,
}
struct NatsServer {
    #[allow(unused)]
    instance: NatsServerInstance,
    ports: NatsServerPorts,
}
impl NatsServer {
    /// Returns a NATS server instance and its ports.
    /// If the environment variable `NATS_PORTS` is set (as NATS then WebSocket ports, e.g. `NATS_PORTS=4222,4223`),
    /// it uses the existing server at those ports.
    /// Otherwise, it starts a managed NATS server in a Docker container.
    async fn from_env() -> Self {
        if let Ok(ports_str) = std::env::var("NATS_PORTS") {
            match ports_str
                .split(',')
                .map(str::parse::<u16>)
                .collect::<Result<Vec<_>, _>>()
                .as_ref()
                .map(Vec::as_slice)
            {
                Ok([nats_port, ws_port]) => Self {
                    instance: NatsServerInstance::Existing,
                    ports: NatsServerPorts {
                        nats: *nats_port,
                        ws: *ws_port,
                    },
                },
                _ => panic!("NATS_PORTS must be in format `nats_port,ws_port`"),
            }
        } else {
            Self::managed().await
        }
    }

    async fn managed() -> Self {
        use testcontainers::{
            GenericImage, ImageExt,
            core::{AccessMode, IntoContainerPort, Mount, WaitFor},
            runners::AsyncRunner,
        };

        const NATS_PORT: u16 = 4222;
        const WS_PORT: u16 = 4223;
        const CONF_PATH: &str = "/container/nats.conf";

        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/resources/nats.conf");
        let mount = Mount::bind_mount(path.to_str().unwrap(), CONF_PATH)
            .with_access_mode(AccessMode::ReadOnly);
        let container = GenericImage::new("nats", "2.12")
            .with_exposed_port(NATS_PORT.tcp())
            .with_exposed_port(WS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
            .with_mount(mount)
            .with_network("bridge")
            .with_cmd(vec!["-c", CONF_PATH])
            .start()
            .await
            .expect("Failed to start NATS server");
        let nats_port = container.get_host_port_ipv4(NATS_PORT).await.unwrap();
        let ws_port = container.get_host_port_ipv4(WS_PORT).await.unwrap();
        Self {
            instance: NatsServerInstance::Managed(Box::new(container)),
            ports: NatsServerPorts {
                nats: nats_port,
                ws: ws_port,
            },
        }
    }
}

struct ConnectionHelper {
    conn: Connection,
    msg_rx: mpsc::Receiver<nats_ewebsock::MessageWithSid>,
    timeout: Duration,
    sleep_duration: Duration,
}
impl ConnectionHelper {
    fn connect_port(port: u16) -> Self {
        let url = format!("ws://localhost:{}", port);
        Self::connect(url)
    }
    fn connect(url: impl Into<String>) -> Self {
        let (conn, msg_rx) = nats_ewebsock::Connection::connect(url).unwrap();
        Self {
            conn,
            msg_rx,
            timeout: Duration::from_secs(3),
            sleep_duration: Duration::from_millis(10),
        }
    }
    fn wait_until_connected(&mut self) {
        self.wait_until(|h| h.conn.is_connected());
    }
    fn wait_until_some<T, F: FnMut(&mut Self) -> Option<T>>(&mut self, mut condition: F) -> T {
        let start = Instant::now();
        loop {
            self.conn.try_receive().unwrap();
            if let Some(value) = condition(self) {
                return value;
            }
            assert!(
                start.elapsed() <= self.timeout,
                "Condition not met within timeout"
            );
            thread::sleep(self.sleep_duration);
        }
    }
    fn wait_until<F: Fn(&mut Self) -> bool>(&mut self, condition: F) {
        self.wait_until_some(|h| condition(h).then_some(()));
    }
    fn wait_duration(&mut self, duration: Duration) {
        let start = Instant::now();
        self.wait_until(|_| start.elapsed() >= duration);
    }

    fn publish_and_wait(&mut self, message: Message) -> ServerResult {
        let pub_res = Arc::new(Mutex::new(None));
        let pub_res_clone = pub_res.clone();
        self.conn
            .publish(message, move |res| {
                pub_res_clone.lock().unwrap().replace(res);
            })
            .unwrap();
        self.wait_until_some(|_| pub_res.lock().unwrap().take())
    }
    fn subscribe_and_wait(
        &mut self,
        subject: impl Into<String>,
    ) -> Result<SubscriptionId, ServerError> {
        let sub_res = Arc::new(Mutex::new(None));
        let sub_res_clone = sub_res.clone();
        let sid = self
            .conn
            .subscribe(subject, None, move |res| {
                sub_res_clone.lock().unwrap().replace(res);
            })
            .unwrap();
        let res = self.wait_until_some(|_| sub_res.lock().unwrap().take());
        res.map(|()| sid)
    }
    fn unsubscribe_and_wait(&mut self, sid: SubscriptionId, max_msgs: Option<u32>) -> ServerResult {
        let unsub_res = Arc::new(Mutex::new(None));
        let unsub_res_clone = unsub_res.clone();
        self.conn
            .unsubscribe(sid, max_msgs, move |res| {
                unsub_res_clone.lock().unwrap().replace(res);
            })
            .unwrap();
        self.wait_until_some(|_| unsub_res.lock().unwrap().take())
    }

    fn receive_message(&mut self) -> nats_ewebsock::MessageWithSid {
        self.wait_until_some(|h| h.msg_rx.try_recv().ok())
    }
}

#[expect(clippy::needless_pass_by_value)]
fn assert_ok(res: ServerResult) {
    assert!(res.is_ok());
}
fn assert_err(expected: ServerError) -> impl Fn(ServerResult) {
    move |res: ServerResult| {
        assert_eq!(res.unwrap_err(), expected);
    }
}

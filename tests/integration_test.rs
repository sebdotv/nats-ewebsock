use bytes::Bytes;
use nats_ewebsock::{Connection, ServerError, ServerResult};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use testcontainers::core::{AccessMode, Mount};
use testcontainers::{
    GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::SyncRunner,
};
use tokio::sync::mpsc;
use tokio_test::block_on;

#[test]
fn receive_single_message() {
    let nats_server = NatsServer::from_env();
    let ports = nats_server.ports;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();
    h.subscribe_and_wait(">").unwrap();

    send_messages(ports.nats, "test", "hello", 1);
    let msg = h.receive_message();
    assert_eq!(msg.subject.as_str(), "test");
    assert!(msg.reply_to.is_none());
    assert_eq!(msg.payload, b"hello");
}

#[test]
fn receive_message_batch() {
    let nats_server = NatsServer::from_env();
    let ports = nats_server.ports;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();
    h.subscribe_and_wait(">").unwrap();

    let n = 1000;
    send_messages(ports.nats, "test", "hello", n);
    for _ in 0..n {
        let msg = h.receive_message();
        assert_eq!(msg.subject.as_str(), "test");
        assert!(msg.reply_to.is_none());
        assert_eq!(msg.payload, b"hello");
    }
}

#[test]
fn multi_subscribe() {
    let nats_server = NatsServer::from_env();
    let ports = nats_server.ports;

    let mut h = ConnectionHelper::connect_port(ports.ws);
    h.wait_until_connected();

    h.conn.subscribe(">", None, assert_ok).unwrap();
    h.conn.subscribe(">", None, assert_ok).unwrap();
    h.conn
        .subscribe(">.*", None, assert_err(ServerError::InvalidSubject))
        .unwrap(); // invalid subject
}

fn send_messages(nats_port: u16, subject: &str, msg: &str, n: usize) {
    let nats_url = format!("nats://localhost:{}", nats_port);
    block_on(async {
        let client = async_nats::connect(nats_url).await?;
        let data = Bytes::from(msg.to_owned());
        for _ in 0..n {
            client.publish(subject.to_owned(), data.clone()).await?;
        }
        client.flush().await?;
        anyhow::Ok(())
    })
    .unwrap();
}

struct NatsServerPorts {
    nats: u16,
    ws: u16,
}

enum NatsServerInstance {
    #[allow(unused)]
    Managed(Box<testcontainers::Container<GenericImage>>),
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
    fn from_env() -> Self {
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
            Self::managed()
        }
    }

    fn managed() -> Self {
        use testcontainers::*;

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
            .expect("Failed to start NATS server");
        let nats_port = container.get_host_port_ipv4(NATS_PORT).unwrap();
        let ws_port = container.get_host_port_ipv4(WS_PORT).unwrap();
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
    msg_rx: mpsc::Receiver<nats_ewebsock::Message>,
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
    fn subscribe_and_wait(&mut self, subject: impl Into<String>) -> ServerResult {
        let sub_res = Arc::new(Mutex::new(None));
        let sub_res_clone = sub_res.clone();
        self.conn
            .subscribe(subject, None, move |res| {
                sub_res_clone.lock().unwrap().replace(res);
            })
            .unwrap();
        self.wait_until_some(|_| sub_res.lock().unwrap().take())
    }
    fn receive_message(&mut self) -> nats_ewebsock::Message {
        self.wait_until_some(|h| h.msg_rx.try_recv().ok())
    }
}

fn assert_ok(res: ServerResult) {
    assert!(res.is_ok());
}
fn assert_err(expected: ServerError) -> impl Fn(ServerResult) {
    move |res: ServerResult| {
        assert_eq!(res.unwrap_err(), expected);
    }
}

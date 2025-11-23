use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

#[allow(clippy::unwrap_used)]
#[allow(clippy::panic)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // connect to NATS server over WebSocket
    let (mut conn, mut msg_rx) = nats_ewebsock::Connection::connect("ws://localhost:4223")?;
    while !conn.is_connected() {
        conn.try_receive()?;
        sleep(Duration::from_millis(10));
    }

    // subscribe to all subjects
    let sub_response = Arc::new(Mutex::new(None));
    let sub_response_clone = sub_response.clone();
    conn.subscribe(">", None, move |res| {
        sub_response_clone.lock().unwrap().replace(res);
    })?;

    // wait for subscription to be acknowledged
    loop {
        conn.try_receive()?;
        if let Some(res) = sub_response.lock().unwrap().take() {
            match res {
                Ok(()) => {
                    break;
                }
                Err(e) => {
                    panic!("Subscription failed: {:?}", e);
                }
            }
        }
    }

    // receive a message
    loop {
        conn.try_receive()?;
        if let Ok(msg) = msg_rx.try_recv() {
            println!(
                "Received message on subject {:?} with len={}: {:?}",
                msg.subject.as_str(),
                msg.payload.len(),
                String::from_utf8_lossy(&msg.payload)
            );
            break;
        }
        sleep(Duration::from_millis(10));
    }

    Ok(())
}

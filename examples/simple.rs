use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

#[allow(clippy::panic)]
#[allow(clippy::unwrap_used)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // connect to NATS server over WebSocket
    let (mut conn, mut msg_rx) = nats_ewebsock::Connection::connect("ws://localhost:4223")?;
    while !conn.is_connected() {
        conn.try_receive()?;
        sleep(Duration::from_millis(10));
    }

    // publish a message
    let msg = nats_ewebsock::Message {
        subject: "test.subject".parse().unwrap(),
        reply_to: None,
        payload: b"Hello, NATS over WebSocket!".to_vec(),
    };
    let pub_response = Arc::new(Mutex::new(None));
    let pub_response_clone = pub_response.clone();
    conn.publish(msg, move |res| {
        pub_response_clone.lock().unwrap().replace(res);
    })?;

    // wait for publish to be acknowledged
    loop {
        conn.try_receive()?;
        if let Some(res) = pub_response.lock().unwrap().take() {
            match res {
                Ok(()) => {
                    break;
                }
                Err(e) => {
                    panic!("Publish failed: {:?}", e);
                }
            }
        }
    }
    println!("Message published successfully.");

    // subscribe to all subjects
    let sub_response = Arc::new(Mutex::new(None));
    let sub_response_clone = sub_response.clone();
    let sid = conn.subscribe(">", None, move |res| {
        sub_response_clone.lock().unwrap().replace(res);
    })?;
    println!("Subscribing with SID {:?}", sid);

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
    println!("Waiting to receive a message...");
    loop {
        conn.try_receive()?;
        if let Ok((sid, msg)) = msg_rx.try_recv() {
            println!(
                "Received message on subject {:?} (subscription {}) with len={}: {:?}",
                msg.subject.as_str(),
                sid.as_str(),
                msg.payload.len(),
                String::from_utf8_lossy(&msg.payload)
            );
            break;
        }
        sleep(Duration::from_millis(10));
    }

    // unsubscribe
    let unsub_response = Arc::new(Mutex::new(None));
    let unsub_response_clone = unsub_response.clone();
    conn.unsubscribe(sid, None, move |res| {
        unsub_response_clone.lock().unwrap().replace(res);
    })?;

    Ok(())
}

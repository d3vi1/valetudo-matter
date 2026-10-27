//! Drive the real background worker: short requests must wake it immediately.
use super::integration_tests::Context;
use super::*;
#[test]
fn idle_worker_wakes_for_short_request_and_immediate_cancel_restart() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (seen_tx, seen_rx) = smol::channel::bounded(2);
    let server = std::thread::spawn(move || {
        let mut held_response = None;
        for (index, body) in [
            r#"["BasicControlCapability","LocateCapability"]"#,
            r#"[{"__class":"StatusStateAttribute","value":"docked","flag":"none"}]"#,
            "{}",
            "{}",
        ]
        .into_iter()
        .enumerate()
        {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            let mut header = Vec::new();
            let mut byte = [0];
            while !header.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                header.push(byte[0]);
            }
            let header = String::from_utf8(header).unwrap();
            if header.starts_with("PUT ") {
                let size = header
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                let mut payload = vec![0; size];
                stream.read_exact(&mut payload).unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&payload).unwrap(),
                    serde_json::json!({"action":"locate"})
                );
            }
            if index == 2 {
                // Keep the first response unanswered until a replacement PUT
                // arrives. The wake must interrupt its HTTP wait, not rely on
                // an ACK, a socket close, or the normal repeat interval.
                seen_tx.send_blocking(()).unwrap();
                held_response = Some(stream);
                continue;
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            if header.starts_with("PUT ") {
                seen_tx.send_blocking(()).unwrap();
            }
        }
        drop(held_response);
        listener
    });
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    smol::block_on(executor.run(async {
        let client = crate::http::ValetudoClient::new(
            executor,
            format!("http://{address}").parse().unwrap(),
        );
        let robot = Device::init(client).await.unwrap();
        let context = Context::default();
        let mut monitor = std::pin::pin!(robot.monitor_identify(&context));
        // Put the worker into its idle wait before sending the short request.
        assert!(smol::future::poll_once(monitor.as_mut()).await.is_none());
        let exercise = async {
            for iteration in 0..2 {
                if iteration != 0 {
                    robot.identify_robot(&context, 0).await.unwrap();
                } else {
                    for _ in 0..64 {
                        robot.identify_robot(&context, 10).await.unwrap();
                    }
                }
                robot.identify_robot(&context, 1).await.unwrap();
                assert!(robot.identify_wake.1.len() <= 1); // Bounded coalescing.
                smol::future::or(
                    async {
                        seen_rx.recv().await.unwrap();
                    },
                    async {
                        smol::Timer::after(std::time::Duration::from_millis(500)).await;
                        panic!("short Identify request did not wake the worker promptly");
                    },
                )
                .await;
            }
            robot.identify_robot(&context, 0).await.unwrap();
            // Give the worker a chance to consume cancellation; no third request.
            smol::future::yield_now().await;
        };
        smol::future::or(monitor, exercise).await;
    }));
    let listener = server.join().unwrap();
    listener.set_nonblocking(true).unwrap();
    assert!(matches!(listener.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock));
}

#[test]
fn cancel_and_expiry_interrupt_stalled_http_without_a_later_send() {
    use std::io::{Read, Write};
    for expire in [false, true] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (seen_tx, seen_rx) = smol::channel::bounded(1);
        let (release, wait) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            for body in [
                r#"["BasicControlCapability","LocateCapability"]"#,
                r#"[{"__class":"StatusStateAttribute","value":"docked","flag":"none"}]"#,
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut bytes = Vec::new();
                let mut byte = [0];
                while !bytes.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                }
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut byte = [0];
            while !bytes.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                bytes.push(byte[0]);
            }
            assert!(String::from_utf8(bytes).unwrap().starts_with("PUT "));
            seen_tx.send_blocking(()).unwrap();
            wait.recv_timeout(std::time::Duration::from_secs(3))
                .unwrap();
            listener.set_nonblocking(true).unwrap();
            assert!(
                matches!(listener.accept(),Err(error) if error.kind()==std::io::ErrorKind::WouldBlock)
            );
        });
        let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
        smol::block_on(executor.run(async {
            let client = crate::http::ValetudoClient::new(
                executor,
                format!("http://{address}").parse().unwrap(),
            );
            let robot = Device::init(client).await.unwrap();
            let context = Context::default();
            robot.identify_robot(&context, 10).await.unwrap();
            if expire {
                robot.identify_deadline.set(Some(
                    std::time::Instant::now() + std::time::Duration::from_millis(50),
                ));
            }
            smol::future::or(
                async {
                    let (interrupted, ()) =
                        smol::future::zip(robot.present_identify_once(), async {
                            seen_rx.recv().await.unwrap();
                            if !expire {
                                robot.identify_robot(&context, 0).await.unwrap();
                            }
                        })
                        .await;
                    assert!(interrupted);
                },
                async {
                    smol::Timer::after(std::time::Duration::from_millis(500)).await;
                    panic!("Identify cancellation/expiry did not interrupt HTTP wait");
                },
            )
            .await;
            assert!(!robot.present_identify_once().await);
        }));
        release.send(()).unwrap();
        server.join().unwrap();
    }
}

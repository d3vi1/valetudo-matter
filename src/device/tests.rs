//! Exercise actual cluster handlers and their encoded responses without a fabric
//! or robot. Context methods outside notification fail if unexpectedly used.
use super::*;
use rs_matter::{
    dm::{
        self,
        clusters::decl::{rvc_clean_mode, rvc_operational_state, rvc_run_mode},
    },
    error::Error,
    im::EventPriority,
    tlv::{FromTLV, TLVElement, TLVTag, TLVWrite, TLVWriteParent},
    utils::storage::WriteBuf,
};

#[derive(Default)]
struct Context {
    notifications: Cell<usize>,
}
impl dm::AttrChangeNotifier for Context {
    fn notify_attr_changed(&self, _: u16, _: u32, _: u32) {
        self.notifications.set(self.notifications.get() + 1);
    }
    fn notify_cluster_changed(&self, _: u16, _: u32) {
        self.notifications.set(self.notifications.get() + 1);
    }
    fn notify_endpoint_changed(&self, _: u16) {
        self.notifications.set(self.notifications.get() + 1);
    }
    fn notify_all_changed(&self) {
        self.notifications.set(self.notifications.get() + 1);
    }
}
impl dm::OwnAttrChangeNotifier for Context {
    fn notify_own_attr_changed(&self, _: u32) {
        self.notifications.set(self.notifications.get() + 1);
    }
    fn notify_own_cluster_changed(&self) {
        self.notifications.set(self.notifications.get() + 1);
    }
    fn notify_own_endpoint_changed(&self) {
        self.notifications.set(self.notifications.get() + 1);
    }
}
impl dm::EventEmitter for Context {
    fn emit_event<F>(&self, _: u16, _: u32, _: u32, _: EventPriority, _: F) -> Result<u64, Error>
    where
        F: FnOnce(dm::events::EventTLVWrite<'_>) -> Result<(), Error>,
    {
        panic!("unexpected direct event");
    }
}
impl dm::OwnEventEmitter for Context {
    fn emit_own_event<F>(&self, _: u32, _: EventPriority, _: F) -> Result<u64, Error>
    where
        F: FnOnce(dm::events::EventTLVWrite<'_>) -> Result<(), Error>,
    {
        panic!("unexpected direct event");
    }
}
impl dm::HandlerContext for Context {
    fn matter(&self) -> &rs_matter::Matter<'_> {
        panic!("unexpected Matter access");
    }
    fn crypto(&self) -> impl rs_matter::crypto::Crypto + '_ {
        rs_matter::crypto::default_crypto(rand::thread_rng(), dm::devices::test::DAC_PRIVKEY)
    }
    fn kv(&self) -> impl rs_matter::persist::KvBlobStoreAccess + '_ {
        rs_matter::persist::DummyKvBlobStoreAccess
    }
    fn networks(&self) -> impl dm::clusters::net_comm::NetworksAccess + '_ {
        dm::clusters::net_comm::DummyNetworkAccess
    }
    fn handler(&self) -> impl dm::AsyncHandler + '_ {
        dm::EmptyHandler
    }
    fn buffers(&self) -> impl rs_matter::utils::storage::pooled::BufferAccess<dm::IMBuffer> + '_ {
        rs_matter::utils::storage::pooled::PooledBuffers::<1, dm::IMBuffer>::new(0)
    }
}
impl dm::OperationContext for Context {
    fn exchange(&self) -> &rs_matter::transport::exchange::Exchange<'_> {
        panic!("unexpected exchange");
    }
    fn endpt(&self) -> u16 {
        1
    }
    fn cluster(&self) -> u32 {
        0x54
    }
}
impl dm::InvokeContext for Context {
    fn cmd(&self) -> &dm::CmdDetails<'_> {
        panic!("unexpected command details");
    }
    fn data(&self) -> &TLVElement<'_> {
        panic!("unexpected raw data");
    }
}

impl dm::ReadContext for Context {
    fn attr(&self) -> &dm::AttrDetails<'_> {
        panic!("unexpected attribute details");
    }
}

fn mode_request(buffer: &mut [u8], mode: u8) -> usize {
    let mut writer = WriteBuf::new(buffer);
    writer.start_struct(&TLVTag::Anonymous).unwrap();
    writer.u8(&TLVTag::Context(0), mode).unwrap();
    writer.end_container().unwrap();
    writer.as_slice().len()
}

#[test]
fn unsupported_modes_return_cluster_status_and_empty_text_without_http() {
    let device = super::cycle_tests::device();
    let mut request_buffer = [0; 32];
    let length = mode_request(&mut request_buffer, 2); // Mapping absent.
    let request = TLVElement::new(&request_buffer[..length]);
    let mut response_buffer = [0; 128];
    let mut writer = WriteBuf::new(&mut response_buffer);
    let response = rvc_run_mode::ChangeToModeResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    smol::block_on(rvc_run_mode::ClusterAsyncHandler::handle_change_to_mode(
        &device,
        Context::default(),
        rvc_run_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
        response,
    ))
    .unwrap();
    let fields = TLVElement::new(writer.as_slice()).structure().unwrap();
    assert_eq!(fields.find_ctx(0).unwrap().u8().unwrap(), 1);
    assert_eq!(fields.find_ctx(1).unwrap().utf8().unwrap(), "");

    let mut writer = WriteBuf::new(&mut response_buffer);
    let response = rvc_clean_mode::ChangeToModeResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    smol::block_on(rvc_clean_mode::ClusterAsyncHandler::handle_change_to_mode(
        &device,
        Context::default(),
        rvc_clean_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
        response,
    ))
    .unwrap();
    let fields = TLVElement::new(writer.as_slice()).structure().unwrap();
    assert_eq!(fields.find_ctx(0).unwrap().u8().unwrap(), 1);
    assert_eq!(fields.find_ctx(1).unwrap().utf8().unwrap(), "");
}

#[test]
fn actual_resume_handler_rejects_docked_without_an_operation() {
    let device = super::cycle_tests::device();
    device.current_state.set(DeviceState {
        value: StatusValue::Docked,
        flag: StatusFlag::None,
    });
    let mut buffer = [0; 128];
    let mut writer = WriteBuf::new(&mut buffer);
    let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    smol::block_on(rvc_operational_state::ClusterAsyncHandler::handle_resume(
        &device,
        Context::default(),
        response,
    ))
    .unwrap();
    let fields = TLVElement::new(writer.as_slice()).structure().unwrap();
    assert_eq!(
        fields
            .find_ctx(0)
            .unwrap()
            .structure()
            .unwrap()
            .find_ctx(0)
            .unwrap()
            .u8()
            .unwrap(),
        3
    );
    assert_eq!(device.current_state.get().value, StatusValue::Docked);
}

#[test]
fn actual_run_handler_returns_generic_failure_on_unknown_state() {
    let device = super::cycle_tests::device();
    device.current_state.set(DeviceState {
        value: StatusValue::Unknown,
        flag: StatusFlag::Unknown,
    });
    let mut request_buffer = [0; 32];
    let length = mode_request(&mut request_buffer, 1);
    let request = TLVElement::new(&request_buffer[..length]);
    let mut response_buffer = [0; 128];
    let mut writer = WriteBuf::new(&mut response_buffer);
    let response = rvc_run_mode::ChangeToModeResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    smol::block_on(rvc_run_mode::ClusterAsyncHandler::handle_change_to_mode(
        &device,
        Context::default(),
        rvc_run_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
        response,
    ))
    .unwrap();
    let fields = TLVElement::new(writer.as_slice()).structure().unwrap();
    assert_eq!(fields.find_ctx(0).unwrap().u8().unwrap(), 2);
    assert!(!fields.find_ctx(1).unwrap().utf8().unwrap().is_empty());
}

#[test]
fn concurrent_fabrics_cannot_issue_home_resume_or_second_start_before_start_ack() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (received_tx, received_rx) = smol::channel::bounded(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(time::Duration::from_secs(5)))
            .unwrap();
        let mut headers = Vec::new();
        let mut byte = [0];
        while !headers.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            headers.push(byte[0]);
        }
        let headers = String::from_utf8(headers).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut body = vec![0; length];
        stream.read_exact(&mut body).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"action":"start"})
        );
        received_tx.send_blocking(()).unwrap();
        release_rx
            .recv_timeout(time::Duration::from_secs(5))
            .unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .unwrap();
    });
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    let mut device = super::cycle_tests::device();
    device.client = ValetudoClient::new(executor, format!("http://{address}").parse().unwrap());
    let ctx = Context::default();
    smol::block_on(executor.run(async {
        smol::future::zip(
            async {
                let mut input = [0; 32];
                let length = mode_request(&mut input, 1);
                let request = TLVElement::new(&input[..length]);
                let mut output = [0; 128];
                let mut writer = WriteBuf::new(&mut output);
                let response = rvc_run_mode::ChangeToModeResponseBuilder::new(
                    TLVWriteParent::new((), &mut writer),
                    &TLVTag::Anonymous,
                )
                .unwrap();
                rvc_run_mode::ClusterAsyncHandler::handle_change_to_mode(
                    &device,
                    &ctx,
                    rvc_run_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
                    response,
                )
                .await
                .unwrap();
                assert_eq!(
                    TLVElement::new(writer.as_slice())
                        .structure()
                        .unwrap()
                        .find_ctx(0)
                        .unwrap()
                        .u8()
                        .unwrap(),
                    0
                );
            },
            async {
                received_rx.recv().await.unwrap();
                let mut output = [0; 128];
                let mut writer = WriteBuf::new(&mut output);
                let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
                    TLVWriteParent::new((), &mut writer),
                    &TLVTag::Anonymous,
                )
                .unwrap();
                rvc_operational_state::ClusterAsyncHandler::handle_go_home(&device, &ctx, response)
                    .await
                    .unwrap();
                assert_eq!(
                    TLVElement::new(writer.as_slice())
                        .structure()
                        .unwrap()
                        .find_ctx(0)
                        .unwrap()
                        .structure()
                        .unwrap()
                        .find_ctx(0)
                        .unwrap()
                        .u8()
                        .unwrap(),
                    3
                );
                let mut writer = WriteBuf::new(&mut output);
                let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
                    TLVWriteParent::new((), &mut writer),
                    &TLVTag::Anonymous,
                )
                .unwrap();
                rvc_operational_state::ClusterAsyncHandler::handle_resume(&device, &ctx, response)
                    .await
                    .unwrap();
                assert_eq!(
                    TLVElement::new(writer.as_slice())
                        .structure()
                        .unwrap()
                        .find_ctx(0)
                        .unwrap()
                        .structure()
                        .unwrap()
                        .find_ctx(0)
                        .unwrap()
                        .u8()
                        .unwrap(),
                    1
                );
                let mut input = [0; 32];
                let length = mode_request(&mut input, 1);
                let request = TLVElement::new(&input[..length]);
                let mut writer = WriteBuf::new(&mut output);
                let response = rvc_run_mode::ChangeToModeResponseBuilder::new(
                    TLVWriteParent::new((), &mut writer),
                    &TLVTag::Anonymous,
                )
                .unwrap();
                rvc_run_mode::ClusterAsyncHandler::handle_change_to_mode(
                    &device,
                    &ctx,
                    rvc_run_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
                    response,
                )
                .await
                .unwrap();
                assert_eq!(
                    TLVElement::new(writer.as_slice())
                        .structure()
                        .unwrap()
                        .find_ctx(0)
                        .unwrap()
                        .u8()
                        .unwrap(),
                    2
                );
                assert_eq!(ctx.notifications.get(), 0);
                release_tx.send(()).unwrap();
            },
        )
        .await;
    }));
    assert_eq!(device.cycle_mode.get(), CycleMode::Cleaning);
    assert_eq!(ctx.notifications.get(), 1);
    assert!(device.try_command().is_some());
    server.join().unwrap();
}

#[test]
fn unknown_restart_cycle_rejects_new_start_and_resume_without_dispatch() {
    let device = super::cycle_tests::device();
    device.current_state.set(DeviceState {
        value: StatusValue::Paused,
        flag: StatusFlag::Resumable,
    });
    device.cycle_known.set(false);
    assert!(device.cycle_active()); // Selection/start guards must treat it as busy.
    let mut input = [0; 32];
    let length = mode_request(&mut input, 1);
    let request = TLVElement::new(&input[..length]);
    let mut output = [0; 128];
    let mut writer = WriteBuf::new(&mut output);
    let response = rvc_run_mode::ChangeToModeResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    smol::block_on(rvc_run_mode::ClusterAsyncHandler::handle_change_to_mode(
        &device,
        Context::default(),
        rvc_run_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
        response,
    ))
    .unwrap();
    assert_eq!(
        TLVElement::new(writer.as_slice())
            .structure()
            .unwrap()
            .find_ctx(0)
            .unwrap()
            .u8()
            .unwrap(),
        3
    );
    let mut writer = WriteBuf::new(&mut output);
    let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    smol::block_on(rvc_operational_state::ClusterAsyncHandler::handle_resume(
        &device,
        Context::default(),
        response,
    ))
    .unwrap();
    assert_eq!(
        TLVElement::new(writer.as_slice())
            .structure()
            .unwrap()
            .find_ctx(0)
            .unwrap()
            .structure()
            .unwrap()
            .find_ctx(0)
            .unwrap()
            .u8()
            .unwrap(),
        1
    );
}

#[test]
fn api_activity_observed_before_http_ack_is_not_lost_on_ack() {
    let device = super::cycle_tests::device();
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Cleaning,
            flag: StatusFlag::Segment,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(device.cycle_observed_active.get());
    device.set_cycle_mode(CycleMode::Cleaning, &Context::default());
    assert!(device.cycle_observed_active.get());
    assert!(device.start_pending_since.get().is_none());
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Docked,
            flag: StatusFlag::None,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
    assert_eq!(
        *device.pending_events.borrow(),
        [OperationalEvent::Completed(0)]
    );
}

#[test]
fn restarted_ambiguous_paused_state_does_not_load_history_or_emit_historical_completion() {
    use std::io::{Read, Write};
    let directory = std::env::temp_dir().join(format!(
        "valetudo-no-history-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir(&directory).unwrap();
    let history = br#"{"version":1,"mode":"Mapping","observed_active":true,"state":{"value":"paused","flag":"resumable"},"paused_activity":{"value":"returning","flag":"none"}}"#;
    std::fs::write(directory.join("rvc-lifecycle.json"), history).unwrap();
    std::fs::write(directory.join("fabric.bin"), b"untouched").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for (path, body) in [
            (
                "/api/v2/robot/capabilities",
                r#"["BasicControlCapability"]"#,
            ),
            (
                "/api/v2/robot/state/attributes",
                r#"[{"__class":"StatusStateAttribute","value":"paused","flag":"resumable"}]"#,
            ),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(time::Duration::from_secs(5)))
                .unwrap();
            let mut headers = Vec::new();
            let mut byte = [0];
            while !headers.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                headers.push(byte[0]);
            }
            let headers = String::from_utf8(headers).unwrap();
            let first: Vec<_> = headers.lines().next().unwrap().split_whitespace().collect();
            assert_eq!(first[0], "GET");
            assert_eq!(first[1].parse::<hyper::Uri>().unwrap().path(), path);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        }
        listener.set_nonblocking(true).unwrap();
        listener
    });
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    let device = smol::block_on(executor.run(Device::init(ValetudoClient::new(
        executor,
        format!("http://{address}").parse().unwrap(),
    ))))
    .unwrap();
    let listener = server.join().unwrap();
    // The actual operation could be paused cleaning or paused docking. The
    // identical public flags do not prove either; disk history is never loaded.
    assert!(!device.cycle_known.get());
    assert!(!device.cycle_observed_active.get());
    assert!(!device.resume_context_trusted.get());
    assert_eq!(device.paused_activity.get(), None);
    assert!(
        smol::block_on(rvc_run_mode::ClusterAsyncHandler::current_mode(
            &device,
            Context::default()
        ))
        .is_err()
    );
    let mut output = [0; 128];
    let mut writer = WriteBuf::new(&mut output);
    let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    smol::block_on(rvc_operational_state::ClusterAsyncHandler::handle_resume(
        &device,
        Context::default(),
        response,
    ))
    .unwrap();
    assert_eq!(
        TLVElement::new(writer.as_slice())
            .structure()
            .unwrap()
            .find_ctx(0)
            .unwrap()
            .structure()
            .unwrap()
            .find_ctx(0)
            .unwrap()
            .u8()
            .unwrap(),
        1
    );
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Idle,
            flag: StatusFlag::None,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(device.cycle_known.get());
    assert!(device.pending_events.borrow().is_empty());
    assert_eq!(
        std::fs::read(directory.join("rvc-lifecycle.json")).unwrap(),
        history
    );
    assert_eq!(
        std::fs::read(directory.join("fabric.bin")).unwrap(),
        b"untouched"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn disconnected_status_revokes_resume_trust_until_new_active_observation() {
    let device = super::cycle_tests::device();
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Cleaning,
            flag: StatusFlag::Segment,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(device.resume_context_trusted.get());
    device.apply_snapshot(&[], &(), 0x54, 0x55, 0x61);
    assert!(!device.resume_context_trusted.get());
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Paused,
            flag: StatusFlag::Resumable,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(!device.resume_context_trusted.get());
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Returning,
            flag: StatusFlag::None,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Paused,
            flag: StatusFlag::Resumable,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(device.resume_context_trusted.get());
    assert_eq!(
        device.paused_activity.get().unwrap().value,
        StatusValue::Returning
    );
}

#[test]
fn late_start_ack_does_not_resurrect_an_already_observed_completed_cycle() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (received_tx, received_rx) = smol::channel::bounded(1);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(time::Duration::from_secs(5)))
            .unwrap();
        let mut headers = Vec::new();
        let mut byte = [0];
        while !headers.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            headers.push(byte[0]);
        }
        let headers = String::from_utf8(headers).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut body = vec![0; length];
        stream.read_exact(&mut body).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"action":"start"})
        );
        received_tx.send_blocking(()).unwrap();
        release_rx
            .recv_timeout(time::Duration::from_secs(5))
            .unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .unwrap();
    });
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    let mut device = super::cycle_tests::device();
    device.client = ValetudoClient::new(executor, format!("http://{address}").parse().unwrap());
    smol::block_on(executor.run(async {
        smol::future::zip(
            async {
                let mut input = [0; 32];
                let length = mode_request(&mut input, 1);
                let request = TLVElement::new(&input[..length]);
                let mut output = [0; 128];
                let mut writer = WriteBuf::new(&mut output);
                let response = rvc_run_mode::ChangeToModeResponseBuilder::new(
                    TLVWriteParent::new((), &mut writer),
                    &TLVTag::Anonymous,
                )
                .unwrap();
                rvc_run_mode::ClusterAsyncHandler::handle_change_to_mode(
                    &device,
                    Context::default(),
                    rvc_run_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
                    response,
                )
                .await
                .unwrap();
                assert_eq!(
                    TLVElement::new(writer.as_slice())
                        .structure()
                        .unwrap()
                        .find_ctx(0)
                        .unwrap()
                        .u8()
                        .unwrap(),
                    0
                );
            },
            async {
                received_rx.recv().await.unwrap();
                device.apply_attributes(
                    &[StateAttribute::StatusStateAttribute {
                        value: StatusValue::Cleaning,
                        flag: StatusFlag::Segment,
                        error: None,
                    }],
                    &(),
                    0x54,
                    0x55,
                    0x61,
                );
                device.apply_attributes(
                    &[StateAttribute::StatusStateAttribute {
                        value: StatusValue::Idle,
                        flag: StatusFlag::None,
                        error: None,
                    }],
                    &(),
                    0x54,
                    0x55,
                    0x61,
                );
                assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
                assert_eq!(
                    *device.pending_events.borrow(),
                    [OperationalEvent::Completed(0)]
                );
                release_tx.send(()).unwrap();
            },
        )
        .await;
        assert_eq!(
            rvc_run_mode::ClusterAsyncHandler::current_mode(&device, Context::default())
                .await
                .unwrap(),
            0
        );
    }));
    assert!(device.start_pending_since.get().is_none());
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Idle,
            flag: StatusFlag::None,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
    assert_eq!(
        *device.pending_events.borrow(),
        [OperationalEvent::Completed(0)]
    );
    assert_eq!(device.completion_generation.get(), 1);
    server.join().unwrap();
}

#[test]
fn observation_gap_prevents_stale_ack_and_historical_completion_after_reconnect() {
    let device = super::cycle_tests::device();
    let receipt = device.start_receipt();
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Cleaning,
            flag: StatusFlag::Segment,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    device.apply_snapshot(&[], &(), 0x54, 0x55, 0x61);
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Paused,
            flag: StatusFlag::Resumable,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(!device.cycle_known.get());
    assert!(!device.cycle_observed_active.get());
    assert_eq!(device.paused_activity.get(), None);
    assert!(
        smol::block_on(rvc_run_mode::ClusterAsyncHandler::current_mode(
            &device,
            Context::default()
        ))
        .is_err()
    );
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Idle,
            flag: StatusFlag::None,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(!device.acknowledge_start(CycleMode::Cleaning, receipt, &Context::default()));
    assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
    assert!(device.pending_events.borrow().is_empty());
}

struct Reply {
    method: &'static str,
    path: &'static str,
    status: u16,
    body: &'static str,
    expected: Option<serde_json::Value>,
}

fn serve(replies: Vec<Reply>) -> (String, std::thread::JoinHandle<()>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let thread = std::thread::spawn(move || {
        for reply in replies {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(time::Duration::from_secs(5)))
                .unwrap();
            let mut headers = Vec::new();
            let mut byte = [0];
            while !headers.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                headers.push(byte[0]);
            }
            let headers = String::from_utf8(headers).unwrap();
            let first: Vec<_> = headers.lines().next().unwrap().split_whitespace().collect();
            assert_eq!(first[0], reply.method);
            assert_eq!(first[1].parse::<hyper::Uri>().unwrap().path(), reply.path);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            let mut body = vec![0; length];
            stream.read_exact(&mut body).unwrap();
            if let Some(expected) = reply.expected {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                    expected
                );
            }
            write!(
                stream,
                "HTTP/1.1 {} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                reply.status,
                reply.body.len(),
                reply.body
            )
            .unwrap();
        }
    });
    (format!("http://{address}"), thread)
}

#[test]
fn optional_capability_failures_do_not_abort_basic_rvc_initialization() {
    let (url, server) = serve(vec![
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities",
            status: 200,
            body: r#"["BasicControlCapability","OperationModeControlCapability","MapSegmentationCapability","FutureCapability"]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities/OperationModeControlCapability/presets",
            status: 503,
            body: "{}",
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/state/attributes",
            status: 200,
            body: r#"[{"__class":"StatusStateAttribute","value":"idle","flag":"none"},{"__class":"FutureAttribute","value":{"new":1}},{"__class":"PresetSelectionStateAttribute","type":"operation_mode","value":"future_preset"}]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities/MapSegmentationCapability",
            status: 503,
            body: "{}",
            expected: None,
        },
    ]);
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    let device = smol::block_on(executor.run(Device::init(ValetudoClient::new(
        executor,
        url.parse().unwrap(),
    ))))
    .unwrap();
    assert!(!device.capabilities.contains(Capability::Unknown));
    assert!(!device.supports_clean_modes());
    assert_eq!(device.current_preset.get(), None);
    assert!(device.segments.is_empty());
    assert!(device.ensure_known_status().is_ok());
    server.join().unwrap();
}

#[test]
fn missing_required_basic_control_is_not_faked() {
    let (url, server) = serve(vec![Reply {
        method: "GET",
        path: "/api/v2/robot/capabilities",
        status: 200,
        body: r#"["OperationModeControlCapability"]"#,
        expected: None,
    }]);
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    let result = smol::block_on(executor.run(Device::init(ValetudoClient::new(
        executor,
        url.parse().unwrap(),
    ))));
    assert!(matches!(result, Err(error) if error.to_string().contains("BasicControlCapability")));
    server.join().unwrap();
}

#[test]
fn real_operation_mode_change_notifies_and_selected_room_resume_uses_basic_control() {
    let (url, server) = serve(vec![
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities",
            status: 200,
            body: r#"["BasicControlCapability","OperationModeControlCapability","MapSegmentationCapability"]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities/OperationModeControlCapability/presets",
            status: 200,
            body: r#"["vacuum","mop","future_mode"]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/state/attributes",
            status: 200,
            body: r#"[{"__class":"StatusStateAttribute","value":"idle","flag":"none"},{"__class":"PresetSelectionStateAttribute","type":"operation_mode","value":"vacuum"}]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities/MapSegmentationCapability",
            status: 200,
            body: r#"[{"id":"16","name":"Room"}]"#,
            expected: None,
        },
        Reply {
            method: "PUT",
            path: "/api/v2/robot/capabilities/OperationModeControlCapability/preset",
            status: 200,
            body: "{}",
            expected: Some(serde_json::json!({"name":"mop"})),
        },
        Reply {
            method: "PUT",
            path: "/api/v2/robot/capabilities/BasicControlCapability",
            status: 200,
            body: "{}",
            expected: Some(serde_json::json!({"action":"start"})),
        },
    ]);
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    smol::block_on(executor.run(async {
        let device = Device::init(ValetudoClient::new(executor, url.parse().unwrap()))
            .await
            .unwrap();
        assert!(device.supports_clean_modes());
        assert_eq!(device.cleaning_presets.len(), 2);
        let ctx = Context::default();
        let mut input = [0; 32];
        let length = mode_request(&mut input, 1);
        let request = TLVElement::new(&input[..length]);
        let mut output = [0; 128];
        let mut writer = WriteBuf::new(&mut output);
        let response = rvc_clean_mode::ChangeToModeResponseBuilder::new(
            TLVWriteParent::new((), &mut writer),
            &TLVTag::Anonymous,
        )
        .unwrap();
        rvc_clean_mode::ClusterAsyncHandler::handle_change_to_mode(
            &device,
            &ctx,
            rvc_clean_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
            response,
        )
        .await
        .unwrap();
        assert_eq!(
            TLVElement::new(writer.as_slice())
                .structure()
                .unwrap()
                .find_ctx(0)
                .unwrap()
                .u8()
                .unwrap(),
            0
        );
        assert_eq!(device.current_preset.get(), Some(Preset::Mop));
        assert_eq!(ctx.notifications.get(), 1); // Same-value SSE may follow later.
        device.selected_areas.borrow_mut().push(16);
        device.apply_attributes(
            &[StateAttribute::StatusStateAttribute {
                value: StatusValue::Cleaning,
                flag: StatusFlag::Segment,
                error: None,
            }],
            &(),
            0x54,
            0x55,
            0x61,
        );
        device.apply_attributes(
            &[StateAttribute::StatusStateAttribute {
                value: StatusValue::Paused,
                flag: StatusFlag::Resumable,
                error: None,
            }],
            &(),
            0x54,
            0x55,
            0x61,
        );
        let mut writer = WriteBuf::new(&mut output);
        let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
            TLVWriteParent::new((), &mut writer),
            &TLVTag::Anonymous,
        )
        .unwrap();
        rvc_operational_state::ClusterAsyncHandler::handle_resume(&device, &ctx, response)
            .await
            .unwrap();
        assert_eq!(
            TLVElement::new(writer.as_slice())
                .structure()
                .unwrap()
                .find_ctx(0)
                .unwrap()
                .structure()
                .unwrap()
                .find_ctx(0)
                .unwrap()
                .u8()
                .unwrap(),
            0
        );
        assert_eq!(device.current_state.get().value, StatusValue::Cleaning);
        assert_eq!(*device.selected_areas.borrow(), [16]);
    }));
    server.join().unwrap();
}

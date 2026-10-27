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
    cluster_paths: RefCell<Vec<(u16,u32)>>,
}
impl dm::AttrChangeNotifier for Context {
    fn notify_attr_changed(&self, _: u16, _: u32, _: u32) {
        self.notifications.set(self.notifications.get() + 1);
    }
    fn notify_cluster_changed(&self, endpoint: u16, cluster: u32) {
        self.cluster_paths.borrow_mut().push((endpoint,cluster));
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
    assert!(device.segments.borrow().is_empty());
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
            body: r#"["BasicControlCapability","OperationModeControlCapability","FanSpeedControlCapability","MapSegmentationCapability"]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities/OperationModeControlCapability/presets",
            status: 200,
            body: r#"["vacuum","mop","low","future_mode"]"#,
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

async fn change_clean_mode(device: &Device, mode: u8, ctx: &Context) -> u8 {
    let mut input = [0; 32];
    let length = mode_request(&mut input, mode);
    let request = TLVElement::new(&input[..length]);
    let mut output = [0; 128];
    let mut writer = WriteBuf::new(&mut output);
    let response = rvc_clean_mode::ChangeToModeResponseBuilder::new(
        TLVWriteParent::new((), &mut writer),
        &TLVTag::Anonymous,
    )
    .unwrap();
    rvc_clean_mode::ClusterAsyncHandler::handle_change_to_mode(
        device,
        ctx,
        rvc_clean_mode::ChangeToModeRequest::from_tlv(&request).unwrap(),
        response,
    )
    .await
    .unwrap();
    TLVElement::new(writer.as_slice())
        .structure()
        .unwrap()
        .find_ctx(0)
        .unwrap()
        .u8()
        .unwrap()
}

#[test]
fn fan_modes_initialize_dispatch_and_handle_unknown_state() {
    let (url, server) = serve(vec![
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities",
            status: 200,
            body: r#"["BasicControlCapability","FanSpeedControlCapability"]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities/FanSpeedControlCapability/presets",
            status: 200,
            body: r#"["low","medium","high","max","off","vacuum","future","low"]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/state/attributes",
            status: 200,
            body: r#"[{"__class":"StatusStateAttribute","value":"idle","flag":"none"},{"__class":"PresetSelectionStateAttribute","type":"fan_speed","value":"medium"}]"#,
            expected: None,
        },
        Reply {
            method: "PUT",
            path: "/api/v2/robot/capabilities/FanSpeedControlCapability/preset",
            status: 200,
            body: "{}",
            expected: Some(serde_json::json!({"name":"high"})),
        },
        Reply {
            method: "PUT",
            path: "/api/v2/robot/capabilities/FanSpeedControlCapability/preset",
            status: 503,
            body: "{}",
            expected: Some(serde_json::json!({"name":"max"})),
        },
    ]);
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    smol::block_on(executor.run(async {
        let device = Device::init(ValetudoClient::new(executor, url.parse().unwrap()))
            .await
            .unwrap();
        assert!(device.fan_speed_modes);
        assert!(device.supports_clean_modes());
        assert_eq!(device.cleaning_presets.len(), 5);
        assert_eq!(device.current_preset.get(), Some(Preset::Medium));
        assert_eq!(
            rvc_clean_mode::ClusterAsyncHandler::current_mode(&device, Context::default())
                .await
                .unwrap(),
            5
        );
        let mut buffer = [0; 1024];
        let mut writer = WriteBuf::new(&mut buffer);
        let array = dm::clusters::decl::globals::ModeOptionStructArrayBuilder::new(
            TLVWriteParent::new((), &mut writer),
            &TLVTag::Anonymous,
        )
        .unwrap();
        rvc_clean_mode::ClusterAsyncHandler::supported_modes(
            &device,
            Context::default(),
            dm::ArrayAttributeRead::ReadAll(array),
        )
        .await
        .unwrap();
        let entries = TLVElement::new(writer.as_slice())
            .array()
            .unwrap()
            .iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 5);
        for (entry, id) in entries.iter().zip(4..=8) {
            assert_eq!(
                entry
                    .structure()
                    .unwrap()
                    .find_ctx(1)
                    .unwrap()
                    .u8()
                    .unwrap(),
                id
            );
        }
        let ctx = Context::default();
        assert_eq!(change_clean_mode(&device, 0, &ctx).await, 1); // recognized but unavailable
        assert_eq!(change_clean_mode(&device, 255, &ctx).await, 1);
        assert_eq!(ctx.notifications.get(), 0);
        assert_eq!(change_clean_mode(&device, 6, &ctx).await, 0);
        assert_eq!(device.current_preset.get(), Some(Preset::High));
        assert_eq!(ctx.notifications.get(), 1);
        assert_eq!(change_clean_mode(&device, 7, &ctx).await, 2);
        assert_eq!(device.current_preset.get(), Some(Preset::High));
        device.apply_attributes(
            &[StateAttribute::PresetSelectionStateAttribute {
                r#type: "operation_mode".into(),
                value: "vacuum".into(),
            }],
            &ctx,
            0x54,
            0x55,
            0x61,
        );
        assert_eq!(device.current_preset.get(), Some(Preset::High));
        device.apply_attributes(
            &[StateAttribute::PresetSelectionStateAttribute {
                r#type: "fan_speed".into(),
                value: "future".into(),
            }],
            &ctx,
            0x54,
            0x55,
            0x61,
        );
        assert_eq!(device.current_preset.get(), None);
        assert!(
            rvc_clean_mode::ClusterAsyncHandler::current_mode(&device, Context::default())
                .await
                .is_err()
        );
        device.apply_attributes(
            &[StateAttribute::PresetSelectionStateAttribute {
                r#type: "fan_speed".into(),
                value: "low".into(),
            }],
            &ctx,
            0x54,
            0x55,
            0x61,
        );
        assert_eq!(device.current_preset.get(), Some(Preset::Low));
        device.apply_snapshot(
            &[StateAttribute::StatusStateAttribute {
                value: StatusValue::Idle,
                flag: StatusFlag::None,
                error: None,
            }],
            &ctx,
            0x54,
            0x55,
            0x61,
        );
        assert_eq!(device.current_preset.get(), None);
    }));
    server.join().unwrap();
}

#[test]
fn fan_modes_require_two_recognized_options_and_optional_api_failures_are_safe() {
    for (status, presets, expected_len, enabled) in [
        (200, r#"["low","future"]"#, 1, false),
        (200, r#"["off","future","off"]"#, 1, false),
        (200, r#"["low","off"]"#, 2, true),
        (200, r#"["vacuum","mop","future"]"#, 0, false),
        (200, r#"{"wrong":"shape"}"#, 0, false),
        (503, "{}", 0, false),
    ] {
        let (url, server) = serve(vec![
            Reply {
                method: "GET",
                path: "/api/v2/robot/capabilities",
                status: 200,
                body: r#"["BasicControlCapability","FanSpeedControlCapability"]"#,
                expected: None,
            },
            Reply {
                method: "GET",
                path: "/api/v2/robot/capabilities/FanSpeedControlCapability/presets",
                status,
                body: presets,
                expected: None,
            },
            Reply {
                method: "GET",
                path: "/api/v2/robot/state/attributes",
                status: 200,
                body: r#"[{"__class":"StatusStateAttribute","value":"idle","flag":"none"},{"__class":"PresetSelectionStateAttribute","type":"fan_speed","value":"future"}]"#,
                expected: None,
            },
        ]);
        let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
        let device = smol::block_on(executor.run(Device::init(ValetudoClient::new(
            executor,
            url.parse().unwrap(),
        ))))
        .unwrap();
        assert_eq!(device.cleaning_presets.len(), expected_len);
        assert_eq!(device.supports_clean_modes(), enabled);
        assert_eq!(device.current_preset.get(), None);
        server.join().unwrap();
    }
}

#[test]
fn mapping_capability_advertises_mode_and_uses_mapping_api_without_unsafe_resume() {
    use rs_matter::dm::ArrayAttributeRead;
    let (url, server) = serve(vec![
        Reply {
            method: "GET",
            path: "/api/v2/robot/capabilities",
            status: 200,
            body: r#"["BasicControlCapability","MappingPassCapability"]"#,
            expected: None,
        },
        Reply {
            method: "GET",
            path: "/api/v2/robot/state/attributes",
            status: 200,
            body: r#"[{"__class":"StatusStateAttribute","value":"idle","flag":"none"}]"#,
            expected: None,
        },
        Reply {
            method: "PUT",
            path: "/api/v2/robot/capabilities/MappingPassCapability",
            status: 200,
            body: "{}",
            expected: Some(serde_json::json!({"action":"start_mapping"})),
        },
        Reply {
            method: "PUT",
            path: "/api/v2/robot/capabilities/BasicControlCapability",
            status: 200,
            body: "{}",
            expected: Some(serde_json::json!({"action":"stop"})),
        },
    ]);
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    smol::block_on(executor.run(async {
        let device = Device::init(ValetudoClient::new(executor, url.parse().unwrap()))
            .await
            .unwrap();
        let mut buffer = [0; 512];
        let mut writer = WriteBuf::new(&mut buffer);
        let builder = rs_matter::dm::clusters::decl::globals::ModeOptionStructArrayBuilder::new(
            TLVWriteParent::new((), &mut writer),
            &TLVTag::Anonymous,
        )
        .unwrap();
        rvc_run_mode::ClusterAsyncHandler::supported_modes(
            &device,
            Context::default(),
            ArrayAttributeRead::ReadAll(builder),
        )
        .await
        .unwrap();
        let entries = TLVElement::new(writer.as_slice())
            .array()
            .unwrap()
            .iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[0]
                .structure()
                .unwrap()
                .find_ctx(1)
                .unwrap()
                .u8()
                .unwrap(),
            0
        );
        assert_eq!(
            entries[1]
                .structure()
                .unwrap()
                .find_ctx(1)
                .unwrap()
                .u8()
                .unwrap(),
            1
        );
        let mapping = entries[2].structure().unwrap();
        assert_eq!(mapping.find_ctx(1).unwrap().u8().unwrap(), 2);
        assert_eq!(mapping.find_ctx(0).unwrap().utf8().unwrap(), "Mapping");
        let tags = mapping
            .find_ctx(2)
            .unwrap()
            .array()
            .unwrap()
            .iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            tags[0]
                .structure()
                .unwrap()
                .find_ctx(1)
                .unwrap()
                .u16()
                .unwrap(),
            0x4002
        );

        let mut input = [0; 32];
        let length = mode_request(&mut input, 2);
        let request = TLVElement::new(&input[..length]);
        let mut writer = WriteBuf::new(&mut buffer);
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
        assert_eq!(device.cycle_mode.get(), CycleMode::Mapping);
        device.apply_attributes(
            &[StateAttribute::StatusStateAttribute {
                value: StatusValue::Moving,
                flag: StatusFlag::Mapping,
                error: None,
            }],
            &(),
            0x54,
            0x55,
            0x61,
        );
        assert_eq!(
            rvc_run_mode::ClusterAsyncHandler::current_mode(&device, Context::default())
                .await
                .unwrap(),
            2
        );
        let mut writer = WriteBuf::new(&mut buffer);
        let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
            TLVWriteParent::new((), &mut writer),
            &TLVTag::Anonymous,
        )
        .unwrap();
        rvc_operational_state::ClusterAsyncHandler::handle_pause(
            &device,
            Context::default(),
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
                .structure()
                .unwrap()
                .find_ctx(0)
                .unwrap()
                .u8()
                .unwrap(),
            3
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
        let mut writer = WriteBuf::new(&mut buffer);
        let response = rvc_operational_state::OperationalCommandResponseBuilder::new(
            TLVWriteParent::new((), &mut writer),
            &TLVTag::Anonymous,
        )
        .unwrap();
        rvc_operational_state::ClusterAsyncHandler::handle_resume(
            &device,
            Context::default(),
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
                .structure()
                .unwrap()
                .find_ctx(0)
                .unwrap()
                .u8()
                .unwrap(),
            1
        );
        // Neither unsupported Pause nor Resume sends an HTTP command; next is Stop.
        let length = mode_request(&mut input, 0);
        let request = TLVElement::new(&input[..length]);
        let mut writer = WriteBuf::new(&mut buffer);
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
        assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
        // Timing is now present for this acknowledged, continuously observed mapping.
        let events = device.pending_events.borrow();
        assert_eq!(events.len(), 1);
        assert!(matches!(events.front().unwrap(), OperationalEvent::CompletedWithTiming(0,
            CompletionTiming { total_seconds: Some(_), paused_seconds: Some(0) })));
        drop(events);
    }));
    server.join().unwrap();
}

#[test]
fn rejected_mapping_api_command_does_not_create_phantom_cycle_or_completion() {
    let (url, server) = serve(vec![Reply {
        method: "PUT",
        path: "/api/v2/robot/capabilities/MappingPassCapability",
        status: 503,
        body: "{}",
        expected: Some(serde_json::json!({"action":"start_mapping"})),
    }]);
    let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
    let mut device = super::cycle_tests::device();
    device.client = ValetudoClient::new(executor, url.parse().unwrap());
    device
        .capabilities
        .insert(Capability::MappingPassCapability);
    smol::block_on(executor.run(async {
        let mut input = [0; 32];
        let length = mode_request(&mut input, 2);
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
            2
        );
        assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
        assert!(device.pending_events.borrow().is_empty());
    }));
    server.join().unwrap();
}

#[test]
fn mapping_state_without_mapping_capability_is_not_published_as_a_selectable_mode() {
    let device = super::cycle_tests::device();
    device.apply_attributes(
        &[StateAttribute::StatusStateAttribute {
            value: StatusValue::Moving,
            flag: StatusFlag::Mapping,
            error: None,
        }],
        &(),
        0x54,
        0x55,
        0x61,
    );
    assert!(
        smol::block_on(rvc_run_mode::ClusterAsyncHandler::current_mode(
            &device,
            Context::default()
        ))
        .is_err()
    );
}

#[test]
fn actual_start_ack_and_continuous_observation_control_encoded_timing_presence() {
    for gap in [false, true] {
        let (url, server) = serve(vec![Reply {
            method: "PUT",
            path: "/api/v2/robot/capabilities/BasicControlCapability",
            status: 200,
            body: "{}",
            expected: Some(serde_json::json!({"action":"start"})),
        }]);
        let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
        let mut device = super::cycle_tests::device();
        device.client = ValetudoClient::new(executor, url.parse().unwrap());
        smol::block_on(executor.run(async {
            let mut input = [0; 32];
            let len = mode_request(&mut input, 1);
            let request = TLVElement::new(&input[..len]);
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
        }));
        let observe = |value| {
            device.apply_attributes(
                &[StateAttribute::StatusStateAttribute {
                    value,
                    flag: StatusFlag::None,
                    error: None,
                }],
                &(),
                0x54,
                0x55,
                0x61,
            )
        };
        observe(StatusValue::Cleaning);
        if gap {
            device.apply_snapshot(&[], &(), 0x54, 0x55, 0x61);
            observe(StatusValue::Cleaning);
        }
        observe(StatusValue::Idle);
        assert_eq!(device.pending_events.borrow().len(), 1);
        let event = device.pending_events.borrow().front().cloned().unwrap();
        let mut bytes = [0; 128];
        let mut writer = WriteBuf::new(&mut bytes);
        event.write(&mut writer).unwrap();
        let fields = TLVElement::new(writer.as_slice()).structure().unwrap();
        assert_eq!(fields.find_ctx(0).unwrap().u8().unwrap(), 0);
        if gap {
            assert!(fields.find_ctx(1).unwrap().is_empty());
            assert!(fields.find_ctx(2).unwrap().is_empty());
        } else {
            assert!(fields.find_ctx(1).unwrap().u32().is_ok());
            assert_eq!(fields.find_ctx(2).unwrap().u32().unwrap(), 0);
        }
        server.join().unwrap();
    }
}

#[test]
fn power_source_attributes_and_endpoint_list_follow_robot_api_not_host() {
    use crate::power_source::PowerSource;
    use rs_matter::dm::clusters::decl::power_source::{
        BatChargeLevelEnum, BatChargeStateEnum, ClusterHandler, PowerSourceStatusEnum,
    };
    let device = super::cycle_tests::device();
    let ctx = Context::default();
    device.apply_attributes(
        &[StateAttribute::BatteryStateAttribute {
            level: Some(73.5),
            flag: BatteryFlag::Discharging,
        }],
        &ctx,
        0x54,
        0x55,
        0x61,
    );
    assert!(ctx.cluster_paths.borrow().contains(&(2, 0x2f)));
    assert!(ctx.cluster_paths.borrow().contains(&(1, 0x61)));
    for native in [false, true] {
        let power = PowerSource::new(&device, native);
        assert_eq!(
            power.bat_percent_remaining(&ctx).unwrap(),
            Some(147u8).into()
        );
        assert_eq!(
            power.bat_charge_level(&ctx).unwrap(),
            BatChargeLevelEnum::OK
        );
        assert_eq!(
            power.bat_charge_state(&ctx).unwrap(),
            BatChargeStateEnum::IsNotCharging
        );
        assert_eq!(power.status(&ctx).unwrap(), PowerSourceStatusEnum::Active);
        let mut bytes = [0; 64];
        let mut writer = WriteBuf::new(&mut bytes);
        let builder = rs_matter::tlv::ToTLVArrayBuilder::new(
            TLVWriteParent::new((), &mut writer),
            &TLVTag::Anonymous,
        )
        .unwrap();
        power
            .endpoint_list(&ctx, dm::ArrayAttributeRead::ReadAll(builder))
            .unwrap();
        let endpoints = TLVElement::new(writer.as_slice())
            .array()
            .unwrap()
            .iter()
            .map(|x| x.unwrap().u16().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(endpoints, if native { vec![0, 1, 2] } else { vec![1, 2] });
    }
    let power = PowerSource::new(&device, true);
    let version = power.dataver();
    device.apply_attributes(
        &[StateAttribute::BatteryStateAttribute {
            level: Some(100.0),
            flag: BatteryFlag::Charged,
        }],
        &ctx,
        0x54,
        0x55,
        0x61,
    );
    assert_ne!(version, power.dataver());
    assert_eq!(
        power.bat_charge_state(&ctx).unwrap(),
        BatChargeStateEnum::IsAtFullCharge
    );
    assert_eq!(
        power.status(&ctx).unwrap(),
        PowerSourceStatusEnum::Unspecified
    );
    ctx.cluster_paths.borrow_mut().clear();
    device.apply_snapshot(&[], &ctx, 0x54, 0x55, 0x61);
    assert_eq!(
        power.bat_percent_remaining(&ctx).unwrap(),
        None::<u8>.into()
    );
    assert!(power.bat_charge_level(&ctx).is_err());
    assert_eq!(
        power.bat_charge_state(&ctx).unwrap(),
        BatChargeStateEnum::Unknown
    );
    assert!(ctx.cluster_paths.borrow().contains(&(2, 0x2f)));
}

#[test]
fn power_source_serialized_percent_does_not_leak_raw_updates_on_flag_reports() {
    use crate::power_source::PowerSource;
    use rs_matter::{
        dm::clusters::decl::power_source::{BatChargeStateEnum, ClusterHandler},
        tlv::ToTLV,
    };
    let device = super::cycle_tests::device();
    let power = PowerSource::new(&device, true);
    let ctx = Context::default();
    let t = std::time::Instant::now();
    let first = BatteryState {
        half_percent: Some(100),
        flag: BatteryFlag::Discharging,
    };
    device.battery.set(first);
    device.power_battery.update(first, t, &ctx);
    let next = BatteryState {
        half_percent: Some(150),
        flag: BatteryFlag::Charging,
    };
    device.battery.set(next);
    device
        .power_battery
        .update(next, t + std::time::Duration::from_secs(1), &ctx);
    assert_eq!(device.battery.get(), next); // RVC continues to consume immediate raw state.
    assert_eq!(
        power.bat_charge_state(&ctx).unwrap(),
        BatChargeStateEnum::IsCharging
    );
    for (now, expected) in [(1, 100), (9, 100), (10, 150)] {
        device.power_battery.update(
            device.battery.get(),
            t + std::time::Duration::from_secs(now),
            &ctx,
        );
        let mut bytes = [0; 32];
        let mut writer = WriteBuf::new(&mut bytes);
        power
            .bat_percent_remaining(&ctx)
            .unwrap()
            .to_tlv(&TLVTag::Anonymous, &mut writer)
            .unwrap();
        assert_eq!(TLVElement::new(writer.as_slice()).u8().unwrap(), expected);
    }
    device.power_battery.update(
        BatteryState::default(),
        t + std::time::Duration::from_secs(11),
        &ctx,
    );
    let mut bytes = [0; 32];
    let mut writer = WriteBuf::new(&mut bytes);
    power
        .bat_percent_remaining(&ctx)
        .unwrap()
        .to_tlv(&TLVTag::Anonymous, &mut writer)
        .unwrap();
    TLVElement::new(writer.as_slice()).null().unwrap();
}

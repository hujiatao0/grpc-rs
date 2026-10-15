// Copyright 2026 PingCAP, Inc.

//! Opt-in request timing for IDC experiments. GRPCIO_TRACE_FILE enables a
//! bounded, asynchronous JSONL recorder. IDs are process-local unless explicitly
//! mapped by the application. No request contents are recorded.

use std::cell::Cell;
use std::ffi::c_void;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::OnceLock;
use std::thread::{self, Thread};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

thread_local! {
    static CURRENT: Cell<Request> = const { Cell::new(Request { call: 0, request: 0 }) };
    static CQ: Cell<u64> = const { Cell::new(0) };
    static OPERATION: Cell<u64> = const { Cell::new(0) };
}

static RECORDER: OnceLock<Option<Recorder>> = OnceLock::new();
static DROPPED: AtomicU64 = AtomicU64::new(0);
static NEXT_SPAN: AtomicU64 = AtomicU64::new(1);
static RECORDING: AtomicBool = AtomicBool::new(false);

extern "C" {
    fn grpc_cq_set_diagnostic_callback(
        callback: Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>,
    );
}

struct Recorder {
    start: Instant,
    sender: SyncSender<(u64, Thread, Event)>,
}

/// A fixed-size event. The interpretation of value/value2 is named by event.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub event: &'static str,
    pub call: u64,
    pub request: u64,
    pub cq: u64,
    pub operation: u64,
    pub value: u64,
    pub value2: u64,
}

impl Event {
    /// Captures the current request, CQ and completion-operation context.
    pub fn new(event: &'static str) -> Self {
        let request = Request::current();
        Self {
            event,
            call: request.call,
            request: request.request,
            cq: CQ.with(Cell::get),
            operation: OPERATION.with(Cell::get),
            value: 0,
            value2: 0,
        }
    }
}

/// Correlation identity that can be carried across an asynchronous callback.
#[derive(Clone, Copy, Debug)]
pub struct Request {
    pub call: u64,
    pub request: u64,
}

impl Request {
    /// Captures identity without retaining the gRPC call or request contents.
    pub fn current() -> Self {
        CURRENT.with(Cell::get)
    }

    /// Records a stage for this request, including on a storage worker thread.
    pub fn record(self, event: &'static str, value: u64, value2: u64) {
        record(Event {
            call: self.call,
            request: self.request,
            value,
            value2,
            ..Event::new(event)
        });
    }

    /// Installs identity for this synchronous scope; do not hold across await.
    pub fn enter(self) -> RequestScope {
        RequestScope(CURRENT.with(|current| current.replace(self)))
    }
}

/// Restores the previous request identity, including when a handler unwinds.
pub struct RequestScope(Request);

impl Drop for RequestScope {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(self.0));
    }
}

pub(crate) struct OperationScope(u64);

impl OperationScope {
    pub(crate) fn enter(operation: u64) -> Self {
        Self(OPERATION.with(|current| current.replace(operation)))
    }
}

impl Drop for OperationScope {
    fn drop(&mut self) {
        OPERATION.with(|current| current.set(self.0));
    }
}

pub(crate) fn set_cq(cq: u64) {
    CQ.with(|current| current.set(cq));
}

/// Records a synchronous processing interval, such as one protobuf encode.
pub struct Span {
    end: Event,
}

impl Span {
    /// Records begin immediately and end on drop, using one unique span ID.
    pub fn new(begin: &'static str, end: &'static str, call: u64) -> Self {
        let mut event = Event::new(begin);
        event.call = call;
        event.operation = NEXT_SPAN.fetch_add(1, Ordering::Relaxed);
        record(event);
        event.event = end;
        Self { end: event }
    }

    /// Adds a byte count or other event-specific measurement to the end event.
    pub fn set_value(&mut self, value: u64) {
        self.end.value = value;
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        record(self.end);
    }
}

/// Whether recording was configured at the first environment's creation.
/// This remains true while runtime capture is off, so stream bindings survive.
pub fn enabled() -> bool {
    matches!(RECORDER.get(), Some(Some(_)))
}

/// Attempts to record without waiting for disk or channel capacity. Any dropped
/// events invalidate complete-request attribution and are reported every second.
pub fn record(event: Event) {
    // Keep low-volume stream metadata even while capture is off. Turning capture
    // on must not require reconnecting or changing which CQ owns a stream.
    if !RECORDING.load(Ordering::Relaxed) && event.event != "stream_bind" {
        return;
    }
    if let Some(Some(recorder)) = RECORDER.get() {
        let at = recorder.start.elapsed().as_nanos() as u64;
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) =
            recorder.sender.try_send((at, thread::current(), event))
        {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn capture_from_control(path: Option<&Path>) -> bool {
    match path {
        None => true,
        Some(path) => std::fs::read_to_string(path)
            .map(|value| value.trim() == "on")
            .unwrap_or(false),
    }
}

fn set_recording(active: bool) {
    RECORDING.store(active, Ordering::Release);
    unsafe {
        grpc_cq_set_diagnostic_callback(if active { Some(cq_ready) } else { None });
    }
}

unsafe extern "C" fn cq_ready(cq: *mut c_void, tag: *mut c_void) {
    record(Event {
        cq: cq as u64,
        operation: tag as u64,
        ..Event::new("cq_ready")
    });
}

pub(crate) fn init() {
    RECORDER.get_or_init(|| {
        let path = std::env::var_os("GRPCIO_TRACE_FILE")?;
        // Refuse to append traces from different process/clock lifetimes.
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .expect("create GRPCIO_TRACE_FILE");
        let (sender, receiver) = mpsc::sync_channel::<(u64, Thread, Event)>(65536);
        let start = Instant::now();
        let control = std::env::var_os("GRPCIO_TRACE_CONTROL").map(std::path::PathBuf::from);
        let mut active = capture_from_control(control.as_deref());
        set_recording(active);
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();
        thread::Builder::new()
            .name("grpc-trace-writer".into())
            .spawn(move || {
                let mut writer = BufWriter::new(file);
                writeln!(
                    writer,
                    "{}",
                    serde_json::json!({"event":"trace_header", "schema":1,
                "pid":std::process::id(), "unix_ns":wall, "clock":"process_monotonic_ns",
                "active":active})
                )
                .unwrap();
                let mut last_flush = Instant::now();
                let mut last_control = Instant::now();
                loop {
                    match receiver.recv_timeout(Duration::from_millis(100)) {
                        Ok((at, source, event)) => {
                            writeln!(
                                writer,
                                "{}",
                                serde_json::json!({"at_ns":at,
                            "thread":format!("{:?}", source.id()), "thread_name":source.name(),
                            "event":event.event, "call":event.call, "request":event.request,
                            "cq":event.cq, "operation":event.operation,
                            "value":event.value, "value2":event.value2})
                            )
                            .unwrap();
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    if last_control.elapsed() >= Duration::from_millis(100) {
                        let requested = capture_from_control(control.as_deref());
                        if requested != active {
                            active = requested;
                            set_recording(active);
                            writeln!(
                                writer,
                                "{}",
                                serde_json::json!({
                                    "event":"trace_control", "active":active,
                                    "at_ns":start.elapsed().as_nanos() as u64
                                })
                            )
                            .unwrap();
                            writer.flush().unwrap();
                        }
                        last_control = Instant::now();
                    }
                    if last_flush.elapsed() >= Duration::from_secs(1) {
                        writeln!(
                            writer,
                            "{}",
                            serde_json::json!({"event":"trace_stats",
                        "dropped":DROPPED.load(Ordering::Relaxed), "active":active,
                        "at_ns":start.elapsed().as_nanos() as u64})
                        )
                        .unwrap();
                        writer.flush().unwrap();
                        last_flush = Instant::now();
                    }
                }
            })
            .expect("start gRPC diagnostic writer");
        Some(Recorder { start, sender })
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_capture_preserves_bindings_and_suppresses_disabled_events() {
        if std::env::var_os("GRPCIO_RUNTIME_PROBE_CHILD").is_none() {
            let path = std::env::temp_dir().join(format!(
                "grpc-runtime-probe-{}-{}.jsonl",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let control = path.with_extension("control");
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "diagnostics::tests::runtime_capture_preserves_bindings_and_suppresses_disabled_events",
                    "--test-threads=1",
                ])
                .env("GRPCIO_RUNTIME_PROBE_CHILD", "1")
                .env("GRPCIO_TRACE_FILE", &path)
                .env("GRPCIO_TRACE_CONTROL", &control)
                .output().unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let mut probes = Vec::new();
            let mut bindings = 0;
            for line in std::fs::read_to_string(&path).unwrap().lines() {
                let row: serde_json::Value = serde_json::from_str(line).unwrap();
                match row["event"].as_str() {
                    Some("probe") => probes.push(row["value"].as_u64().unwrap()),
                    Some("stream_bind") => bindings += 1,
                    Some("trace_stats") => assert_eq!(row["dropped"].as_u64(), Some(0)),
                    _ => {}
                }
            }
            assert_eq!(probes, vec![2, 4]);
            assert_eq!(bindings, 1);
            std::fs::remove_file(path).unwrap();
            std::fs::remove_file(control).unwrap();
            return;
        }
        init();
        assert!(enabled());
        assert!(!RECORDING.load(Ordering::Acquire)); // Missing control fails closed.
        let request = Request {
            call: 11,
            request: 22,
        };
        request.record("stream_bind", 99, 0);
        request.record("probe", 1, 0);
        let control = std::env::var_os("GRPCIO_TRACE_CONTROL").unwrap();
        for (active, value) in [(true, 2), (false, 3), (true, 4)] {
            std::fs::write(&control, if active { "on" } else { "off" }).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while RECORDING.load(Ordering::Acquire) != active {
                assert!(
                    Instant::now() < deadline,
                    "control acknowledgement timed out"
                );
                thread::sleep(Duration::from_millis(10));
            }
            assert!(enabled());
            request.record("probe", value, 0);
        }
        std::fs::write(&control, "invalid").unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while RECORDING.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        }
        thread::sleep(Duration::from_millis(1200));
    }

    #[test]
    fn native_observer_measures_a_blocked_cq() {
        // Isolate the process-wide opt-in recorder from other unit tests.
        if std::env::var_os("GRPCIO_CQ_PROBE_CHILD").is_none() {
            let path = std::env::temp_dir().join(format!(
                "grpc-cq-probe-{}-{}.jsonl",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "diagnostics::tests::native_observer_measures_a_blocked_cq",
                    "--test-threads=1",
                ])
                .env("GRPCIO_CQ_PROBE_CHILD", "1")
                .env("GRPCIO_TRACE_FILE", &path)
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let mut ready = std::collections::HashMap::new();
            let mut maximum_wait = 0;
            for line in std::fs::read_to_string(&path).unwrap().lines() {
                let row: serde_json::Value = serde_json::from_str(line).unwrap();
                match row["event"].as_str() {
                    Some("cq_ready") => {
                        ready.insert(
                            row["operation"].as_u64().unwrap(),
                            row["at_ns"].as_u64().unwrap(),
                        );
                    }
                    Some("cq_pop") => {
                        if let Some(at) = ready.remove(&row["operation"].as_u64().unwrap()) {
                            maximum_wait = maximum_wait.max(row["at_ns"].as_u64().unwrap() - at);
                        }
                    }
                    Some("trace_stats") => assert_eq!(row["dropped"].as_u64(), Some(0)),
                    _ => {}
                }
            }
            std::fs::remove_file(path).unwrap();
            assert!(
                maximum_wait >= 20_000_000,
                "CQ wait was only {}ns",
                maximum_wait
            );
            return;
        }

        use crate::task::{CallTag, Executor, Kicker};
        use crate::{CallOption, ChannelBuilder, EnvBuilder, Marshaller, Method, MethodType};
        use std::sync::Arc;

        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(1)
                .name_prefix("cq-probe")
                .build(),
        );
        let channel = ChannelBuilder::new(env).connect("127.0.0.1:1");
        let method = Method {
            ty: MethodType::Unary,
            name: "/diagnostic/Probe",
            req_mar: Marshaller {
                ser: |_: &(), _| Ok(()),
                de: |_| Ok(()),
            },
            resp_mar: Marshaller {
                ser: |_: &(), _| Ok(()),
                de: |_| Ok(()),
            },
        };
        let call = channel
            .create_call(&method, &CallOption::default())
            .unwrap();
        let kicker = Kicker::from_call(call);
        let (wake_tx, wake_rx) = mpsc::channel();
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut first = true;
        Executor::new(channel.cq()).spawn(
            futures_util::future::poll_fn(move |cx| {
                if first {
                    first = false;
                    wake_tx.send(cx.waker().clone()).unwrap();
                    return std::task::Poll::Pending;
                }
                blocked_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                std::task::Poll::Ready(())
            }),
            kicker.clone(),
        );
        wake_rx.recv_timeout(Duration::from_secs(5)).unwrap().wake();
        blocked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (future, tag) = CallTag::action_pair();
        // An empty batch completes on this thread even while the CQ is blocked.
        kicker.kick(Box::new(tag)).unwrap();
        thread::sleep(Duration::from_millis(30));
        release_tx.send(()).unwrap();
        futures_executor::block_on(future).unwrap();
        thread::sleep(Duration::from_millis(1200));
    }

    #[test]
    fn request_scope_restores_identity_after_nested_work() {
        let original = Request::current();
        {
            let _outer = Request {
                call: 11,
                request: 22,
            }
            .enter();
            let carried = Request::current();
            let restored = thread::spawn(move || {
                let _scope = carried.enter();
                let event = Event::new("storage_ready");
                (event.call, event.request)
            })
            .join()
            .unwrap();
            assert_eq!(restored, (11, 22));
            {
                let _inner = Request {
                    call: 33,
                    request: 44,
                }
                .enter();
                assert_eq!(Event::new("nested").request, 44);
            }
            assert_eq!(Request::current().request, 22);
        }
        assert_eq!(Request::current().call, original.call);
        assert_eq!(Request::current().request, original.request);
    }
}

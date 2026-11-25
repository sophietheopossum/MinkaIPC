//! Non-blocking NDJSON client for the ShojiWM IPC socket.
//!
//! Wire format 
//! (see `ShojiWM/packages/config/src/minka/workspace-ipc.ts` in
//! the live config; `~/.config/shojiwm` is a stale installed
//! copy the session overrides via `SHOJI_CONFIG`):
//!   request    {"id": n, "method": "...", "params": {...}}
//!   response   {"id": n, "result": ...} | {"id": n, "error": ...}
//!   broadcast  {"event": "...", "payload": ...}
//!
//! Design rule R1 of the Minka shell: never block the render loop. All
//! socket I/O lives on a dedicated thread; events reach the consumer
//! through a `calloop` channel it inserts into its own event loop. The
//! socket is recreated whenever the ShojiWM config hot-reloads, so
//! reconnecting forever (1s backoff) is a feature, not error handling.

use std::io::{
    BufRead,
    BufReader,
    Read,
    Write,
};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};
use std::sync::{
    Arc,
    Mutex,
};
use std::thread;
use std::time::{
    Duration,
    Instant,
};

use calloop::channel::{
    self,
    Channel,
};
use serde_json::{
    json,
    Value,
};

#[derive(Debug)]
pub enum IpcEvent {
    /// Socket (re)connected. Re-request any state you cache.
    Connected,
    /// Socket lost; the client is already retrying in the background.
    Disconnected,
    /// Server-initiated broadcast, e.g. `snap.preview`.
    Broadcast {
        event: String,
        payload: Value,
    },
    /// Reply to a `request()`, correlated by the returned id.
    Response {
        id: u64,
        result: Result<Value, Value>,
    },
}

pub struct IpcClient {
    writer: Arc<Mutex<Option<UnixStream>>>,
    next_id: AtomicU64,
}

impl IpcClient {
    /// `$XDG_RUNTIME_DIR/shojiwm-$WAYLAND_DISPLAY.sock`
    pub fn socket_path() -> Option<PathBuf> {
        let runtime = std::env::var_os(
            "XDG_RUNTIME_DIR",
        )?;
        let display = std::env::var(
            "WAYLAND_DISPLAY",
        ).ok()?;
        Some(
            PathBuf::from(
                runtime
            ).join(
                format!(
                    "shojiwm-{display}.sock"
                )
            )
        )
    }

    /// Start the I/O thread. Insert the returned channel into a calloop
    /// event loop; the thread exits on its own once the channel is dropped.
    pub fn spawn() -> std::io::Result<(Arc<IpcClient>, Channel<IpcEvent>)> {
        let path = Self::socket_path()
            .ok_or_else(|| {
                std::io::Error::other(
                    "XDG_RUNTIME_DIR / WAYLAND_DISPLAY not set",
                )
            })?;

        let (tx, rx) = channel::channel();
        let client = Arc::new(IpcClient {
            writer: Arc::new(
                Mutex::new(
                    None,
                ),
            ),
            next_id: AtomicU64::new(1),
        });

        let writer = Arc::clone(&client.writer);
        thread::Builder::new()
            .name("minka-ipc".into())
            .spawn(move || loop {
                let stream = match UnixStream::connect(&path) {
                    Ok(s) => s,
                    Err(_) => {
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };
                *writer
                    .lock()
                    .unwrap() = stream
                    .try_clone()
                    .ok();
                if tx.send(
                    IpcEvent::Connected
                ).is_err() {
                    // consumer gone
                    return;
                }

                let reader = BufReader::new(
                    &stream,
                );
                for line in reader.lines() {
                    let Ok(
                        line
                    ) = line else {
                        break
                    };
                    let Some(
                        event
                    ) = parse_line(
                        &line
                    ) else {
                        continue;
                    };
                    if tx.send(event).is_err() {
                        return;
                    }
                }

                *writer
                    .lock()
                    .unwrap() = None;
                if tx.send(
                    IpcEvent::Disconnected
                ).is_err() {
                    return;
                }
                thread::sleep(
                    Duration::from_secs(1)
                );
            })?;

        Ok((client, rx))
    }

    /// Fire a request; the reply arrives as `IpcEvent::Response` with the
    /// returned id. Returns None when currently disconnected (drop-and-move-
    /// on is correct here: on `Connected` callers re-request fresh state).
    pub fn request(
        &self, method: &str, params: Value
    ) -> Option<u64> {
        let id = self.next_id.
            fetch_add(
                1,
                Ordering::Relaxed,
            );
        let line = json!({ "id": id, "method": method, "params": params });
        let mut guard = self.writer
            .lock()
            .unwrap();
        let stream = guard
            .as_mut()?;
        let mut payload = line
            .to_string();
        payload
            .push(
                '\n',
            );
        match stream.write_all(
            payload
                .as_bytes(),
        ) {
            Ok(()) => Some(id),
            Err(_) => {
                // reader thread will notice and reconnect
                *guard = None;
                None
            }
        }
    }
}

/// Why [`request_blocking`] got no result.
#[derive(Debug)]
pub enum RequestError {
    /// `XDG_RUNTIME_DIR` or `WAYLAND_DISPLAY` is not set.
    NoSocket,
    /// Connecting, sending or reading failed, or the server hung up
    /// before it answered.
    Io(std::io::Error),
    /// No answer within the timeout.
    TimedOut,
    /// The server answered with an error.
    Server(Value),
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestError::NoSocket => f.write_str("XDG_RUNTIME_DIR / WAYLAND_DISPLAY not set"),
            RequestError::Io(e) => write!(f, "{e}"),
            RequestError::TimedOut => f.write_str("no answer in time"),
            RequestError::Server(e) => write!(f, "the compositor answered with an error: {e}"),
        }
    }
}

impl std::error::Error for RequestError {}

/// One request and its result, on a connection of its own, for a caller
/// with no event loop to put [`IpcClient`]'s channel in (a command line).
/// Once connected it waits for at most `timeout`, however the answer
/// trickles in; broadcasts that arrive meanwhile are skipped. Connecting
/// itself has no bound, as the server accepts on a thread of its own.
/// Not for a render loop (rule R1).
pub fn request_blocking(
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, RequestError> {
    let path = IpcClient::socket_path().ok_or(RequestError::NoSocket)?;
    request_blocking_at(&path, method, params, timeout)
}

fn request_blocking_at(
    path: &std::path::Path,
    method: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value, RequestError> {
    let deadline = Instant::now() + timeout;
    // What is left of the timeout; never zero, which would mean none.
    let left = || {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
            .ok_or(RequestError::TimedOut)
    };
    let failed = |e: std::io::Error| match e.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => RequestError::TimedOut,
        _ => RequestError::Io(e),
    };
    let mut stream = UnixStream::connect(path).map_err(RequestError::Io)?;
    stream.set_write_timeout(Some(left()?)).map_err(RequestError::Io)?;
    let mut line = json!({ "id": 1, "method": method, "params": params }).to_string();
    line.push('\n');
    stream.write_all(line.as_bytes()).map_err(failed)?;
    // Read by hand, not with read_line: that reads on until a newline, so
    // a peer sending a byte now and then would hold it past the deadline.
    // Here the deadline is looked at before every read.
    let mut pending: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        while let Some(end) = pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = pending.drain(..=end).collect();
            let event = std::str::from_utf8(&line).ok().and_then(parse_line);
            if let Some(IpcEvent::Response { id: 1, result }) = event {
                return result.map_err(RequestError::Server);
            }
        }
        stream.set_read_timeout(Some(left()?)).map_err(RequestError::Io)?;
        match stream.read(&mut chunk) {
            Ok(0) => {
                return Err(RequestError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the compositor hung up before it answered",
                )));
            }
            Ok(n) => pending.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(failed(e)),
        }
    }
}

fn parse_line(
    line: &str
) -> Option<IpcEvent> {
    let value: Value = serde_json::from_str(
        line
    )
        .ok()?;
    if let Some(
        event
    ) = value.get("event").and_then(Value::as_str) {
        return Some(IpcEvent::Broadcast {
            event: event
                .to_string(),
            payload: value
                .get(
                    "payload"
                ).cloned()
                .unwrap_or(
                    Value::Null
                ),
        });
    }
    let id = value
        .get("id").and_then(Value::as_u64)?;
    let result = match value.get("error") {
        Some(err) => Err(
            err
                .clone(),
        ),
        None => Ok(
            value
                .get(
                    "result"
                ).cloned()
                .unwrap_or(
                    Value::Null
                )
        ),
    };
    Some(
        IpcEvent::Response {
            id,
            result,
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    /// A socket in a directory of its own, answered once by `serve`.
    fn server(
        name: &str,
        serve: impl FnOnce(UnixStream) + Send + 'static,
    ) -> (std::path::PathBuf, thread::JoinHandle<()>) {
        let dir = std::env::temp_dir().join(format!("minka-ipc-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ipc.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            serve(stream);
        });
        (path, handle)
    }

    fn read_request(stream: &UnixStream) -> Value {
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn the_answer_is_found_after_broadcasts() {
        let (path, server) = server("answer", |mut stream| {
            let request = read_request(&stream);
            assert_eq!(request["method"], "workspaces.get");
            assert_eq!(request["params"], json!({}));
            let id = &request["id"];
            let reply = format!(
                "{}\n{}\n",
                json!({ "event": "snap.preview", "payload": null }),
                json!({ "id": id, "result": { "currentMonitor": "DP-1" } }),
            );
            stream.write_all(reply.as_bytes()).unwrap();
        });
        let result =
            request_blocking_at(&path, "workspaces.get", json!({}), Duration::from_secs(5));
        server.join().unwrap();
        cleanup(&path);
        assert_eq!(result.unwrap()["currentMonitor"], "DP-1");
    }

    #[test]
    fn an_error_answer_is_an_error() {
        let (path, server) = server("error", |mut stream| {
            let id = read_request(&stream)["id"].clone();
            let reply = format!("{}\n", json!({ "id": id, "error": "no such method" }));
            stream.write_all(reply.as_bytes()).unwrap();
        });
        let result = request_blocking_at(&path, "nope", Value::Null, Duration::from_secs(5));
        server.join().unwrap();
        cleanup(&path);
        assert!(matches!(result, Err(RequestError::Server(e)) if e == "no such method"));
    }

    #[test]
    fn silence_times_out() {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let (path, server) = server("silence", move |stream| {
            // Holds the connection open, answering nothing, until the test is done.
            let _ = rx.recv();
            drop(stream);
        });
        let start = Instant::now();
        let result =
            request_blocking_at(&path, "workspaces.get", json!({}), Duration::from_millis(200));
        let took = start.elapsed();
        tx.send(()).unwrap();
        server.join().unwrap();
        cleanup(&path);
        assert!(matches!(result, Err(RequestError::TimedOut)), "{result:?}");
        assert!(took < Duration::from_secs(2), "{took:?}");
    }

    #[test]
    fn a_line_that_trickles_in_times_out() {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let (path, server) = server("trickle", move |mut stream| {
            read_request(&stream);
            // A byte every 20 ms and never a newline, until the test is done.
            stream.write_all(b"{\"id\":1,").unwrap();
            while rx.try_recv().is_err() {
                if stream.write_all(b" ").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        });
        let start = Instant::now();
        let result =
            request_blocking_at(&path, "workspaces.get", json!({}), Duration::from_millis(200));
        let took = start.elapsed();
        tx.send(()).unwrap();
        server.join().unwrap();
        cleanup(&path);
        assert!(matches!(result, Err(RequestError::TimedOut)), "{result:?}");
        assert!(took < Duration::from_secs(1), "{took:?}");
    }

    #[test]
    fn an_answer_in_pieces_is_put_together() {
        let (path, server) = server("pieces", |mut stream| {
            let id = read_request(&stream)["id"].clone();
            let reply = format!("{}\n", json!({ "id": id, "result": { "currentMonitor": "eDP-1" } }));
            let (a, b) = reply.as_bytes().split_at(reply.len() / 2);
            stream.write_all(a).unwrap();
            thread::sleep(Duration::from_millis(30));
            stream.write_all(b).unwrap();
        });
        let result =
            request_blocking_at(&path, "workspaces.get", json!({}), Duration::from_secs(5));
        server.join().unwrap();
        cleanup(&path);
        assert_eq!(result.unwrap()["currentMonitor"], "eDP-1");
    }

    #[test]
    fn hanging_up_is_an_error() {
        let (path, server) = server("hangup", |stream| {
            read_request(&stream);
            drop(stream);
        });
        let result = request_blocking_at(&path, "workspaces.get", json!({}), Duration::from_secs(5));
        server.join().unwrap();
        cleanup(&path);
        assert!(matches!(result, Err(RequestError::Io(_))), "{result:?}");
    }

    #[test]
    fn no_socket_is_an_error() {
        let path = std::env::temp_dir().join(format!("minka-ipc-test-{}-none.sock", std::process::id()));
        let result = request_blocking_at(&path, "workspaces.get", json!({}), Duration::from_secs(1));
        assert!(matches!(result, Err(RequestError::Io(_))), "{result:?}");
    }
}

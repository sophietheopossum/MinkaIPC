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
use std::time::Duration;

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

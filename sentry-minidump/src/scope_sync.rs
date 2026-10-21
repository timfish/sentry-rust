//! Moves the main hub's scope to the crash reporter.
//!
//! Nothing crosses the process boundary until a crash. Then a helper
//! thread turns the main hub's scope into an [`Event`], and the crash
//! handler sends the JSON to the reporter before it requests the minidump.

#[cfg(target_os = "macos")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(target_os = "macos")]
use std::sync::Barrier;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread::{self, Thread};
use std::time::Duration;

use minidumper_child::MessageSender;
use sentry_core::protocol::Event;
use sentry_core::Hub;

/// Message kinds on the socket between the app and the reporter.
///
/// `MSG_SCOPE_UPDATE` carries a `ScopeUpdate` from the deprecated scope
/// methods on `MinidumpIntegration`.
pub(crate) const MSG_SCOPE_UPDATE: u32 = 0;
pub(crate) const MSG_SCOPE_CHUNK: u32 = 1;
pub(crate) const MSG_SCOPE_END: u32 = 2;

/// The reporter reads each message with one `recv`. A local socket on
/// macOS buffers at most 8 KiB, so a larger message never arrives. Keep
/// each message, with its header, well below that.
const CHUNK_SIZE: usize = 4 * 1024;

/// Hand-off between the crash handler and the helper thread that
/// serializes the scope.
///
/// The crash handler may run on the crashing thread, which can hold the
/// allocator or a hub lock. So the handler only touches atomics, wakes the
/// helper and waits at most `timeout`. All the work that can block runs
/// on the helper.
pub(crate) struct ScopeSync {
    done: AtomicBool,
    buffer: Mutex<Vec<u8>>,
    requested: AtomicBool,
    /// Unset when the helper thread failed to start.
    thread: OnceLock<Thread>,
    /// The helper's Mach port.
    #[cfg(target_os = "macos")]
    helper_port: AtomicU32,
    timeout: Duration,
}

impl ScopeSync {
    /// Starts the helper thread, which parks until a crash happens.
    pub(crate) fn start(timeout: Duration) -> Arc<Self> {
        let sync = Arc::new(ScopeSync {
            done: AtomicBool::new(false),
            buffer: Mutex::new(Vec::new()),
            requested: AtomicBool::new(false),
            thread: OnceLock::new(),
            #[cfg(target_os = "macos")]
            helper_port: AtomicU32::new(0),
            timeout,
        });

        // On macOS the handler needs the helper's port to resume it, so
        // `start` waits until the helper has stored it.
        #[cfg(target_os = "macos")]
        let port_stored = Arc::new(Barrier::new(2));
        #[cfg(target_os = "macos")]
        let helper_port_stored = port_stored.clone();

        let helper_sync = sync.clone();
        let spawned = thread::Builder::new()
            .name("sentry-minidump-scope".into())
            .spawn(move || {
                let sync = helper_sync;
                #[cfg(target_os = "macos")]
                {
                    // SAFETY: `pthread_self` is always valid for the
                    // calling thread.
                    #[expect(unsafe_code, reason = "reads the helper thread's Mach port")]
                    let port = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
                    sync.helper_port.store(port, Ordering::Release);
                    helper_port_stored.wait();
                }
                while !sync.requested.load(Ordering::Acquire) {
                    thread::park();
                }
                let bytes = scope_bytes();
                *sync.buffer.lock().unwrap_or_else(PoisonError::into_inner) = bytes;
                sync.done.store(true, Ordering::Release);
            });

        match spawned {
            Ok(handle) => {
                #[cfg(target_os = "macos")]
                port_stored.wait();
                let _ = sync.thread.set(handle.thread().clone());
            }
            Err(err) => {
                sentry_core::sentry_debug!("could not start scope helper thread: {err}");
            }
        }
        sync
    }

    /// Runs inside the crash handler.
    ///
    /// It must not allocate or take locks. It only touches atomics, wakes
    /// the helper, sleeps, and writes from a buffer the helper filled.
    pub(crate) fn on_crash(&self, sender: &MessageSender<'_>) {
        let Some(helper_thread) = self.thread.get() else {
            return;
        };
        self.requested.store(true, Ordering::Release);

        #[cfg(target_os = "macos")]
        resume_helper(self.helper_port.load(Ordering::Acquire));

        helper_thread.unpark();

        let step = Duration::from_millis(1);
        let mut waited = Duration::ZERO;
        while !self.done.load(Ordering::Acquire) {
            if waited >= self.timeout {
                return;
            }
            thread::sleep(step);
            waited = waited.saturating_add(step);
        }

        // The lock was released before `done` was set, so this cannot block.
        if let Ok(buffer) = self.buffer.try_lock() {
            send_chunks(sender, &buffer);
        }
    }
}

/// On macOS the crash handler runs on its own thread and suspends every
/// other thread first, the helper included.
#[cfg(target_os = "macos")]
fn resume_helper(port: u32) {
    // SAFETY: The helper only exits after it sets `done`, so `port`
    // still names it. `thread_resume` only lowers its suspend count.
    #[expect(unsafe_code, reason = "Mach call to resume the helper thread")]
    unsafe {
        mach2::thread_act::thread_resume(port);
    }
}

fn send_chunks(sender: &MessageSender<'_>, bytes: &[u8]) {
    for chunk in bytes.chunks(CHUNK_SIZE) {
        if sender.send_message(MSG_SCOPE_CHUNK, chunk).is_err() {
            return;
        }
    }
    sender.send_message(MSG_SCOPE_END, &[]).ok();
}

/// Serializes the main hub's scope.
///
/// `configure_scope` takes the hub's write lock. If the crashed thread
/// holds the hub's lock, this blocks and the crash handler gives up after
/// its timeout.
fn scope_bytes() -> Vec<u8> {
    Hub::main()
        .configure_scope(|scope| scope.clone())
        .apply_to_event(Event::default())
        .and_then(|event| serde_json::to_vec(&event).ok())
        .unwrap_or_default()
}

/// Collects the scope messages in the reporter process.
#[derive(Default)]
pub(crate) struct ScopeReceiver {
    chunks: Mutex<Vec<u8>>,
    event: Mutex<Option<Event<'static>>>,
}

impl ScopeReceiver {
    pub(crate) fn on_message(&self, kind: u32, buffer: &[u8]) {
        match kind {
            MSG_SCOPE_CHUNK => self
                .chunks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(buffer),
            MSG_SCOPE_END => {
                let bytes = std::mem::take(
                    &mut *self.chunks.lock().unwrap_or_else(PoisonError::into_inner),
                );
                if let Ok(event) = serde_json::from_slice::<Event<'static>>(&bytes) {
                    *self.event.lock().unwrap_or_else(PoisonError::into_inner) = Some(event);
                }
            }
            _ => {}
        }
    }

    /// Returns the received scope as an event, or an empty event.
    pub(crate) fn take_event(&self) -> Event<'static> {
        self.event
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receiver_reassembles_chunks() {
        let event = Event {
            message: Some("hello".into()),
            ..Default::default()
        };
        let bytes = serde_json::to_vec(&event).unwrap();
        let receiver = ScopeReceiver::default();
        let (a, b) = bytes.split_at(bytes.len() / 2);
        receiver.on_message(MSG_SCOPE_CHUNK, a);
        receiver.on_message(MSG_SCOPE_CHUNK, b);
        receiver.on_message(MSG_SCOPE_END, &[]);
        assert_eq!(receiver.take_event().message.as_deref(), Some("hello"));
        assert!(receiver.take_event().message.is_none());
    }

    #[test]
    fn receiver_ignores_bad_json_and_other_kinds() {
        let receiver = ScopeReceiver::default();
        receiver.on_message(99, b"ignored");
        receiver.on_message(MSG_SCOPE_CHUNK, b"not json");
        receiver.on_message(MSG_SCOPE_END, &[]);
        assert!(receiver.take_event().message.is_none());
    }

    #[test]
    fn scope_bytes_uses_the_main_hub() {
        Hub::main().configure_scope(|scope| scope.set_tag("hub", "main"));
        let event: Event<'static> = serde_json::from_slice(&scope_bytes()).unwrap();
        assert_eq!(event.tags.get("hub").map(String::as_str), Some("main"));
    }
}

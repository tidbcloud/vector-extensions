//! Runtime feature / mode switches shared across sources and sinks.
//!
//! Nextgen vs legacy is selected by the global environment variable
//! `VECTOR_NEXTGEN` (not a Cargo feature), so one binary serves both modes.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::OnceLock;

/// Environment variable that enables nextgen mode when set to a truthy value
/// (`1`, `true`, `yes`, or `on`; case-insensitive).
pub const VECTOR_NEXTGEN_ENV: &str = "VECTOR_NEXTGEN";

const UNSET: u8 = 0;
const LEGACY: u8 = 1;
const NEXTGEN: u8 = 2;

static MODE_CACHE: OnceLock<bool> = OnceLock::new();
static TEST_OVERRIDE: AtomicU8 = AtomicU8::new(UNSET);

fn parse_nextgen_env() -> bool {
    match std::env::var(VECTOR_NEXTGEN_ENV) {
        Ok(v) => {
            let v = v.trim();
            v == "1"
                || v.eq_ignore_ascii_case("true")
                || v.eq_ignore_ascii_case("yes")
                || v.eq_ignore_ascii_case("on")
        }
        Err(_) => false,
    }
}

/// Returns whether the process is running in nextgen mode.
///
/// Resolved once from [`VECTOR_NEXTGEN_ENV`] for the process lifetime, unless a
/// test override is active.
pub fn is_nextgen_mode() -> bool {
    match TEST_OVERRIDE.load(Ordering::Relaxed) {
        LEGACY => return false,
        NEXTGEN => return true,
        _ => {}
    }
    *MODE_CACHE.get_or_init(parse_nextgen_env)
}

/// Override nextgen mode for unit tests. Pass `None` to clear the override.
///
/// Prefer [`lock_nextgen_mode_for_test`] when tests may run in parallel.
#[cfg(test)]
pub fn set_nextgen_mode_for_test(enabled: Option<bool>) {
    match enabled {
        Some(true) => TEST_OVERRIDE.store(NEXTGEN, Ordering::SeqCst),
        Some(false) => TEST_OVERRIDE.store(LEGACY, Ordering::SeqCst),
        None => TEST_OVERRIDE.store(UNSET, Ordering::SeqCst),
    }
}

/// RAII guard that locks nextgen mode for the duration of a test and clears on drop.
#[cfg(test)]
pub struct NextgenModeGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
static TEST_MODE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Lock and set nextgen mode for a test. Clears the override when the guard is dropped.
#[cfg(test)]
pub fn lock_nextgen_mode_for_test(enabled: bool) -> NextgenModeGuard {
    let lock = TEST_MODE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    set_nextgen_mode_for_test(Some(enabled));
    NextgenModeGuard { _lock: lock }
}

#[cfg(test)]
impl Drop for NextgenModeGuard {
    fn drop(&mut self) {
        set_nextgen_mode_for_test(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_truthy_values() {
        let _guard = lock_nextgen_mode_for_test(false);
        // Override path
        assert!(!is_nextgen_mode());
        set_nextgen_mode_for_test(Some(true));
        assert!(is_nextgen_mode());
        set_nextgen_mode_for_test(Some(false));
        assert!(!is_nextgen_mode());
    }
}

//! Isolated platform transport integration experiments; no backend is admitted automatically.
#![deny(unsafe_op_in_unsafe_fn)]
#[cfg(target_os = "macos")]
mod ffi;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::*;
#[cfg(feature = "gnutls-probe")]
pub mod gnutls;
#[cfg(feature = "gnutls-probe")]
mod gnutls_core;
#[cfg(feature = "gnutls-probe")]
mod gnutls_ffi;
#[cfg(all(target_os = "linux", feature = "gnutls-probe"))]
pub use gnutls::*;

mod trust;
pub use trust::AdditionalRoots;

#[cfg(target_os = "macos")]
mod macos_trust;

// SPDX-License-Identifier: MIT
//! Opt-in OBS authentication configuration. Secrets have no serialization or
//! value-bearing Debug implementation. Unsupported reads select compatibility.
use serde_json::Value;
use std::{
    fmt,
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
};

const CONFIG_BYTES: u64 = 256 * 1024;
const SUBPATH: [&str; 4] = [
    "obs-studio",
    "plugin_config",
    "obs-websocket",
    "config.json",
];

pub fn trim(value: &str) -> &str {
    value.trim_matches(|ch| {
        matches!(ch,
        '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
        | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
        | '\u{205f}' | '\u{3000}' | '\u{feff}')
    })
}
pub fn enabled(value: &str) -> bool {
    matches!(
        trim(value).to_ascii_lowercase().as_str(),
        "1" | "true" | "yes"
    )
}
#[derive(Clone, Copy)]
pub enum Platform {
    Mac,
    Windows,
    Linux,
}
impl Platform {
    pub fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::Mac
        } else if cfg!(windows) {
            Self::Windows
        } else {
            Self::Linux
        }
    }
}
fn append(mut path: PathBuf) -> PathBuf {
    for part in SUBPATH {
        path.push(part);
    }
    // Node path.join normalizes lexically before opening; canonicalize would
    // follow symlinks and require an existing file, which changes that contract.
    let mut normalized = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(
                    normalized.components().next_back(),
                    Some(Component::Normal(_))
                ) {
                    normalized.pop();
                } else if !path.has_root() {
                    normalized.push("..");
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}
pub fn config_paths(
    get: impl Fn(&str) -> Option<String>,
    platform: Platform,
    home: &Path,
) -> Vec<PathBuf> {
    match platform {
        Platform::Mac => vec![append(home.join("Library").join("Application Support"))],
        Platform::Windows => vec![append(
            get("APPDATA")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join("AppData").join("Roaming")),
        )],
        Platform::Linux => vec![
            append(
                get("XDG_CONFIG_HOME")
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from)
                    .unwrap_or_else(|| home.join(".config")),
            ),
            append(home.join(".var/app/com.obsproject.Studio/config")),
        ],
    }
}

pub struct Resolution {
    password: Option<String>,
    diagnostic: Option<String>,
}
impl fmt::Debug for Resolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resolution")
            .field("password_present", &self.password.is_some())
            .field("diagnostic", &self.diagnostic)
            .finish()
    }
}
impl Resolution {
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
    pub fn diagnostic(&self) -> Option<&str> {
        self.diagnostic.as_deref()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unsupported {
    HomeUnavailable,
    EncodingOrSize,
}

/// None means unreadable/missing; Err means source behavior requires the SDK.
/// An environment password wins and disabled opt-in never invokes either callback.
pub fn resolve_password(
    get: impl Fn(&str) -> Option<String>,
    paths: impl FnOnce() -> Result<Vec<PathBuf>, Unsupported>,
    mut read: impl FnMut(&Path) -> Result<Option<String>, Unsupported>,
) -> Result<Resolution, Unsupported> {
    if let Some(password) = get("OBS_WEBSOCKET_PASSWORD").filter(|s| !s.is_empty()) {
        return Ok(Resolution {
            password: Some(password),
            diagnostic: None,
        });
    }
    if !get("OBS_MCP_READ_OBS_CONFIG").is_some_and(|s| enabled(&s)) {
        return Ok(Resolution {
            password: None,
            diagnostic: None,
        });
    }
    let paths = paths()?;
    for path in &paths {
        let Some(contents) = read(path)? else {
            continue;
        };
        let Ok(config) = serde_json::from_str::<Value>(&contents) else {
            // Node accepts escaped lone surrogates which serde rejects. Leave
            // malformed/unsupported JSON interpretation with the same owner.
            return Err(Unsupported::EncodingOrSize);
        };
        if config["auth_required"] == false {
            return Ok(Resolution {
                password: None,
                diagnostic: Some(format!(
                    "OBS WebSocket authentication is off according to {}",
                    path.display()
                )),
            });
        }
        if let Some(password) = config["server_password"].as_str().filter(|s| !s.is_empty()) {
            return Ok(Resolution {
                password: Some(password.into()),
                diagnostic: Some(format!(
                    "Using the OBS WebSocket password from {}",
                    path.display()
                )),
            });
        }
    }
    Ok(Resolution {
        password: None,
        diagnostic: Some(format!(
            "error: OBS_MCP_READ_OBS_CONFIG is set, but no OBS WebSocket password was found in: {}",
            paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    })
}
fn read_config(path: &Path) -> Result<Option<String>, Unsupported> {
    let Ok(file) = File::open(path) else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    if file.take(CONFIG_BYTES + 1).read_to_end(&mut bytes).is_err() {
        return Ok(None);
    }
    if bytes.len() as u64 > CONFIG_BYTES {
        return Err(Unsupported::EncodingOrSize);
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| Unsupported::EncodingOrSize)
}
pub fn resolve_environment() -> Result<Resolution, Unsupported> {
    let get = |name: &str| std::env::var(name).ok();
    resolve_password(
        get,
        || {
            // std handles the host's account fallback when HOME is absent. This
            // deprecated API remains explicit until Windows home semantics qualify.
            #[allow(deprecated)]
            let home = std::env::home_dir().ok_or(Unsupported::HomeUnavailable)?;
            Ok(config_paths(get, Platform::current(), &home))
        },
        read_config,
    )
}

/// Exact stateDirectory selection; no directories or files are created here.
/// Explicit OBS_MCP_STATE_DIR is returned verbatim, including relative paths.
pub fn state_directory(
    get: impl Fn(&str) -> Option<String>,
    platform: Platform,
    home: &Path,
) -> PathBuf {
    if let Some(path) = get("OBS_MCP_STATE_DIR").filter(|value| !value.is_empty()) {
        return path.into();
    }
    let path = match platform {
        Platform::Mac => home
            .join("Library")
            .join("Application Support")
            .join("obs-mcp"),
        Platform::Windows => get("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData").join("Local"))
            .join("obs-mcp"),
        Platform::Linux => get("XDG_STATE_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local").join("state"))
            .join("obs-mcp"),
    };
    let mut normalized = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(
                    normalized.components().next_back(),
                    Some(Component::Normal(_))
                ) {
                    normalized.pop();
                } else if !path.has_root() {
                    normalized.push("..");
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

/// Bind the selected namespace to the current working directory once. The
/// operation/journal stores separately validate approved absolute path identity.
pub fn resolve_state_root() -> Result<PathBuf, Unsupported> {
    let get = |name: &str| std::env::var(name).ok();
    let path = if let Some(path) = get("OBS_MCP_STATE_DIR").filter(|value| !value.is_empty()) {
        PathBuf::from(path)
    } else {
        #[allow(deprecated)]
        let home = std::env::home_dir().ok_or(Unsupported::HomeUnavailable)?;
        state_directory(get, Platform::current(), &home)
    };
    std::path::absolute(path).map_err(|_| Unsupported::HomeUnavailable)
}

/// Explicit process-local certificate anchors. Empty optional launcher values
/// preserve system trust; no nonempty setting is silently ignored.
pub fn additional_tls_roots(
    setting: Option<&std::ffi::OsStr>,
    enabled: bool,
) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let Some(setting) = setting.filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    if !enabled {
        return Err(ca_error());
    }
    read_tls_ca_file(Path::new(setting)).map(Some)
}
fn ca_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "OBS_MCP_TLS_CA_FILE requires an absolute safe certificate-only PEM file and an enabled TLS backend",
    )
}
/// Bounded immutable snapshot, with no certificate or path contents in errors.
pub fn read_tls_ca_file(path: &Path) -> std::io::Result<Vec<Vec<u8>>> {
    read_tls_ca_file_with(path, || {})
}
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn read_tls_ca_file_with(path: &Path, after_open: impl FnOnce()) -> std::io::Result<Vec<Vec<u8>>> {
    use std::{
        fs,
        os::unix::fs::{MetadataExt, OpenOptionsExt},
    };
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(ca_error());
    }
    let mut chain = Vec::new();
    let mut current = PathBuf::new();
    for c in path.components() {
        current.push(c);
        let m = fs::symlink_metadata(&current).map_err(|_| ca_error())?;
        if m.file_type().is_symlink() || m.mode() & 0o022 != 0 || (current != path && !m.is_dir()) {
            return Err(ca_error());
        }
        chain.push((current.clone(), m));
    }
    let before = &chain.last().ok_or_else(ca_error)?.1;
    if !before.is_file() || before.nlink() != 1 || before.len() == 0 || before.len() > CONFIG_BYTES
    {
        return Err(ca_error());
    }
    // Values from pinned libc0.2.191 target definitions, checked against SDK fcntl.h:
    // final-component O_NOFOLLOW rejects a raced symlink; O_NONBLOCK avoids a raced FIFO wait.
    #[cfg(target_os = "macos")]
    const FLAGS: Option<i32> = Some(0x20000000 | 0x4); // O_NOFOLLOW_ANY too (macOS11+).
    #[cfg(all(target_os = "linux", any(target_arch = "x86", target_arch = "x86_64")))]
    const FLAGS: Option<i32> = Some(0x20000 | 0x800);
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    const FLAGS: Option<i32> = Some(0x8000 | 0x800);
    #[cfg(all(
        target_os = "linux",
        not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))
    ))]
    const FLAGS: Option<i32> = None; // No guessed target-specific file-open ABI.
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(FLAGS.ok_or_else(ca_error)?)
        .open(path)
        .map_err(|_| ca_error())?;
    let unchanged = |a: &fs::Metadata, b: &fs::Metadata| {
        a.dev() == b.dev()
            && a.ino() == b.ino()
            && a.uid() == b.uid()
            && a.gid() == b.gid()
            && a.len() == b.len()
            && a.mtime() == b.mtime()
            && a.mtime_nsec() == b.mtime_nsec()
            && a.ctime() == b.ctime()
            && a.ctime_nsec() == b.ctime_nsec()
            && a.mode() == b.mode()
            && a.nlink() == b.nlink()
    };
    if !unchanged(before, &file.metadata().map_err(|_| ca_error())?) {
        return Err(ca_error());
    }
    after_open();
    let mut bytes = Vec::new();
    (&file)
        .take(CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ca_error())?;
    if bytes.len() as u64 != before.len()
        || !unchanged(before, &file.metadata().map_err(|_| ca_error())?)
    {
        return Err(ca_error());
    }
    for (name, metadata) in chain {
        let now = fs::symlink_metadata(name).map_err(|_| ca_error())?;
        // Directory siblings can change while a CA is read (including other
        // clients' fixtures). Bind ancestor identity/authority, not unrelated
        // directory-entry timestamps; the file itself remains fully unchanged.
        let same = if metadata.is_dir() {
            now.is_dir()
                && metadata.dev() == now.dev()
                && metadata.ino() == now.ino()
                && metadata.mode() == now.mode()
                && metadata.uid() == now.uid()
                && metadata.gid() == now.gid()
        } else {
            unchanged(&metadata, &now)
        };
        if !same {
            return Err(ca_error());
        }
    }
    parse_ca_pem(&bytes)
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_tls_ca_file_with(
    _path: &Path,
    _after_open: impl FnOnce(),
) -> std::io::Result<Vec<Vec<u8>>> {
    Err(ca_error())
}
fn parse_ca_pem(bytes: &[u8]) -> std::io::Result<Vec<Vec<u8>>> {
    use base64::Engine;
    let text = std::str::from_utf8(bytes).map_err(|_| ca_error())?;
    let mut rest = text;
    let mut certs = Vec::new();
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        if rest.is_empty() {
            break;
        }
        rest = rest
            .strip_prefix("-----BEGIN CERTIFICATE-----")
            .ok_or_else(ca_error)?;
        let (body, tail) = rest
            .split_once("-----END CERTIFICATE-----")
            .ok_or_else(ca_error)?;
        let encoded: String = body.chars().filter(|c| !c.is_ascii_whitespace()).collect();
        let der = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| ca_error())?;
        if der.is_empty() || certs.len() >= 32 {
            return Err(ca_error());
        }
        certs.push(der);
        rest = tail;
    }
    if certs.is_empty() {
        return Err(ca_error());
    }
    Ok(certs)
}
#[cfg(test)]
mod tls_ca_tests {
    use super::*;
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn strict_certificate_only_and_bounds() {
        assert_eq!(
            parse_ca_pem(b"-----BEGIN CERTIFICATE-----\nMAMCAQE=\n-----END CERTIFICATE-----")
                .unwrap(),
            vec![vec![48, 3, 2, 1, 1]]
        );
        for bad in [
            "",
            "-----BEGIN PRIVATE KEY-----a-----END PRIVATE KEY-----",
            "garbage-----BEGIN CERTIFICATE-----MAMCAQE=-----END CERTIFICATE-----",
            "-----BEGIN CERTIFICATE-----!-----END CERTIFICATE-----",
            "-----BEGIN CERTIFICATE-----MAMCAQE=-----END CERTIFICATE-----trailing",
        ] {
            assert!(parse_ca_pem(bad.as_bytes()).is_err());
        }
        let cert = "-----BEGIN CERTIFICATE-----MAMCAQE=-----END CERTIFICATE-----";
        assert!(parse_ca_pem(cert.repeat(33).as_bytes()).is_err());
    }
    #[test]
    fn optional_empty_and_disabled_are_explicit() {
        assert!(additional_tls_roots(None, false).unwrap().is_none());
        assert!(
            additional_tls_roots(Some(std::ffi::OsStr::new("")), false)
                .unwrap()
                .is_none()
        );
        assert!(additional_tls_roots(Some(std::ffi::OsStr::new("/not-read")), false).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn file_binding_replacement_and_write_races_refuse() {
        use std::{
            fs,
            os::unix::fs::{PermissionsExt, symlink},
        };
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!(".ca-test-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let cleanup = Cleanup(root.clone());
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.join("anchors.pem");
        let pem = b"-----BEGIN CERTIFICATE-----MAMCAQE=-----END CERTIFICATE-----";
        fs::write(&path, pem).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_tls_ca_file(&path).is_ok());
        assert!(
            read_tls_ca_file_with(&path, || {
                fs::write(root.join("unrelated"), b"fixture sibling").unwrap();
            })
            .is_ok()
        );
        assert!(read_tls_ca_file(Path::new("relative.pem")).is_err());
        assert!(read_tls_ca_file(&root).is_err());
        let hard = root.join("hard");
        fs::hard_link(&path, &hard).unwrap();
        assert!(read_tls_ca_file(&path).is_err());
        fs::remove_file(hard).unwrap();
        fs::write(&path, vec![b'a'; CONFIG_BYTES as usize + 1]).unwrap();
        assert!(read_tls_ca_file(&path).is_err());
        fs::write(&path, pem).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(read_tls_ca_file(&path).is_err());
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let link = root.join("link");
        symlink(&path, &link).unwrap();
        assert!(read_tls_ca_file(&link).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(read_tls_ca_file(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            read_tls_ca_file_with(&path, || {
                fs::write(&path, b"changed").unwrap();
            })
            .is_err()
        );
        fs::write(&path, pem).unwrap();
        assert!(
            read_tls_ca_file_with(&path, || {
                fs::rename(&path, root.join("old")).unwrap();
                fs::write(&path, pem).unwrap();
            })
            .is_err()
        );
        drop(cleanup);
    }
}

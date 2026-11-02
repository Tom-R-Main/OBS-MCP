// SPDX-License-Identifier: GPL-2.0-only
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

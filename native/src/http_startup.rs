// SPDX-License-Identifier: MIT
//! Transport selection without exposing configured values in diagnostics.
use std::{ffi::OsString, io};

// Deliberately not Debug/Serialize: the bearer token is opaque configuration.
pub(super) struct Configuration {
    pub(super) port: u16,
    pub(super) token: Option<String>,
}

fn invalid_port() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "OBS_MCP_HTTP_PORT requires a decimal port from 0 through 65535",
    )
}

impl Configuration {
    pub(super) fn environment() -> io::Result<Option<Self>> {
        Self::parse(std::env::var_os("OBS_MCP_HTTP_PORT"), || {
            std::env::var_os("OBS_MCP_HTTP_TOKEN")
        })
    }

    fn parse(
        port: Option<OsString>,
        token: impl FnOnce() -> Option<OsString>,
    ) -> io::Result<Option<Self>> {
        let Some(port) = port else {
            return Ok(None);
        };
        let port = port.into_string().map_err(|_| invalid_port())?;
        let port = obs_mcp_native::config::trim(&port);
        if port.is_empty() {
            return Ok(None);
        }
        // Native admission accepts decimal integers only. JavaScript Number's
        // hexadecimal/exponent/float spellings are deliberately not inferred.
        if !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid_port());
        }
        let port = port.parse().map_err(|_| invalid_port())?;
        let token = token()
            .map(|value| {
                value.into_string().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Native HTTP bearer token configuration is unavailable",
                    )
                })
            })
            .transpose()?
            .filter(|value| !value.is_empty());
        Ok(Some(Self { port, token }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_or_javascript_blank_port_selects_stdio_without_reading_http_token() {
        for port in [None, Some(""), Some("\u{feff} \t\r\n\u{feff}")] {
            assert!(
                Configuration::parse(port.map(OsString::from), || {
                    panic!("stdio must not consult HTTP credentials")
                })
                .unwrap()
                .is_none()
            );
        }
    }

    #[test]
    fn port_zero_and_decimal_bounds_preserve_the_exact_opaque_token() {
        for (input, expected) in [("0", 0), ("\u{feff}4455\u{feff}", 4455), ("65535", 65535)] {
            let config = Configuration::parse(Some(input.into()), || Some(" exact-token ".into()))
                .unwrap()
                .unwrap();
            assert_eq!(config.port, expected);
            assert_eq!(config.token.as_deref(), Some(" exact-token "));
        }
        assert!(
            Configuration::parse(Some("0".into()), || Some("".into()))
                .unwrap()
                .unwrap()
                .token
                .is_none()
        );
    }

    #[test]
    fn invalid_port_refuses_before_reading_token_without_echoing_input() {
        for value in [
            "-1",
            "+1",
            "65536",
            "1.5",
            "1e3",
            "0x10",
            "PRIVATE_VALUE",
            "\u{0085}1",
        ] {
            let failure = Configuration::parse(Some(value.into()), || {
                panic!("bad port must refuse before reading credentials")
            })
            .err()
            .expect("invalid port must refuse");
            assert_eq!(failure.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(failure.to_string(), invalid_port().to_string());
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_http_token_cannot_silently_disable_authentication() {
        use std::os::unix::ffi::OsStringExt;
        let failure =
            Configuration::parse(Some("0".into()), || Some(OsString::from_vec(vec![0xff])))
                .err()
                .expect("unreadable configured credential must refuse");
        assert_eq!(failure.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            failure.to_string(),
            "Native HTTP bearer token configuration is unavailable"
        );
    }
}

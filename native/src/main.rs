// SPDX-License-Identifier: GPL-2.0-only
// Native-only product entry. Source/SDK remains independent development oracle;
// no compatibility runtime is compiled. The fixed Rust resolver helper entry
// runs before runtime/configuration; it cannot select another executable.
mod dynamic_tools;
mod native_service;
mod prompt_catalog;
mod protocol;
mod protocol_catalog;
mod resource_catalog;
mod resource_notifications;
mod schema;
mod stdio_input;
mod stdio_output;
mod tool_filter;
use std::{ffi::OsString, io};

#[derive(Debug, PartialEq, Eq)]
enum Startup {
    Serve,
    Help,
    Version,
}
fn startup(mut args: impl Iterator<Item = OsString>) -> io::Result<Startup> {
    let Some(first) = args.next() else {
        return Ok(Startup::Serve);
    };
    if args.next().is_none() {
        if first == "--help" {
            return Ok(Startup::Help);
        }
        if first == "--version" {
            return Ok(Startup::Version);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Unsupported arguments; use --help or configure the native service through environment variables",
    ))
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run() -> io::Result<()> {
    match startup(std::env::args_os().skip(1))? {
        Startup::Help => {
            println!(
                "OBS MCP native {}\nUsage: obs-mcp-native [--help | --version]\n\nWith no arguments, run the Rust MCP service configured through environment variables.\nOBS_MCP_NATIVE_STATEFUL is retired; the service is always native.\nOBS_MCP_FORCE_COMPAT requests are refused; OBS_MCP_COMPAT_NODE and OBS_MCP_COMPAT_ENTRY have no effect.\nSee docs/native-distribution.md for profiles, media prerequisites and platform qualification.",
                env!("CARGO_PKG_VERSION")
            );
            Ok(())
        }
        Startup::Version => {
            println!("obs-mcp-native {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Startup::Serve => {
            if std::env::var("OBS_MCP_FORCE_COMPAT").is_ok_and(|value| tool_filter::enabled(&value))
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Node compatibility mode is unavailable in the native-only distribution",
                ));
            }
            native_service::run().await
        }
    }
}
fn main() -> std::process::ExitCode {
    if let Some(code) = obs_mcp_resolver::internal_entry() {
        return code;
    }
    runtime_main();
    std::process::ExitCode::SUCCESS
}
#[tokio::main(flavor = "current_thread")]
async fn runtime_main() {
    if let Err(error) = run().await {
        eprintln!("OBS MCP native: {error}");
        std::process::exit(1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_arguments_always_select_native_without_legacy_mode_switches() {
        assert_eq!(startup(std::iter::empty()).unwrap(), Startup::Serve);
        assert_eq!(
            startup([OsString::from("--help")].into_iter()).unwrap(),
            Startup::Help
        );
        assert_eq!(
            startup([OsString::from("--version")].into_iter()).unwrap(),
            Startup::Version
        );
        for args in [
            vec!["--http"],
            vec!["--help", "--version"],
            vec!["--port", "secret-value"],
        ] {
            let error = startup(args.into_iter().map(OsString::from)).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(!error.to_string().contains("secret-value"));
        }
    }
}

// SPDX-License-Identifier: GPL-2.0-only
mod compat;
mod dynamic_tools;
mod native_service;
mod prompt_catalog;
mod protocol;
mod protocol_catalog;
mod read_workflow;
mod resource_catalog;
mod schema;
mod stdio_output;
mod tool_filter;

use std::{io, time::Duration};

use compat::{Frames, write_frame};
use obs_mcp_native::obs::ObsClient;
use protocol::{Admission, ProtocolSession};
use read_workflow::ReadWorkflowRequest;
use serde_json::{Value, json};
use tokio::io::BufReader;
use tool_filter::ToolFilter;

fn needs_compatibility(get: impl Fn(&str) -> Option<String>) -> bool {
    let enabled = |name: &str| get(name).is_some_and(|value| tool_filter::enabled(&value));
    [
        "OBS_MCP_FORCE_COMPAT",
        "OBS_MCP_DYNAMIC_TOOLSETS",
        "OBS_MCP_CONFIRM_LIVE",
    ]
    .iter()
    .any(|name| enabled(name))
        || get("OBS_MCP_HTTP_TOKEN").is_some_and(|value| !value.is_empty())
        // Keep encrypted endpoints on the qualified SDK owner until a compatible
        // native TLS backend is qualified. Never downgrade wss to plaintext.
        || get("OBS_WEBSOCKET_URL")
            .is_some_and(|value| value.trim().to_ascii_lowercase().starts_with("wss://"))
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

async fn send(output: &mut tokio::io::Stdout, response: &Value) -> io::Result<()> {
    write_frame(
        output,
        &serde_json::to_vec(response).map_err(io::Error::other)?,
    )
    .await
}

async fn handoff<F: std::future::Future<Output = ()>>(
    mut obs: ObsClient,
    input: &mut Frames<BufReader<tokio::io::Stdin>>,
    output: &mut tokio::io::Stdout,
    history: &[Vec<u8>],
    initial: Vec<Vec<u8>>,
    stop: std::pin::Pin<&mut F>,
) -> io::Result<()> {
    let _ = tokio::time::timeout(Duration::from_secs(1), obs.disconnect()).await;
    drop(obs); // No native WebSocket remains when the compatibility child starts.
    compat::run(input, output, history, initial, stop).await
}

async fn run() -> io::Result<()> {
    if std::env::var("OBS_MCP_NATIVE_STATEFUL").is_ok_and(|value| tool_filter::enabled(&value)) {
        return native_service::run().await;
    }
    if std::env::args_os().len() > 1
        || std::env::var_os("OBS_MCP_HTTP_PORT").is_some_and(|value| !value.is_empty())
        || std::env::var_os("OBS_MCP_MAX_SCREENSHOT_BYTES").is_some_and(|value| !value.is_empty())
    {
        return compat::exec();
    }
    let mut input = Frames::new(BufReader::new(tokio::io::stdin()));
    let mut output = tokio::io::stdout();
    let stop = shutdown_signal();
    tokio::pin!(stop);
    if needs_compatibility(|name| std::env::var(name).ok()) {
        return compat::run(&mut input, &mut output, &[], Vec::new(), stop.as_mut()).await;
    }
    let filter = ToolFilter::parse(|name| std::env::var(name).ok(), schema::catalog())
        .map_err(io::Error::other)?;
    let url = std::env::var("OBS_WEBSOCKET_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "ws://localhost:4455".into());
    let resolved = match obs_mcp_native::config::resolve_environment() {
        Ok(resolved) => resolved,
        Err(_) => {
            return compat::run(&mut input, &mut output, &[], Vec::new(), stop.as_mut()).await;
        }
    };
    if let Some(message) = resolved.diagnostic() {
        eprintln!("[obs-mcp] {message}");
    }
    let password = resolved.password().map(str::to_owned);
    let mut obs = ObsClient::new(url.clone(), password.clone(), Duration::from_secs(10));
    let mut protocol = ProtocolSession::default();
    loop {
        let frame = tokio::select! { biased; () = &mut stop => break, next = input.next() => match next? { Some(frame) => frame, None => break } };
        if frame.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value: Value = match serde_json::from_slice(&frame) {
            Ok(value) => value,
            Err(_) => {
                return handoff(
                    obs,
                    &mut input,
                    &mut output,
                    protocol.history(),
                    vec![frame],
                    stop.as_mut(),
                )
                .await;
            }
        };
        let (reply, name, arguments, resource_list) = match protocol.admit(&value, &frame) {
            Admission::Reply(response) => {
                send(&mut output, &response).await?;
                continue;
            }
            Admission::List(reply) => {
                send(&mut output, &reply.complete(filter.manifest(), true)).await?;
                continue;
            }
            Admission::Initialized => continue,
            Admission::ResourceList(reply) => (reply, "", json!({}), true),
            Admission::Call {
                reply,
                name,
                arguments,
            } => (reply, name, arguments, false),
            Admission::Compatibility
            | Admission::Listen { .. }
            | Admission::ResourceRead { .. }
            | Admission::ResourceCompletion { .. } => {
                return handoff(
                    obs,
                    &mut input,
                    &mut output,
                    protocol.history(),
                    vec![frame],
                    stop.as_mut(),
                )
                .await;
            }
        };
        if !resource_list && !filter.includes(name) {
            // The SDK retains the exact hidden-tool refusal projection. Hand
            // over unchanged filter configuration before any native dispatch.
            return handoff(
                obs,
                &mut input,
                &mut output,
                protocol.history(),
                vec![frame],
                stop.as_mut(),
            )
            .await;
        }
        // Explicitly admitted offline catalog inspection; this route cannot
        // touch OBS and still passes through static tool visibility and schema
        // validation. Invalid arguments retain the SDK's error projection.
        if name == "obs-describe-request"
            && let Some(arguments) =
                schema::validated_local_arguments(name, "describe-request", &arguments)
        {
            send(
                &mut output,
                &reply.complete(protocol_catalog::describe(&arguments), false),
            )
            .await?;
            continue;
        }
        if let Some(request) = ReadWorkflowRequest::parse(name, &arguments)
            && schema::workflow_declared(name, request.handler())
        {
            // The fast socket is closed before opening a subscribed workflow
            // session. Keep cleanup ownership outside the cancellable future.
            obs.disconnect().await;
            let mut workflow = read_workflow::ReadWorkflow::new(url.clone(), password.clone());
            enum WorkflowOutcome {
                Completed(Result<Value, ()>),
                Incoming(io::Result<Option<Vec<u8>>>),
                Stop,
            }
            let outcome = {
                let work = workflow.run(&request);
                tokio::pin!(work);
                tokio::select! {
                    biased;
                    () = &mut stop => WorkflowOutcome::Stop,
                    frame = input.next() => WorkflowOutcome::Incoming(frame),
                    response = &mut work => WorkflowOutcome::Completed(response),
                }
            };
            // Completion, cancellation, EOF, concurrent input and errors all
            // join both native owners before sending a result or SDK handoff.
            workflow.cleanup().await?;
            match outcome {
                WorkflowOutcome::Stop | WorkflowOutcome::Incoming(Ok(None)) => break,
                WorkflowOutcome::Incoming(Err(error)) => return Err(error),
                WorkflowOutcome::Completed(Ok(result))
                    if schema::valid_response(name, &result["structuredContent"]) =>
                {
                    send(&mut output, &reply.complete(result, false)).await?;
                    continue;
                }
                WorkflowOutcome::Completed(_) => {
                    return handoff(
                        obs,
                        &mut input,
                        &mut output,
                        protocol.history(),
                        vec![frame],
                        stop.as_mut(),
                    )
                    .await;
                }
                WorkflowOutcome::Incoming(Ok(Some(next))) => {
                    let initial = if reply.cancelled_by(&next) {
                        vec![next]
                    } else {
                        vec![frame, next]
                    };
                    return handoff(
                        obs,
                        &mut input,
                        &mut output,
                        protocol.history(),
                        initial,
                        stop.as_mut(),
                    )
                    .await;
                }
            }
        }
        let (request_type, data) = if resource_list {
            ("GetSceneList".to_owned(), json!({}))
        } else {
            match schema::native_request(name, &arguments) {
                Ok(Some(request)) => request,
                _ => {
                    return handoff(
                        obs,
                        &mut input,
                        &mut output,
                        protocol.history(),
                        vec![frame],
                        stop.as_mut(),
                    )
                    .await;
                }
            }
        };
        if !request_type.starts_with("Get") || request_type == "GetInputPropertiesListPropertyItems"
        {
            return handoff(
                obs,
                &mut input,
                &mut output,
                protocol.history(),
                vec![frame],
                stop.as_mut(),
            )
            .await;
        }
        enum ReadOutcome {
            Completed(Result<Value, String>),
            Incoming(io::Result<Option<Vec<u8>>>),
            Stop,
        }
        let outcome = {
            let request = obs.request(&request_type, data);
            tokio::pin!(request);
            tokio::select! {
                biased;
                () = &mut stop => ReadOutcome::Stop,
                frame = input.next() => ReadOutcome::Incoming(frame),
                response = &mut request => ReadOutcome::Completed(response),
            }
        };
        match outcome {
            ReadOutcome::Stop | ReadOutcome::Incoming(Ok(None)) => break,
            ReadOutcome::Incoming(Err(error)) => return Err(error),
            ReadOutcome::Incoming(Ok(Some(next))) => {
                let cancelled = reply.cancelled_by(&next);
                // Every native call is read-only. Concurrent requests move the whole session to
                // the SDK; only the unanswered read is replayed, never a mutation.
                let initial = if cancelled {
                    vec![next]
                } else {
                    vec![frame, next]
                };
                return handoff(
                    obs,
                    &mut input,
                    &mut output,
                    protocol.history(),
                    initial,
                    stop.as_mut(),
                )
                .await;
            }
            ReadOutcome::Completed(Ok(response)) => {
                if resource_list {
                    if let Some(result) = resource_catalog::listing(&response) {
                        send(&mut output, &reply.complete(result, true)).await?;
                        continue;
                    }
                    return handoff(
                        obs,
                        &mut input,
                        &mut output,
                        protocol.history(),
                        vec![frame],
                        stop.as_mut(),
                    )
                    .await;
                }
                let response = if response.is_object() {
                    response
                } else {
                    json!({})
                };
                if !schema::valid_response(name, &response) {
                    return handoff(
                        obs,
                        &mut input,
                        &mut output,
                        protocol.history(),
                        vec![frame],
                        stop.as_mut(),
                    )
                    .await;
                }
                let result = json!({"content":[{"type":"text","text":serde_json::to_string_pretty(&response).map_err(io::Error::other)?}],"structuredContent":response});
                send(&mut output, &reply.complete(result, false)).await?;
            }
            ReadOutcome::Completed(Err(_)) => {
                return handoff(
                    obs,
                    &mut input,
                    &mut output,
                    protocol.history(),
                    vec![frame],
                    stop.as_mut(),
                )
                .await;
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), obs.disconnect()).await;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("OBS MCP native: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_host_defaults_keep_the_native_front_enabled() {
        let values = [
            ("OBS_MCP_TOOLSETS", "all"),
            ("OBS_MCP_READ_ONLY", "false"),
            ("OBS_MCP_DYNAMIC_TOOLSETS", "0"),
            ("OBS_MCP_CONFIRM_LIVE", "no"),
            ("OBS_MCP_READ_OBS_CONFIG", "false"),
        ];
        assert!(!needs_compatibility(|name| values
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_owned())));
        assert!(!needs_compatibility(
            |name| (name == "OBS_MCP_TOOLSETS").then(|| " all, scenes ".into())
        ));
        assert!(needs_compatibility(
            |name| (name == "OBS_MCP_CONFIRM_LIVE").then(|| " YES ".into())
        ));
        assert!(!needs_compatibility(
            |name| (name == "OBS_MCP_TOOLSETS").then(|| "core".into())
        ));
        assert!(!needs_compatibility(
            |name| (name == "OBS_MCP_READ_ONLY").then(|| "true".into())
        ));
        assert!(needs_compatibility(|name| (name
            == "OBS_MCP_DYNAMIC_TOOLSETS")
            .then(|| "true".into())));
        assert!(needs_compatibility(
            |name| (name == "OBS_WEBSOCKET_URL").then(|| " WSS://localhost:4455 ".into())
        ));
        assert!(!needs_compatibility(
            |name| (name == "OBS_WEBSOCKET_URL").then(|| "ws://localhost:4455".into())
        ));
    }
}

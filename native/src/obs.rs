// SPDX-License-Identifier: GPL-2.0-only
//! Exclusive, bounded read-only OBS v5 session. No events, mutation or replay.
use crate::identity::{Generation, IdentityClock, RequestIdentity};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, time::Duration};
use tokio::{
    net::TcpStream,
    time::{Instant, timeout, timeout_at},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

pub(crate) const MAX_BYTES: usize = 8 * 1024 * 1024;
// The supported 6MiB decoded screenshot expands to exactly 8MiB in base64.
// Its data URL and OBS response envelope need bounded additional room. This
// receive-only allowance does not enlarge queued requests or event permits.
pub(crate) const MAX_INCOMING_BYTES: usize = MAX_BYTES + 64 * 1024;
pub(crate) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Session {
    socket: Socket,
    generation: Generation,
    available: Option<HashSet<String>>,
}

/// The caller serializes requests and disconnects before permanent TS handoff.
/// A request owns the socket until success, so dropping its future also closes
/// a possibly dispatched operation instead of leaving an uncertain session live.
pub struct ObsClient {
    url: String,
    password: Option<String>,
    timeout: Duration,
    identities: IdentityClock,
    session: Option<Session>,
}

impl ObsClient {
    pub fn new(url: String, password: Option<String>, timeout: Duration) -> Self {
        Self {
            url,
            password,
            timeout,
            identities: IdentityClock::default(),
            session: None,
        }
    }

    pub async fn request(&mut self, request_type: &str, data: Value) -> Result<Value, String> {
        // The front end additionally checks its exact generated eligibility map.
        // Nullable native property enumeration must always use the TS guards.
        if !request_type.starts_with("Get")
            || request_type.len() > 128
            || !request_type.bytes().all(|b| b.is_ascii_alphanumeric())
            || request_type == "GetInputPropertiesListPropertyItems"
        {
            return Err("Request is unavailable on the native read-only transport".into());
        }
        if self.timeout.is_zero() {
            return Err("OBS read deadline expired; no automatic retry was made".into());
        }
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or("OBS read deadline is invalid")?;
        let session = self.session.take();
        // Taking the session before admission ensures allocator failure also closes
        // the old socket. The allocator cannot wrap or recover after retirement.
        let identity = self.identities.admit(session.is_none())?;
        let generation = identity.generation();
        let work = async {
            let mut session = match session {
                Some(session) => session,
                None => connect(&self.url, self.password.as_deref(), generation, deadline).await?,
            };
            if session.generation != generation {
                return Err("OBS connection changed before dispatch".into());
            }
            if session
                .available
                .as_ref()
                .is_some_and(|set| !set.contains(request_type))
            {
                return Err(format!(
                    "OBS does not advertise support for request '{request_type}'"
                ));
            }
            let response = session.rpc(identity, request_type, data, deadline).await?;
            check_deadline(deadline)?;
            Ok((session, response))
        };
        match timeout_at(deadline, work).await {
            Ok(Ok((session, response))) => {
                self.session = Some(session);
                Ok(response)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err("OBS read deadline expired; no automatic retry was made".into()),
        }
    }

    /// Await before starting the sole TypeScript child. No reader task survives.
    pub async fn disconnect(&mut self) {
        if let Some(mut session) = self.session.take() {
            let _ = timeout(Duration::from_millis(250), async {
                session.socket.close(None).await.ok();
                while let Some(message) = session.socket.next().await {
                    if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                        break;
                    }
                }
            })
            .await;
            // Dropping the stream closes it even when the peer ignores Close.
        }
    }
}

fn check_deadline(deadline: Instant) -> Result<(), String> {
    if Instant::now() >= deadline {
        Err("OBS read deadline expired; no automatic retry was made".into())
    } else {
        Ok(())
    }
}

async fn send(socket: &mut Socket, message: Value, deadline: Instant) -> Result<(), String> {
    check_deadline(deadline)?;
    let text = serde_json::to_string(&message).map_err(|_| "OBS request could not be encoded")?;
    if text.len() > MAX_BYTES {
        return Err("OBS request exceeds the native byte limit".into());
    }
    check_deadline(deadline)?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| "OBS socket send failed".into())
}

async fn receive(socket: &mut Socket, deadline: Instant) -> Result<Value, String> {
    loop {
        check_deadline(deadline)?;
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                let value: Value =
                    serde_json::from_str(text.as_str()).map_err(|_| "Malformed OBS message")?;
                if !value.is_object() || !value["op"].is_u64() || !value["d"].is_object() {
                    return Err("Malformed OBS message envelope".into());
                }
                check_deadline(deadline)?;
                return Ok(value);
            }
            Some(Ok(Message::Ping(_))) => {
                socket
                    .flush()
                    .await
                    .map_err(|_| "OBS socket control write failed")?;
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) | None => {
                return Err("OBS connection closed; no automatic retry was made".into());
            }
            Some(Err(_)) => return Err("OBS socket read failed or exceeded its byte limit".into()),
            Some(Ok(_)) => return Err("Unsupported OBS message encoding".into()),
        }
    }
}

/// Reused by the stateful actor and the qualified read-only predecessor. This
/// helper authenticates only; callers retain their own RPC and event owners.
pub(crate) async fn open_socket(
    url: &str,
    password: Option<&str>,
    event_subscriptions: u64,
    deadline: Instant,
) -> Result<Socket, String> {
    let config = WebSocketConfig::default()
        .read_buffer_size(64 * 1024)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(MAX_BYTES + 16 * 1024)
        .max_message_size(Some(MAX_INCOMING_BYTES))
        .max_frame_size(Some(MAX_INCOMING_BYTES));
    let (mut socket, _) = connect_async_with_config(url, Some(config), false)
        .await
        .map_err(|_| "Unable to connect to OBS WebSocket")?;
    let hello = receive(&mut socket, deadline).await?;
    if hello["op"] != 0
        || hello["d"]["obsWebSocketVersion"].as_str().is_none()
        || hello["d"]["rpcVersion"].as_u64().is_none_or(|v| v < 1)
    {
        return Err("Invalid OBS v5 Hello".into());
    }
    let mut identify =
        json!({"op": 1, "d": {"rpcVersion": 1, "eventSubscriptions": event_subscriptions}});
    if let Some(authentication) = hello["d"].get("authentication") {
        let salt = authentication["salt"]
            .as_str()
            .ok_or("Invalid OBS authentication challenge")?;
        let challenge = authentication["challenge"]
            .as_str()
            .ok_or("Invalid OBS authentication challenge")?;
        let password = password
            .filter(|p| !p.is_empty())
            .ok_or("OBS requires a password; configure it or use the compatibility route")?;
        let secret = STANDARD.encode(Sha256::digest(format!("{password}{salt}")));
        identify["d"]["authentication"] =
            Value::String(STANDARD.encode(Sha256::digest(format!("{secret}{challenge}"))));
    }
    send(&mut socket, identify, deadline).await?;
    loop {
        let message = receive(&mut socket, deadline).await?;
        if message["op"] == 2 {
            if message["d"]["negotiatedRpcVersion"] != 1 {
                return Err("Unsupported OBS RPC version".into());
            }
            break;
        }
        if message["op"] == 0 {
            return Err("OBS connection changed during identification".into());
        }
    }
    Ok(socket)
}

async fn connect(
    url: &str,
    password: Option<&str>,
    generation: Generation,
    deadline: Instant,
) -> Result<Session, String> {
    let socket = open_socket(url, password, 0, deadline).await?;
    let mut session = Session {
        socket,
        generation,
        available: None,
    };
    let version = session
        .rpc(
            RequestIdentity::capabilities(generation),
            "GetVersion",
            Value::Null,
            deadline,
        )
        .await?;
    if !version.is_object() {
        return Err("Malformed OBS capability response".into());
    }
    session.available = version["availableRequests"].as_array().map(|requests| {
        requests
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    });
    Ok(session)
}

impl Session {
    async fn rpc(
        &mut self,
        identity: RequestIdentity,
        request_type: &str,
        data: Value,
        deadline: Instant,
    ) -> Result<Value, String> {
        if self.generation != identity.generation() {
            return Err("OBS connection changed before dispatch".into());
        }
        let request_id = identity.to_string();
        let mut message =
            json!({"op": 6, "d": {"requestId": request_id, "requestType": request_type}});
        if !data.is_null() {
            message["d"]["requestData"] = data;
        }
        send(&mut self.socket, message, deadline).await?;
        loop {
            let message = receive(&mut self.socket, deadline).await?;
            if message["op"] == 0 || message["op"] == 2 {
                return Err("OBS session changed while awaiting a reply".into());
            }
            if message["op"] != 7 || message["d"]["requestId"].as_str() != Some(request_id.as_str())
            {
                continue;
            }
            let response = &message["d"];
            if response["requestType"].as_str() != Some(request_type) {
                return Err("OBS reply request identity mismatch".into());
            }
            let code = response["requestStatus"]["code"]
                .as_u64()
                .ok_or("Malformed OBS reply status")?;
            match response["requestStatus"]["result"].as_bool() {
                Some(true) => {
                    return Ok(response
                        .get("responseData")
                        .filter(|v| !v.is_null())
                        .cloned()
                        .unwrap_or_else(|| json!({})));
                }
                Some(false) => {
                    return Err(format!(
                        "OBS request {request_type} failed with code {code}"
                    ));
                }
                None => return Err("Malformed OBS reply status".into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;
    type Peer = WebSocketStream<TcpStream>;

    async fn listener() -> (String, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        (format!("ws://{}", listener.local_addr().unwrap()), listener)
    }

    async fn peer_json(peer: &mut Peer) -> Value {
        let message = timeout(Duration::from_secs(2), peer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    async fn peer_send(peer: &mut Peer, message: Value) {
        peer.send(Message::Text(message.to_string().into()))
            .await
            .unwrap();
    }

    async fn reply(peer: &mut Peer, request: &Value, data: Value) {
        peer_send(
            peer,
            json!({"op": 7, "d": {
                "requestId": request["d"]["requestId"], "requestType": request["d"]["requestType"],
                "requestStatus": {"result": true, "code": 100}, "responseData": data
            }}),
        )
        .await;
    }

    async fn hello(peer: &mut Peer, auth: bool) {
        let mut hello = json!({"op": 0, "d": {"obsWebSocketVersion": "5.7.0", "rpcVersion": 1}});
        if auth {
            hello["d"]["authentication"] =
                json!({"salt": "fixture-salt", "challenge": "fixture-challenge"});
        }
        peer_send(peer, hello).await;
    }

    async fn handshake(peer: &mut Peer, requests: &[&str], auth: bool) {
        hello(peer, auth).await;
        let identify = peer_json(peer).await;
        assert_eq!(identify["op"], 1);
        assert_eq!(identify["d"]["eventSubscriptions"], 0);
        assert_eq!(identify["d"]["rpcVersion"], 1);
        if auth {
            // Independent Python hashlib/base64 fixture, not computed by this helper.
            assert_eq!(
                identify["d"]["authentication"],
                "c2JC/0iz1OnAFlxoRN7VPCl01BBUOZN+YaBU0/FEYwM="
            );
        } else {
            assert!(identify["d"].get("authentication").is_none());
        }
        peer_send(peer, json!({"op": 2, "d": {"negotiatedRpcVersion": 1}})).await;
        let capabilities = peer_json(peer).await;
        assert_eq!(capabilities["d"]["requestType"], "GetVersion");
        reply(
            peer,
            &capabilities,
            json!({"availableRequests": requests, "obsVersion": "32.2.2"}),
        )
        .await;
    }

    async fn closed(peer: &mut Peer) {
        let message = timeout(Duration::from_secs(2), peer.next()).await.unwrap();
        assert!(
            matches!(message, Some(Ok(Message::Close(_))) | Some(Err(_)) | None),
            "unexpected message after request failure"
        );
    }

    #[tokio::test]
    async fn authenticates_without_events_reuses_one_socket_and_reads_fresh_public_version() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            handshake(&mut peer, &["GetVersion", "GetStats"], true).await;
            let first = peer_json(&mut peer).await;
            assert_eq!(first["d"]["requestType"], "GetStats");
            assert_eq!(first["d"]["requestData"], json!({"scope": "fixture"}));
            reply(&mut peer, &first, json!({"activeFps": 60})).await;
            let version = peer_json(&mut peer).await;
            assert_eq!(version["d"]["requestType"], "GetVersion");
            assert_ne!(version["d"]["requestId"], first["d"]["requestId"]);
            reply(
                &mut peer,
                &version,
                json!({"obsVersion": "fresh-public-result"}),
            )
            .await;
            closed(&mut peer).await;
        });
        let mut client =
            ObsClient::new(url, Some("fixture-password".into()), Duration::from_secs(1));
        assert!(client.session.is_none());
        assert_eq!(
            client
                .request("GetStats", json!({"scope": "fixture"}))
                .await
                .unwrap(),
            json!({"activeFps": 60})
        );
        assert_eq!(
            client.request("GetVersion", Value::Null).await.unwrap(),
            json!({"obsVersion": "fresh-public-result"})
        );
        assert_eq!(
            client
                .identities
                .admit(false)
                .unwrap()
                .generation()
                .to_string(),
            "1"
        );
        client.disconnect().await;
        assert!(client.session.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn refuses_mutations_and_guarded_property_enumeration_without_connecting() {
        let mut client = ObsClient::new(
            "invalid-url-must-not-be-used".into(),
            None,
            Duration::from_secs(1),
        );
        for request in [
            "StartRecord",
            "SetInputSettings",
            "CallVendorRequest",
            "GetInputPropertiesListPropertyItems",
            "GetBad\nName",
        ] {
            assert!(
                client
                    .request(request, Value::Null)
                    .await
                    .unwrap_err()
                    .contains("read-only transport")
            );
        }
        assert!(client.session.is_none());
        assert!(client.session.is_none());
    }

    #[tokio::test]
    async fn refuses_absent_advertised_method_before_sending_it() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            handshake(&mut peer, &["GetVersion"], false).await;
            closed(&mut peer).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_secs(1));
        assert!(
            client
                .request("GetStats", Value::Null)
                .await
                .unwrap_err()
                .contains("does not advertise")
        );
        assert!(client.session.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_generation_mismatch_before_another_socket_send() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            handshake(&mut peer, &["GetStats"], false).await;
            let first = peer_json(&mut peer).await;
            reply(&mut peer, &first, json!({})).await;
            closed(&mut peer).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_secs(1));
        client.request("GetStats", Value::Null).await.unwrap();
        client.identities.admit(true).unwrap();
        assert!(
            client
                .request("GetStats", Value::Null)
                .await
                .unwrap_err()
                .contains("connection changed before dispatch")
        );
        assert!(client.session.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_oversized_request_before_its_dispatch() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            handshake(&mut peer, &["GetStats"], false).await;
            closed(&mut peer).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_secs(2));
        assert!(
            client
                .request("GetStats", json!({"data": "x".repeat(MAX_BYTES)}))
                .await
                .unwrap_err()
                .contains("byte limit")
        );
        assert!(client.session.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn drops_unmatched_replies_and_unsolicited_events() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            handshake(&mut peer, &["GetStats"], false).await;
            let request = peer_json(&mut peer).await;
            peer_send(&mut peer, json!({"op": 7, "d": {"requestId": "obsolete"}})).await;
            peer_send(
                &mut peer,
                json!({"op": 5, "d": {"eventType": "RecordStateChanged"}}),
            )
            .await;
            reply(&mut peer, &request, json!({"current": true})).await;
            closed(&mut peer).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_secs(1));
        assert_eq!(
            client.request("GetStats", Value::Null).await.unwrap(),
            json!({"current": true})
        );
        client.disconnect().await;
        task.await.unwrap();
    }

    #[tokio::test]
    async fn lost_reply_is_one_dispatch_and_old_socket_cannot_complete_new_session() {
        let (url, listener) = listener().await;
        let dispatched = Arc::new(AtomicUsize::new(0));
        let calls = dispatched.clone();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut old = accept_async(stream).await.unwrap();
            handshake(&mut old, &["GetStats"], false).await;
            let first = peer_json(&mut old).await;
            calls.fetch_add(1, Ordering::SeqCst);
            // No response. A separate user read, after the timeout, may connect.
            let (stream, _) = listener.accept().await.unwrap();
            let mut current = accept_async(stream).await.unwrap();
            handshake(&mut current, &["GetStats"], false).await;
            let second = peer_json(&mut current).await;
            calls.fetch_add(1, Ordering::SeqCst);
            assert_ne!(first["d"]["requestId"], second["d"]["requestId"]);
            let forged = json!({"op": 7, "d": {"requestId": second["d"]["requestId"], "requestType": "GetStats", "requestStatus": {"result": true, "code": 100}, "responseData": {"stale": true}}});
            let _ = old.send(Message::Text(forged.to_string().into())).await;
            reply(&mut current, &second, json!({"current": true})).await;
            closed(&mut current).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_millis(80));
        assert!(
            client
                .request("GetStats", Value::Null)
                .await
                .unwrap_err()
                .contains("deadline")
        );
        assert_eq!(dispatched.load(Ordering::SeqCst), 1);
        assert!(client.session.is_none());
        assert_eq!(
            client
                .identities
                .admit(false)
                .unwrap()
                .generation()
                .to_string(),
            "1"
        );
        assert_eq!(
            client.request("GetStats", Value::Null).await.unwrap(),
            json!({"current": true})
        );
        assert_eq!(dispatched.load(Ordering::SeqCst), 2);
        assert_eq!(
            client
                .identities
                .admit(false)
                .unwrap()
                .generation()
                .to_string(),
            "2"
        );
        client.disconnect().await;
        task.await.unwrap();
    }

    #[tokio::test]
    async fn caller_cancellation_drops_the_owned_socket() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            handshake(&mut peer, &["GetStats"], false).await;
            let _ = peer_json(&mut peer).await;
            closed(&mut peer).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_secs(1));
        assert!(
            timeout(
                Duration::from_millis(80),
                client.request("GetStats", Value::Null)
            )
            .await
            .is_err()
        );
        assert!(client.session.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn deadline_covers_connect_handshake_and_capability_discovery() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            hello(&mut peer, false).await;
            let _ = peer_json(&mut peer).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            peer_send(
                &mut peer,
                json!({"op": 2, "d": {"negotiatedRpcVersion": 1}}),
            )
            .await;
            let request = peer_json(&mut peer).await;
            assert_eq!(request["d"]["requestType"], "GetVersion");
            // A fresh timeout for each phase would incorrectly allow 200+200ms.
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = peer.send(Message::Text(json!({"op": 7, "d": {"requestId": request["d"]["requestId"], "requestType": "GetVersion", "requestStatus": {"result": true, "code": 100}, "responseData": {"availableRequests": ["GetStats"]}}}).to_string().into())).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_millis(300));
        assert!(
            client
                .request("GetStats", Value::Null)
                .await
                .unwrap_err()
                .contains("deadline")
        );
        assert!(client.session.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_malformed_or_oversized_frames_without_echoing_private_data() {
        for oversized in [false, true] {
            let (url, listener) = listener().await;
            let task = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut peer = accept_async(stream).await.unwrap();
                handshake(&mut peer, &["GetStats"], false).await;
                let request = peer_json(&mut peer).await;
                let data = if oversized {
                    json!({"op":7,"d":{"requestId":request["d"]["requestId"],"requestType":"GetStats","requestStatus":{"result":true,"code":100},"responseData":{"padding":"x".repeat(MAX_INCOMING_BYTES)}}}).to_string()
                } else {
                    "PRIVATE-PAYLOAD-not-json".into()
                };
                let _ = peer.send(Message::Text(data.into())).await;
                closed(&mut peer).await;
            });
            let mut client = ObsClient::new(
                format!("{url}/?token=PRIVATE-URL"),
                None,
                Duration::from_secs(1),
            );
            let error = client.request("GetStats", Value::Null).await.unwrap_err();
            assert!(!error.contains("PRIVATE"));
            assert!(client.session.is_none());
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn explicit_obs_errors_preserve_code_without_echoing_comment_secrets() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            handshake(&mut peer, &["GetStats"], false).await;
            let request = peer_json(&mut peer).await;
            peer_send(&mut peer, json!({"op": 7, "d": {"requestId": request["d"]["requestId"], "requestType": "GetStats", "requestStatus": {"result": false, "code": 501, "comment": "PRIVATE-PAYLOAD"}}})).await;
            closed(&mut peer).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_secs(1));
        let error = client.request("GetStats", Value::Null).await.unwrap_err();
        assert_eq!(error, "OBS request GetStats failed with code 501");
        assert!(client.session.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn missing_password_is_refused_before_identify_or_any_rpc() {
        let (url, listener) = listener().await;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            hello(&mut peer, true).await;
            closed(&mut peer).await;
        });
        let mut client = ObsClient::new(url, None, Duration::from_secs(1));
        assert!(
            client
                .request("GetStats", Value::Null)
                .await
                .unwrap_err()
                .contains("requires a password")
        );
        assert!(client.session.is_none());
        task.await.unwrap();
    }
}

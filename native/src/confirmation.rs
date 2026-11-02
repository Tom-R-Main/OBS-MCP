// SPDX-License-Identifier: GPL-2.0-only
//! Inert, one-shot request continuations. T must contain only a prepared call,
//! never a dispatched future, receipt loan, socket, or effect owner.
use obs_mcp_native::control::Disposition;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, time::Duration};
use tokio::time::Instant;
const MAX_ID: u64 = 9_007_199_254_740_991;
#[derive(Clone, Copy)]
pub(super) struct Bounds {
    pub items: usize,
    pub bytes: usize,
    pub lifetime: Duration,
}
impl Default for Bounds {
    fn default() -> Self {
        Self {
            items: 16,
            bytes: 8 * 1024 * 1024,
            lifetime: Duration::from_secs(60),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Challenge {
    Modern(String),
    Legacy(u64),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Refusal {
    Capacity,
    Identity,
    InvalidState,
    Expired,
}
struct Pending<T> {
    original: Value,
    binding: [u8; 32],
    challenge: Challenge,
    deadline: Instant,
    bytes: usize,
    call: T,
}
pub(super) struct Removed<T> {
    pub challenge: Challenge,
    pub call: T,
}
pub(super) struct Continuations<T> {
    nonce: String,
    next: Option<u64>,
    bounds: Bounds,
    bytes: usize,
    pending: HashMap<String, Pending<T>>,
}
impl<T> Continuations<T> {
    pub fn disabled() -> Self {
        let mut owner = Self::with_nonce(
            String::new(),
            Bounds {
                items: 0,
                bytes: 0,
                ..Bounds::default()
            },
        );
        owner.next = None;
        owner
    }
    pub fn new() -> Result<Self, Refusal> {
        let nonce = obs_mcp_native::runtime_identity::uuid_v4().map_err(|_| Refusal::Identity)?;
        Ok(Self::with_nonce(nonce, Bounds::default()))
    }
    fn with_nonce(nonce: String, bounds: Bounds) -> Self {
        Self {
            nonce,
            next: Some(0),
            bounds,
            bytes: 0,
            pending: HashMap::new(),
        }
    }
    pub fn contains_original(&self, id: &Value) -> bool {
        self.pending.values().any(|entry| &entry.original == id)
    }
    pub fn issue(
        &mut self,
        original: Value,
        binding: [u8; 32],
        call: T,
        retained: usize,
        legacy: bool,
        now: Instant,
    ) -> Result<Challenge, (Refusal, T)> {
        let Some(sequence) = self.next else {
            return Err((Refusal::Identity, call));
        };
        let key = format!("native-confirmation/{}/{sequence}", self.nonce);
        let Some(bytes) = retained
            .checked_add(key.len())
            .and_then(|n| n.checked_add(std::mem::size_of::<Pending<T>>()))
        else {
            return Err((Refusal::Capacity, call));
        };
        let Some(total) = self.bytes.checked_add(bytes) else {
            return Err((Refusal::Capacity, call));
        };
        if self.pending.len() >= self.bounds.items
            || total > self.bounds.bytes
            || self.contains_original(&original)
        {
            return Err((Refusal::Capacity, call));
        }
        let Some(deadline) = now.checked_add(self.bounds.lifetime) else {
            return Err((Refusal::Identity, call));
        };
        self.next = sequence.checked_add(1).filter(|n| *n <= MAX_ID);
        let challenge = if legacy {
            Challenge::Legacy(sequence)
        } else {
            Challenge::Modern(key.clone())
        };
        self.pending.insert(
            key,
            Pending {
                original,
                binding,
                challenge: challenge.clone(),
                deadline,
                bytes,
                call,
            },
        );
        self.bytes = total;
        Ok(challenge)
    }
    fn remove(&mut self, key: &str) -> Option<Pending<T>> {
        let entry = self.pending.remove(key)?;
        self.bytes = self
            .bytes
            .checked_sub(entry.bytes)
            .expect("owned continuation byte accounting");
        Some(entry)
    }
    pub fn take_modern(
        &mut self,
        state: &str,
        binding: [u8; 32],
        now: Instant,
    ) -> Result<T, Refusal> {
        let entry = self.pending.get(state).ok_or(Refusal::InvalidState)?;
        if entry.deadline <= now {
            self.remove(state);
            return Err(Refusal::Expired);
        }
        if entry.binding != binding
            || !matches!(&entry.challenge,Challenge::Modern(token) if token==state)
        {
            return Err(Refusal::InvalidState);
        }
        Ok(self.remove(state).expect("validated continuation").call)
    }
    /// Responses occupy the server-request direction, not client request IDs.
    /// Unknown/malformed response frames never consume a live continuation.
    #[cfg(test)]
    pub fn take_legacy(&mut self, frame: &Value, now: Instant) -> Option<(T, Value)> {
        self.take_legacy_for(frame, now, |_| true)
    }
    pub fn take_legacy_for(
        &mut self,
        frame: &Value,
        now: Instant,
        owns: impl Fn(&T) -> bool,
    ) -> Option<(T, Value)> {
        let fields = frame.as_object()?;
        if frame["jsonrpc"] != "2.0"
            || fields
                .keys()
                .any(|k| !matches!(k.as_str(), "jsonrpc" | "id" | "result" | "error"))
            || fields.contains_key("result") == fields.contains_key("error")
        {
            return None;
        }
        let id = frame["id"].as_u64()?;
        let key = self
            .pending
            .iter()
            .find(|(_, p)| p.challenge == Challenge::Legacy(id) && owns(&p.call))
            .map(|(k, _)| k.clone())?;
        if self.pending[&key].deadline <= now {
            return None;
        }
        let call = self.remove(&key)?.call;
        let response = if fields.contains_key("error") {
            json!({"action":"cancel"})
        } else {
            frame["result"].clone()
        };
        let answer = json!({"confirm":response});
        // A malformed response is never fresh consent or a new lifetime.
        // Close this inert legacy request as cancelled instead of re-asking.
        let answer = if matches!(
            obs_mcp_native::control::ConfirmationAnswer::from_responses(Some(&answer)),
            obs_mcp_native::control::ConfirmationAnswer::Missing
        ) {
            json!({"confirm":{"action":"cancel"}})
        } else {
            answer
        };
        Some((call, answer))
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending.values().map(|p| p.deadline).min()
    }
    pub fn expire(&mut self, now: Instant) -> Vec<Removed<T>> {
        let keys = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| self.remove(&key))
            .map(|p| Removed {
                challenge: p.challenge,
                call: p.call,
            })
            .collect()
    }
    pub fn cancel(&mut self, original: &Value) -> Vec<Removed<T>> {
        let keys = self
            .pending
            .iter()
            .filter(|(_, p)| &p.original == original)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| self.remove(&key))
            .map(|p| Removed {
                challenge: p.challenge,
                call: p.call,
            })
            .collect()
    }
}
pub(super) fn binding(
    name: &str,
    original_arguments: &Value,
    envelope: &Value,
    caller: &str,
    profile: u8,
) -> Result<[u8; 32], Refusal> {
    let bytes = serde_json::to_vec(&json!([
        name,
        original_arguments,
        envelope,
        caller,
        profile
    ]))
    .map_err(|_| Refusal::Capacity)?;
    Ok(Sha256::digest(bytes).into())
}
// SDK2.4 retains the legacy empty elicitation offer as form support. An
// explicitly URL-only offer cannot authorize a form request in either era.
pub(super) fn supports_form(caps: Option<&Value>) -> bool {
    caps.and_then(|caps| caps.get("elicitation"))
        .and_then(Value::as_object)
        .is_some_and(|offer| offer.is_empty() || offer.contains_key("form"))
}
pub(super) fn eligible(name: &str) -> bool {
    matches!(
        name,
        "obs-take-scene"
            | "obs-start-stream"
            | "obs-stop-stream"
            | "obs-toggle-stream"
            | "obs-stop-record"
            | "obs-toggle-record"
            | "obs-take-stop"
            | "obs-call-request"
            | "obs-batch"
            | "obs-start-output"
            | "obs-stop-output"
            | "obs-toggle-output"
    )
}
pub(super) fn input_schema(name: &str, original: &Value, enabled: bool) -> Value {
    let mut schema = original.clone();
    if enabled && eligible(name) {
        schema = obs_mcp_native::tool_validation::confirmation_schema(name)
            .expect("checked Source confirmation schema");
    }
    schema
}
pub(super) fn required(question: String) -> Value {
    Disposition::InputRequired { question }
        .tool_result()
        .expect("required result")
}
pub(super) fn legacy_question(id: u64, question: String) -> Value {
    let mut request = required(question)["inputRequests"]["confirm"].clone();
    request["jsonrpc"] = json!("2.0");
    request["id"] = json!(id);
    request["params"]["_meta"] = json!({"progressToken":id});
    request
}
pub(super) fn legacy_cancel(id: u64) -> Value {
    json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":id,"reason":"Confirmation cancelled"}})
}

#[cfg(test)]
mod tests {
    use super::*;
    fn owner() -> Continuations<&'static str> {
        Continuations::with_nonce("fixture".into(), Bounds::default())
    }
    fn modern(
        owner: &mut Continuations<&'static str>,
        id: u64,
        label: &'static str,
        now: Instant,
    ) -> String {
        match owner
            .issue(json!(id), [1; 32], label, 100, false, now)
            .unwrap()
        {
            Challenge::Modern(token) => token,
            _ => unreachable!(),
        }
    }
    #[test]
    fn typed_payload_consumed_once_and_wrong_binding_does_not_consume_victim() {
        let mut owner = owner();
        let now = Instant::now();
        let token = modern(&mut owner, 1, "original", now);
        assert_eq!(
            owner.take_modern(&token, [2; 32], now),
            Err(Refusal::InvalidState)
        );
        assert_eq!(owner.take_modern(&token, [1; 32], now), Ok("original"));
        assert_eq!(
            owner.take_modern(&token, [1; 32], now),
            Err(Refusal::InvalidState)
        );
        assert_eq!(owner.bytes, 0);
    }
    #[test]
    fn nonce_original_id_capacity_and_exact_expiry_remain_distinct() {
        let now = Instant::now();
        let mut first = owner();
        let token = modern(&mut first, 1, "one", now);
        let mut other = Continuations::with_nonce("different".into(), Bounds::default());
        modern(&mut other, 1, "two", now);
        assert_eq!(
            other.take_modern(&token, [1; 32], now),
            Err(Refusal::InvalidState)
        );
        assert_eq!(
            first.issue(json!(1), [1; 32], "duplicate", 0, false, now),
            Err((Refusal::Capacity, "duplicate"))
        );
        assert_eq!(
            first.take_modern(&token, [1; 32], now + Duration::from_secs(60)),
            Err(Refusal::Expired)
        );
        assert_eq!(first.bytes, 0);
        let next = modern(&mut first, 1, "after expiry", now);
        assert_ne!(next, token);
    }
    #[test]
    fn finite_id_retires_and_capacity_refusal_returns_unstarted_payload() {
        let now = Instant::now();
        let mut owner = owner();
        owner.next = Some(MAX_ID);
        let token = modern(&mut owner, 1, "last", now);
        assert!(token.ends_with(&MAX_ID.to_string()));
        assert_eq!(
            owner.issue(json!(2), [1; 32], "retained", 0, false, now),
            Err((Refusal::Identity, "retained"))
        );
        assert_eq!(owner.take_modern(&token, [1; 32], now), Ok("last"));
        let mut tiny = Continuations::with_nonce(
            "tiny".into(),
            Bounds {
                items: 1,
                bytes: 1,
                lifetime: Duration::from_secs(1),
            },
        );
        assert_eq!(
            tiny.issue(json!(1), [1; 32], "retained", 2, false, now),
            Err((Refusal::Capacity, "retained"))
        );
        assert_eq!(tiny.bytes, 0);
    }
    #[test]
    fn legacy_direction_unknown_response_cancel_and_timeout_own_only_inert_payload() {
        let mut owner = owner();
        let now = Instant::now();
        assert_eq!(
            owner.issue(json!(0), [1; 32], "legacy", 100, true, now),
            Ok(Challenge::Legacy(0))
        );
        assert!(owner.take_legacy(&json!({"jsonrpc":"2.0","id":1,"result":{"action":"accept","content":{"confirm":true}}}),now).is_none());
        assert!(
            owner
                .take_legacy(
                    &json!({"jsonrpc":"2.0","id":0,"method":"fake","result":{}}),
                    now
                )
                .is_none()
        );
        let (call, response) = owner
            .take_legacy(
                &json!({"jsonrpc":"2.0","id":0,"result":{"action":"decline"}}),
                now,
            )
            .unwrap();
        assert_eq!(call, "legacy");
        assert_eq!(response, json!({"confirm":{"action":"decline"}}));
        modern(&mut owner, 2, "cancel", now);
        assert_eq!(owner.cancel(&json!(2)).pop().unwrap().call, "cancel");
        modern(&mut owner, 3, "expire", now);
        assert_eq!(
            owner
                .expire(now + Duration::from_secs(60))
                .pop()
                .unwrap()
                .call,
            "expire"
        );
        assert_eq!(owner.bytes, 0);
    }
    #[test]
    fn original_metadata_caller_arguments_and_profile_all_bind_resume() {
        let base = binding("tool", &json!({"a":1}), &json!({"caller":"a"}), "a", 1).unwrap();
        for next in [
            binding("other", &json!({"a":1}), &json!({"caller":"a"}), "a", 1),
            binding("tool", &json!({"a":2}), &json!({"caller":"a"}), "a", 1),
            binding("tool", &json!({"a":1}), &json!({"caller":"b"}), "a", 1),
            binding("tool", &json!({"a":1}), &json!({"caller":"a"}), "b", 1),
            binding("tool", &json!({"a":1}), &json!({"caller":"a"}), "a", 2),
        ] {
            assert_ne!(base, next.unwrap());
        }
    }
}

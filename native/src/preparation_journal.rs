// SPDX-License-Identifier: GPL-2.0-only
//! Durable preparation provenance. `PossibleDispatch` deliberately precedes the
//! socket send: it never claims that a request was sent. A crash in that gap is
//! uncertain, and reading the desired value cannot upgrade it to Accepted.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
pub const MAX_RECOVERY_ATTEMPTS: usize = 64;
const MAX_REVISION: u64 = 9_007_199_254_740_991;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Step {
    Intent,
    PossibleDispatch,
    Accepted,
    NotDispatched,
    Rejected,
    Unknown,
    ObservedBefore,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Fence {
    pub generation: u64,
    pub evidence_revision: u64,
}
impl Fence {
    fn valid(self) -> bool {
        self.generation <= MAX_REVISION && self.evidence_revision <= MAX_REVISION
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Attempt {
    pub fence: Fence,
    pub steps: Vec<Step>,
}
impl Attempt {
    pub fn status(&self) -> Step {
        *self.steps.last().expect("validated attempt")
    }
    fn valid(&self, recovery: bool) -> bool {
        if !self.fence.valid() {
            return false;
        }
        matches!(
            self.steps.as_slice(),
            [Step::Intent]
                | [Step::Intent, Step::PossibleDispatch]
                | [Step::Intent, Step::NotDispatched]
                | [
                    Step::Intent,
                    Step::PossibleDispatch,
                    Step::Accepted | Step::NotDispatched | Step::Rejected | Step::Unknown
                ]
        ) || (recovery && self.steps == [Step::ObservedBefore])
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldProvenance {
    pub preparation: Option<Attempt>,
    pub recovery: Vec<Attempt>,
}
impl FieldProvenance {
    pub fn preparation_status(&self) -> Option<Step> {
        self.preparation.as_ref().map(Attempt::status)
    }
    pub fn recovery_status(&self) -> Option<Step> {
        self.recovery.last().map(Attempt::status)
    }
    pub fn owns_restore(&self) -> bool {
        self.preparation_status() == Some(Step::Accepted)
    }
    pub fn restored(&self) -> bool {
        matches!(
            self.recovery_status(),
            Some(Step::Accepted | Step::ObservedBefore)
        )
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Provenance {
    pub codec: String,
    pub endpoint_hash: String,
    pub subject_hash: String,
    pub revision: u64,
    pub completed: bool,
    pub fields: Vec<FieldProvenance>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvenanceError {
    Invalid,
    Transition,
    Limit,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub revision: u64,
    pub completed: bool,
    pub accepted: usize,
    pub possible: usize,
    pub unknown: usize,
    pub not_dispatched: usize,
    pub rejected: usize,
    pub untouched: usize,
    pub intent: usize,
    pub recovery_accepted: usize,
    pub recovery_possible: usize,
    pub recovery_unknown: usize,
    pub recovery_observed_before: usize,
    pub recovery_intent: usize,
    pub recovery_not_dispatched: usize,
    pub recovery_rejected: usize,
}
impl Provenance {
    pub fn new(endpoint: &str, subject: &Value, count: usize) -> Self {
        Self {
            codec: "obs-mcp-preparation-v2".into(),
            endpoint_hash: hash(b"obs-mcp-endpoint-v1\0", endpoint.as_bytes()),
            subject_hash: subject_hash(subject),
            revision: 0,
            completed: false,
            fields: (0..count)
                .map(|_| FieldProvenance {
                    preparation: None,
                    recovery: vec![],
                })
                .collect(),
        }
    }
    pub fn validate(
        &self,
        endpoint: &str,
        subject: &Value,
        count: usize,
    ) -> Result<(), ProvenanceError> {
        if self.codec != "obs-mcp-preparation-v2"
            || self.endpoint_hash != hash(b"obs-mcp-endpoint-v1\0", endpoint.as_bytes())
            || self.subject_hash != subject_hash(subject)
            || self.revision > MAX_REVISION
            || self.fields.len() != count
        {
            return Err(ProvenanceError::Invalid);
        }
        let mut events = u64::from(self.completed);
        let mut preparation_stopped = false;
        let mut recovery_stopped = false;
        for field in &self.fields {
            if preparation_stopped && field.preparation.is_some() {
                return Err(ProvenanceError::Invalid);
            }
            preparation_stopped |= field.preparation_status() != Some(Step::Accepted);
            if recovery_stopped && !field.recovery.is_empty() {
                return Err(ProvenanceError::Invalid);
            }
            recovery_stopped |= !field.restored();
            if matches!(
                field.preparation_status(),
                Some(Step::PossibleDispatch | Step::Unknown)
            ) && !field.recovery.is_empty()
            {
                return Err(ProvenanceError::Invalid);
            }
            events = events
                .checked_add(
                    field
                        .preparation
                        .as_ref()
                        .map_or(0, |attempt| attempt.steps.len()) as u64,
                )
                .ok_or(ProvenanceError::Invalid)?;
            for attempt in &field.recovery {
                events = events
                    .checked_add(attempt.steps.len() as u64)
                    .ok_or(ProvenanceError::Invalid)?;
            }
            if field
                .preparation
                .as_ref()
                .is_some_and(|attempt| !attempt.valid(false))
                || field.recovery.len() > MAX_RECOVERY_ATTEMPTS
                || field.recovery.iter().any(|attempt| !attempt.valid(true))
            {
                return Err(ProvenanceError::Invalid);
            }
            for attempt in field
                .recovery
                .iter()
                .take(field.recovery.len().saturating_sub(1))
            {
                if !matches!(attempt.status(), Step::NotDispatched | Step::Rejected) {
                    return Err(ProvenanceError::Invalid);
                }
            }
            if !field.owns_restore()
                && field
                    .recovery
                    .iter()
                    .any(|attempt| attempt.steps != [Step::ObservedBefore])
            {
                return Err(ProvenanceError::Invalid);
            }
        }
        if events != self.revision {
            return Err(ProvenanceError::Invalid);
        }
        if self.completed
            && (!self.fields.iter().all(FieldProvenance::restored) || self.uncertain())
        {
            return Err(ProvenanceError::Invalid);
        }
        Ok(())
    }
    fn bump(&mut self) -> Result<(), ProvenanceError> {
        if self.completed {
            return Err(ProvenanceError::Transition);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .filter(|revision| *revision <= MAX_REVISION)
            .ok_or(ProvenanceError::Limit)?;
        Ok(())
    }
    pub fn begin_preparation(&mut self, index: usize, fence: Fence) -> Result<(), ProvenanceError> {
        if !fence.valid()
            || self.fields.iter().any(|field| !field.recovery.is_empty())
            || self
                .fields
                .iter()
                .take(index)
                .any(|field| !field.owns_restore())
        {
            return Err(ProvenanceError::Transition);
        }
        if self
            .fields
            .get(index)
            .is_none_or(|field| field.preparation.is_some() || !field.recovery.is_empty())
        {
            return Err(ProvenanceError::Transition);
        }
        self.bump()?;
        self.fields[index].preparation = Some(Attempt {
            fence,
            steps: vec![Step::Intent],
        });
        Ok(())
    }
    pub fn begin_recovery(&mut self, index: usize, fence: Fence) -> Result<(), ProvenanceError> {
        if self.uncertain()
            || !fence.valid()
            || self
                .fields
                .iter()
                .take(index)
                .any(|field| !field.restored())
        {
            return Err(ProvenanceError::Transition);
        }
        let field = self.fields.get(index).ok_or(ProvenanceError::Transition)?;
        if !field.owns_restore()
            || field.recovery.len() >= MAX_RECOVERY_ATTEMPTS
            || field
                .recovery_status()
                .is_some_and(|status| !matches!(status, Step::NotDispatched | Step::Rejected))
        {
            return Err(ProvenanceError::Transition);
        }
        self.bump()?;
        self.fields[index].recovery.push(Attempt {
            fence,
            steps: vec![Step::Intent],
        });
        Ok(())
    }
    pub fn advance(
        &mut self,
        index: usize,
        recovery: bool,
        next: Step,
    ) -> Result<(), ProvenanceError> {
        if !recovery && self.fields.iter().any(|field| !field.recovery.is_empty()) {
            return Err(ProvenanceError::Transition);
        }
        let field = self.fields.get(index).ok_or(ProvenanceError::Transition)?;
        let attempt = if recovery {
            field.recovery.last()
        } else {
            field.preparation.as_ref()
        }
        .ok_or(ProvenanceError::Transition)?;
        if !matches!(
            (attempt.status(), next),
            (Step::Intent, Step::PossibleDispatch | Step::NotDispatched)
                | (
                    Step::PossibleDispatch,
                    Step::Accepted | Step::NotDispatched | Step::Rejected | Step::Unknown
                )
        ) {
            return Err(ProvenanceError::Transition);
        }
        self.bump()?;
        let attempt = if recovery {
            self.fields[index].recovery.last_mut().unwrap()
        } else {
            self.fields[index].preparation.as_mut().unwrap()
        };
        attempt.steps.push(next);
        Ok(())
    }
    pub fn observed_before(&mut self, index: usize, fence: Fence) -> Result<(), ProvenanceError> {
        if !fence.valid()
            || self
                .fields
                .iter()
                .take(index)
                .any(|field| !field.restored())
        {
            return Err(ProvenanceError::Transition);
        }
        let field = self.fields.get(index).ok_or(ProvenanceError::Transition)?;
        if matches!(
            field.preparation_status(),
            Some(Step::PossibleDispatch | Step::Unknown)
        ) || field
            .recovery_status()
            .is_some_and(|status| !matches!(status, Step::NotDispatched | Step::Rejected))
            || field.recovery.len() >= MAX_RECOVERY_ATTEMPTS
        {
            return Err(ProvenanceError::Transition);
        }
        self.bump()?;
        self.fields[index].recovery.push(Attempt {
            fence,
            steps: vec![Step::ObservedBefore],
        });
        Ok(())
    }
    pub fn complete(&mut self) -> Result<(), ProvenanceError> {
        if self.uncertain() || !self.fields.iter().all(FieldProvenance::restored) {
            return Err(ProvenanceError::Transition);
        }
        self.bump()?;
        self.completed = true;
        Ok(())
    }
    pub fn uncertain(&self) -> bool {
        self.fields.iter().any(|field| {
            matches!(
                field.preparation_status(),
                Some(Step::PossibleDispatch | Step::Unknown)
            ) || matches!(
                field.recovery_status(),
                Some(Step::PossibleDispatch | Step::Unknown)
            )
        })
    }
    pub fn attempted(&self) -> usize {
        self.fields
            .iter()
            .filter(|field| {
                matches!(
                    field.preparation_status(),
                    Some(Step::PossibleDispatch | Step::Accepted | Step::Rejected | Step::Unknown)
                )
            })
            .count()
    }
    pub fn all_preparation_accepted(&self) -> bool {
        !self.completed && self.fields.iter().all(FieldProvenance::owns_restore)
    }
    pub fn summary(&self) -> Summary {
        let mut value = Summary {
            revision: self.revision,
            completed: self.completed,
            accepted: 0,
            possible: 0,
            unknown: 0,
            not_dispatched: 0,
            rejected: 0,
            untouched: 0,
            intent: 0,
            recovery_accepted: 0,
            recovery_possible: 0,
            recovery_unknown: 0,
            recovery_observed_before: 0,
            recovery_intent: 0,
            recovery_not_dispatched: 0,
            recovery_rejected: 0,
        };
        for field in &self.fields {
            match field.preparation_status() {
                Some(Step::Accepted) => value.accepted += 1,
                Some(Step::PossibleDispatch) => value.possible += 1,
                Some(Step::Unknown) => value.unknown += 1,
                Some(Step::NotDispatched) => value.not_dispatched += 1,
                Some(Step::Rejected) => value.rejected += 1,
                Some(Step::Intent) => value.intent += 1,
                _ => value.untouched += 1,
            }
            match field.recovery_status() {
                Some(Step::Accepted) => value.recovery_accepted += 1,
                Some(Step::PossibleDispatch) => value.recovery_possible += 1,
                Some(Step::Unknown) => value.recovery_unknown += 1,
                Some(Step::ObservedBefore) => value.recovery_observed_before += 1,
                Some(Step::Intent) => value.recovery_intent += 1,
                Some(Step::NotDispatched) => value.recovery_not_dispatched += 1,
                Some(Step::Rejected) => value.recovery_rejected += 1,
                _ => {}
            }
        }
        value
    }
    pub fn successor(&self, next: &Self) -> Result<(), ProvenanceError> {
        let events = |value: &Self| {
            value
                .fields
                .iter()
                .try_fold(u64::from(value.completed), |total, field| {
                    let total = total.checked_add(
                        field
                            .preparation
                            .as_ref()
                            .map_or(0, |attempt| attempt.steps.len())
                            as u64,
                    )?;
                    field.recovery.iter().try_fold(total, |total, attempt| {
                        total.checked_add(attempt.steps.len() as u64)
                    })
                })
        };
        if events(self) != Some(self.revision) || events(next) != Some(next.revision) {
            return Err(ProvenanceError::Transition);
        }
        if self == next {
            return Ok(());
        }
        if self.completed
            || self.codec != next.codec
            || self.endpoint_hash != next.endpoint_hash
            || self.subject_hash != next.subject_hash
            || self.fields.len() != next.fields.len()
            || next.revision != self.revision.checked_add(1).ok_or(ProvenanceError::Limit)?
        {
            return Err(ProvenanceError::Transition);
        }
        let mut candidates = vec![];
        for (index, field) in self.fields.iter().enumerate() {
            let next_field = &next.fields[index];
            if field == next_field {
                continue;
            }
            if field.preparation != next_field.preparation
                && self.fields.iter().any(|field| !field.recovery.is_empty())
            {
                return Err(ProvenanceError::Transition);
            }
            candidates.push(index);
            if !attempt_extension(field.preparation.as_ref(), next_field.preparation.as_ref())
                || next_field.recovery.len() < field.recovery.len()
                || next_field.recovery.len() > field.recovery.len() + 1
            {
                return Err(ProvenanceError::Transition);
            }
            for (old, new) in field.recovery.iter().zip(&next_field.recovery) {
                if !attempt_extension(Some(old), Some(new)) {
                    return Err(ProvenanceError::Transition);
                }
            }
            if next_field.recovery.len() == field.recovery.len() + 1
                && !next_field.recovery.last().unwrap().valid(true)
            {
                return Err(ProvenanceError::Transition);
            }
        }
        if next.completed {
            if !candidates.is_empty()
                || !next.fields.iter().all(FieldProvenance::restored)
                || next.uncertain()
            {
                return Err(ProvenanceError::Transition);
            }
        } else if candidates.len() != 1 {
            return Err(ProvenanceError::Transition);
        }
        Ok(())
    }
}
fn attempt_extension(old: Option<&Attempt>, new: Option<&Attempt>) -> bool {
    match (old, new) {
        (None, None) => true,
        (None, Some(new)) => new.steps == [Step::Intent] || (new.steps == [Step::ObservedBefore]),
        (Some(old), Some(new)) => {
            old == new
                || (old.fence == new.fence
                    && new.steps.len() == old.steps.len() + 1
                    && new.steps.starts_with(&old.steps))
        }
        _ => false,
    }
}
fn subject_hash(subject: &Value) -> String {
    hash(
        b"obs-mcp-preparation-subject-v2\0",
        &serde_json::to_vec(subject).expect("JSON subject"),
    )
}
fn hash(domain: &[u8], bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    format!("{:x}", hash.finalize())
}

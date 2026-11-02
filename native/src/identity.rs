// SPDX-License-Identifier: GPL-2.0-only
//! Socket-scoped identities. Exhaustion retires the allocator rather than wrapping.
use std::{fmt, num::NonZeroU64};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Generation(NonZeroU64);

impl fmt::Display for Generation {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(out)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestTag {
    Capabilities,
    Call(NonZeroU64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RequestIdentity {
    generation: Generation,
    tag: RequestTag,
}

impl RequestIdentity {
    pub(crate) fn generation(self) -> Generation {
        self.generation
    }

    pub(crate) fn capabilities(generation: Generation) -> Self {
        Self {
            generation,
            tag: RequestTag::Capabilities,
        }
    }
}

impl fmt::Display for RequestIdentity {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(out, "{}:", self.generation)?;
        match self.tag {
            RequestTag::Capabilities => out.write_str("capabilities"),
            RequestTag::Call(sequence) => sequence.fmt(out),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct IdentityClock {
    generation: u64,
    sequence: u64,
    retired: bool,
}

impl IdentityClock {
    /// Reserve both counters before any network work. A failed reservation permanently
    /// retires this owner; even reconnecting may not make an old identity valid again.
    pub(crate) fn admit(&mut self, new_session: bool) -> Result<RequestIdentity, &'static str> {
        if self.retired {
            return Err("OBS identity owner is retired");
        }
        let generation = if new_session {
            self.generation.checked_add(1)
        } else {
            Some(self.generation)
        }
        .and_then(NonZeroU64::new);
        let sequence = self.sequence.checked_add(1).and_then(NonZeroU64::new);
        let (Some(generation), Some(sequence)) = (generation, sequence) else {
            self.retired = true;
            return Err("OBS identity exhausted or uninitialized; owner retired");
        };
        self.generation = generation.get();
        self.sequence = sequence.get();
        Ok(RequestIdentity {
            generation: Generation(generation),
            tag: RequestTag::Call(sequence),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_changes_generation_without_reusing_request_identity() {
        let mut clock = IdentityClock::default();
        let first = clock.admit(true).unwrap();
        let next = clock.admit(false).unwrap();
        let reconnected = clock.admit(true).unwrap();
        assert_eq!(first.to_string(), "1:1");
        assert_eq!(next.to_string(), "1:2");
        assert_eq!(reconnected.to_string(), "2:3");
        assert_ne!(first.generation(), reconnected.generation());
        assert_eq!(
            RequestIdentity::capabilities(first.generation()).to_string(),
            "1:capabilities"
        );
    }

    #[test]
    fn exhaustion_retires_the_owner_instead_of_resetting_on_reconnect() {
        for (generation, sequence) in [(u64::MAX, 7), (7, u64::MAX)] {
            let mut clock = IdentityClock {
                generation,
                sequence,
                retired: false,
            };
            assert!(clock.admit(true).unwrap_err().contains("retired"));
            assert!(clock.admit(false).unwrap_err().contains("retired"));
            assert!(clock.admit(true).unwrap_err().contains("retired"));
            assert_eq!((clock.generation, clock.sequence), (generation, sequence));
        }
    }

    #[test]
    fn uninitialized_reuse_is_not_a_valid_session() {
        let mut clock = IdentityClock::default();
        assert!(clock.admit(false).is_err());
        assert!(clock.admit(true).is_err());
    }
}

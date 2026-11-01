// SPDX-License-Identifier: GPL-2.0-only
//! Finite numeric projection and bounded Source-number JSON encoding.
//! This does not recover raw decimal parsing, property insertion order, lone
//! surrogates or overflow already refused by the surrounding JSON parser.
use crate::observation::js_string;
use serde_json::{Number, Value};
use std::io::{self, Write};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumericNormalizationError {
    Depth,
    EncodedBytes,
    NumberUnavailable,
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_depth: usize,
    pub max_encoded_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_depth: 128,
            max_encoded_bytes: crate::obs::MAX_BYTES,
        }
    }
}
impl Limits {
    /// The existing actor's receive envelope also accommodates image overhead;
    /// this is not the smaller bound used for validation-error diagnostics.
    pub fn readback() -> Self {
        Self {
            max_depth: 128,
            max_encoded_bytes: crate::obs::MAX_INCOMING_BYTES,
        }
    }
}
fn check(
    value: &Value,
    depth: usize,
    limits: Limits,
    nodes: &mut usize,
) -> Result<(), NumericNormalizationError> {
    if depth > limits.max_depth {
        return Err(NumericNormalizationError::Depth);
    }
    // Any encoded JSON node needs at least one byte, so this budget does not
    // impose an additional smaller valid-input node-count restriction.
    *nodes = nodes
        .checked_sub(1)
        .ok_or(NumericNormalizationError::EncodedBytes)?;
    match value {
        Value::Number(number) => {
            number
                .as_f64()
                .filter(|n| n.is_finite())
                .ok_or(NumericNormalizationError::NumberUnavailable)?;
        }
        Value::Array(values) => {
            for value in values {
                check(value, depth + 1, limits, nodes)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                check(value, depth + 1, limits, nodes)?;
            }
        }
        _ => {}
    }
    Ok(())
}
struct Count {
    used: usize,
    limit: usize,
}
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.used = self
            .used
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| io::Error::other("Source JSON byte limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn emit(output: &mut impl Write, value: &Value) -> io::Result<()> {
    match value {
        Value::Number(_) => output.write_all(js_string(Some(value)).as_bytes()),
        Value::Array(values) => {
            output.write_all(b"[")?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.write_all(b",")?;
                }
                emit(output, value)?;
            }
            output.write_all(b"]")
        }
        Value::Object(values) => {
            output.write_all(b"{")?;
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    output.write_all(b",")?;
                }
                serde_json::to_writer(&mut *output, key).map_err(io::Error::other)?;
                output.write_all(b":")?;
                emit(output, value)?;
            }
            output.write_all(b"}")
        }
        _ => serde_json::to_writer(output, value).map_err(io::Error::other),
    }
}
/// Borrow a complete immutable frame through validation and publication. The
/// length is checked once before permits; no complete frame or opaque Value is
/// cloned or allocated until the caller chooses into_buffer after admission.
/// This proof grants no network or mutation authority by itself.
pub struct Prepared<'a> {
    value: &'a Value,
    length: usize,
}
impl std::fmt::Debug for Prepared<'_> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("PreparedSourceJson")
            .field("encoded_bytes", &self.length)
            .finish_non_exhaustive()
    }
}
impl<'a> Prepared<'a> {
    pub fn new(value: &'a Value, limits: Limits) -> Result<Self, NumericNormalizationError> {
        let mut nodes = limits.max_encoded_bytes;
        check(value, 0, limits, &mut nodes)?;
        let mut count = Count {
            used: 0,
            limit: limits.max_encoded_bytes,
        };
        emit(&mut count, value).map_err(|_| NumericNormalizationError::EncodedBytes)?;
        Ok(Self {
            value,
            length: count.used,
        })
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    /// A writer failure can leave that writer partial; its owner must retain the
    /// byte permits and complete cleanup. Success reuses the validated borrow.
    pub fn write_to(&self, output: &mut impl Write) -> io::Result<()> {
        emit(output, self.value)
    }
    pub fn into_buffer(self) -> Result<Vec<u8>, NumericNormalizationError> {
        let mut bytes = Vec::with_capacity(self.length);
        self.write_to(&mut bytes)
            .map_err(|_| NumericNormalizationError::EncodedBytes)?;
        debug_assert_eq!(bytes.len(), self.length);
        Ok(bytes)
    }
}
/// Count with exactly the same Source-number encoder used after byte admission.
pub fn encoded_len(value: &Value, limits: Limits) -> Result<usize, NumericNormalizationError> {
    Ok(Prepared::new(value, limits)?.len())
}
pub fn encode_source_json(
    value: &Value,
    limits: Limits,
) -> Result<Vec<u8>, NumericNormalizationError> {
    Prepared::new(value, limits)?.into_buffer()
}
pub fn normalize_json_numbers(value: &mut Value) -> Result<(), NumericNormalizationError> {
    normalize_with_limits(value, Limits::default())
}
/// Refusal is atomic: validate/count Source encoding before changing any node.
/// The owning FIFO and actor must use encoded_len/encode_source_json too;
/// default serde_json encoding is not interchangeable for all numeric tokens.
pub fn normalize_with_limits(
    value: &mut Value,
    limits: Limits,
) -> Result<(), NumericNormalizationError> {
    encoded_len(value, limits)?;
    fn visit(value: &mut Value) {
        match value {
            Value::Number(number) => {
                let n = number.as_f64().expect("checked finite number");
                let text = js_string(Some(&Value::Number(number.clone())));
                *number = if let Ok(n) = text.parse::<u64>() {
                    Number::from(n)
                } else if let Ok(n) = text.parse::<i64>() {
                    Number::from(n)
                } else {
                    Number::from_f64(n).expect("checked finite number")
                };
            }
            Value::Array(values) => {
                for value in values {
                    visit(value);
                }
            }
            Value::Object(values) => {
                for value in values.values_mut() {
                    visit(value);
                }
            }
            _ => {}
        }
    }
    visit(value);
    Ok(())
}

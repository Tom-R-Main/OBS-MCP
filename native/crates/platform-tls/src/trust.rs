//! Immutable process-local anchors. These contain certificates, never keys or
//! operation state. Native trust engines remain responsible for X.509 policy.
use std::{io, sync::Arc};
#[derive(Clone)]
pub struct AdditionalRoots(Arc<[Vec<u8>]>);
impl AdditionalRoots {
    pub fn from_der(certificates: Vec<Vec<u8>>) -> io::Result<Self> {
        let size: usize = certificates.iter().map(Vec::len).sum();
        if certificates.is_empty()
            || certificates.len() > 32
            || size > 256 * 1024
            || certificates.iter().any(|c| !complete_der(c))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid additional TLS certificate anchors",
            ));
        }
        Ok(Self(certificates.into()))
    }
    pub(crate) fn certificates(&self) -> &[Vec<u8>] {
        &self.0
    }
}
fn complete_der(bytes: &[u8]) -> bool {
    if bytes.len() < 2 || bytes[0] != 0x30 {
        return false;
    }
    let (header, len) = if bytes[1] < 128 {
        (2, usize::from(bytes[1]))
    } else {
        let count = usize::from(bytes[1] & 0x7f);
        if count == 0
            || count > std::mem::size_of::<usize>()
            || bytes.len() < count + 2
            || bytes[2] == 0
        {
            return false;
        }
        let mut len = 0usize;
        for b in &bytes[2..2 + count] {
            let Some(next) = len
                .checked_mul(256)
                .and_then(|n| n.checked_add(usize::from(*b)))
            else {
                return false;
            };
            len = next;
        }
        if len < 128 {
            return false;
        }
        (2 + count, len)
    };
    header.checked_add(len) == Some(bytes.len())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_empty_truncated_concatenated_and_noncanonical_der() {
        for bytes in [
            vec![],
            vec![0x30, 3, 1],
            vec![0x30, 0, 0],
            vec![0x30, 0x81, 0],
        ] {
            assert!(AdditionalRoots::from_der(vec![bytes]).is_err());
        }
        assert!(AdditionalRoots::from_der(vec![vec![0x30, 0]]).is_ok());
    }
}

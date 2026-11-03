//! Additional anchors change only a per-connection SecTrust. The verification
//! block captures immutable configuration, never Entry/Shared/IO/deadline state.
use crate::{AdditionalRoots, ffi::*};
use std::{io, ptr, sync::Arc};
struct CfOwned(Cf);
impl Drop for CfOwned {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { CFRelease(self.0) }
        }
    }
}
/// An immutable CFArray retaining immutable SecCertificate instances. No mutable
/// CF object or trust result is shared; each verification receives its own trust.
pub(crate) struct Anchors {
    array: CfOwned,
    _der: AdditionalRoots,
}
// SAFETY: the CFArray/SecCertificates are immutable and use atomic CF reference
// counting. Only borrowed reads occur concurrently; each SecTrust is callback-local.
unsafe impl Send for Anchors {}
unsafe impl Sync for Anchors {}
impl Anchors {
    pub(crate) fn new(roots: &AdditionalRoots) -> io::Result<Arc<Self>> {
        let mut certificates = Vec::new();
        for der in roots.certificates() {
            // SAFETY: bounded owned DER; CFData copies bytes. Both Create results
            // have independent +1 owners and are released on every error path.
            let data =
                CfOwned(unsafe { CFDataCreate(ptr::null(), der.as_ptr(), der.len() as isize) });
            if data.0.is_null() {
                return Err(invalid());
            }
            let certificate = CfOwned(unsafe { SecCertificateCreateWithData(ptr::null(), data.0) });
            if certificate.0.is_null() {
                return Err(invalid());
            }
            certificates.push(certificate);
        }
        let pointers: Vec<Cf> = certificates.iter().map(|c| c.0).collect();
        // SAFETY: kCFType callbacks retain each certificate; array is immutable.
        let array = CfOwned(unsafe {
            CFArrayCreate(
                ptr::null(),
                pointers.as_ptr(),
                pointers.len() as isize,
                &raw const kCFTypeArrayCallBacks,
            )
        });
        if array.0.is_null() {
            return Err(invalid());
        }
        Ok(Arc::new(Self {
            array,
            _der: roots.clone(),
        }))
    }
    pub(crate) fn verify(&self, trust: Raw) -> bool {
        if trust.is_null() {
            return false;
        }
        // SAFETY: sec_trust_copy_ref yields +1 underlying trust scoped to this
        // callback. Preserve its native TLS server-name/purpose/validity policies.
        // Additional anchors are additive (Only=false). No keychain/global write.
        // Disable network fetch for this evaluation so missing intermediates or
        // revocation fetches cannot initiate application-unowned network activity.
        unsafe {
            let trust = CfOwned(sec_trust_copy_ref(trust));
            if trust.0.is_null() {
                return false;
            }
            let mut error = ptr::null();
            let valid = SecTrustSetNetworkFetchAllowed(trust.0, 0) == 0
                && SecTrustSetAnchorCertificates(trust.0, self.array.0) == 0
                && SecTrustSetAnchorCertificatesOnly(trust.0, 0) == 0
                && SecTrustEvaluateWithError(trust.0, &mut error);
            let _error = CfOwned(error);
            valid
        }
    }
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid additional TLS certificate anchors",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_parser_rejects_non_certificate_der_before_admission() {
        let roots = AdditionalRoots::from_der(vec![vec![0x30, 0]]).unwrap();
        assert!(Anchors::new(&roots).is_err());
    }
}

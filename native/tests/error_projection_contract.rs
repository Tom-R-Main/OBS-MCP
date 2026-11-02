// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{demo::DemoError, snapshots::SnapshotError};

#[test]
fn peer_evidence_errors_keep_private_values_out_of_ordinary_diagnostics() {
    let secret = "PRIVATE-PEER-SETTING";
    for error in [
        DemoError::Evidence(secret.into()),
        DemoError::PeerRejected {
            code: 500,
            message: secret.into(),
        },
    ] {
        assert!(!format!("{error:?} {error}").contains(secret));
        assert_eq!(error.tool_message(), secret);
    }
    let error = SnapshotError::Read(secret.into());
    assert!(!format!("{error:?} {error}").contains(secret));
    assert_eq!(error.tool_message(), secret);
    let propagated = DemoError::from(error);
    assert!(!format!("{propagated:?} {propagated}").contains(secret));
    assert_eq!(propagated.tool_message(), secret);
}

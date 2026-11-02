// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::clock::{ClockError, from_epoch_duration};
use serde_json::{Value, json};
use std::{process::Command, time::Duration};

#[test]
fn utc_metadata_matches_the_pinned_predecessor_date_contract() {
    let cases: Vec<u64> = vec![
        0,
        1,
        999,
        1000,
        86_399_999,
        86_400_000,
        951_782_399_999,
        951_782_400_000,
        951_868_800_000,
        1_793_473_234_567,
        4_107_542_400_000,
        13_574_563_200_000,
        253_402_300_799_999,
    ];
    let script = format!(
        "console.log(JSON.stringify({}.map(ms=>new Date(ms).toISOString())))",
        json!(cases)
    );
    let oracle = Command::new("node").args(["-e", &script]).output().unwrap();
    assert!(oracle.status.success());
    let dates: Value = serde_json::from_slice(&oracle.stdout).unwrap();
    for (index, epoch) in cases.iter().enumerate() {
        let snapshot = from_epoch_duration(Duration::from_millis(*epoch)).unwrap();
        assert_eq!(snapshot.epoch_ms, *epoch);
        assert_eq!(json!(snapshot.utc), dates[index]);
    }
    assert_eq!(
        from_epoch_duration(Duration::from_millis(253_402_300_800_000)).unwrap_err(),
        ClockError::OutOfRange
    );
    assert_eq!(
        from_epoch_duration(Duration::MAX).unwrap_err(),
        ClockError::OutOfRange
    );
}

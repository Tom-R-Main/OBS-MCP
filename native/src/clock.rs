// SPDX-License-Identifier: GPL-2.0-only
//! Wall-clock metadata is separate from monotonic operation deadlines.
use crate::custody::ClockSnapshot;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockError {
    BeforeEpoch,
    OutOfRange,
}

fn leap(year: u64) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

/// The journal's version-1 UTC schema has four-digit years and nonnegative
/// epoch metadata. Clock failures refuse admission rather than inventing time.
pub fn from_epoch_duration(duration: Duration) -> Result<ClockSnapshot, ClockError> {
    let epoch_ms = u64::try_from(duration.as_millis()).map_err(|_| ClockError::OutOfRange)?;
    let mut days = duration.as_secs() / 86_400;
    if days > 2_932_896 {
        return Err(ClockError::OutOfRange);
    } // 9999-12-31
    let mut year = 1970;
    loop {
        let length = if leap(year) { 366 } else { 365 };
        if days < length {
            break;
        }
        days -= length;
        year += 1;
    }
    if year > 9999 {
        return Err(ClockError::OutOfRange);
    }
    let months = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0;
    while days >= months[month] {
        days -= months[month];
        month += 1;
    }
    let time = duration.as_secs() % 86_400;
    Ok(ClockSnapshot {
        epoch_ms,
        utc: format!(
            "{year:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            month + 1,
            days + 1,
            time / 3600,
            (time / 60) % 60,
            time % 60,
            duration.subsec_millis()
        ),
    })
}

pub fn snapshot() -> Result<ClockSnapshot, ClockError> {
    from_epoch_duration(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ClockError::BeforeEpoch)?,
    )
}

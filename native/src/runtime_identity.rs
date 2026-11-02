// SPDX-License-Identifier: GPL-2.0-only
//! Fresh opaque public identifiers; entropy failure never falls back to time,
//! PID, a counter, or a reusable identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityError {
    EntropyUnavailable,
}

pub fn uuid_v4() -> Result<String, IdentityError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| IdentityError::EntropyUnavailable)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn ids_are_fresh_standard_v4_and_accepted_by_the_client_schema() {
        let ids: std::collections::HashSet<_> =
            (0..128).map(|_| super::uuid_v4().unwrap()).collect();
        assert_eq!(ids.len(), 128);
        for id in &ids {
            assert_eq!(id.len(), 36);
            assert_eq!(&id[14..15], "4");
            assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"));
        }
        let project = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        let output=std::process::Command::new("node").args(["--input-type=module","-e","import{z}from'zod';for(const id of JSON.parse(process.argv[1]))z.uuid().parse(id);",&serde_json::to_string(&ids).unwrap()]).current_dir(project).output().unwrap();
        assert!(
            output.status.success(),
            "Actual client UUID schema rejected a generated ID"
        );
    }
}

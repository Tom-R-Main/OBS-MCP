// SPDX-License-Identifier: GPL-2.0-only
//! Immutable service-lifetime authority. A tool filter cannot promote a profile.
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NativeProfile {
    #[default]
    Production,
    Advanced,
}
impl NativeProfile {
    pub fn parse(value: Option<&str>) -> Result<Self, InvalidProfile> {
        match value.map(crate::config::trim).unwrap_or("") {
            "" | "production" => Ok(Self::Production),
            "advanced" => Ok(Self::Advanced),
            _ => Err(InvalidProfile),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Advanced => "advanced",
        }
    }
    pub fn allows(self, name: &str) -> bool {
        if crate::advanced_commands::Request::is_name(name)
            || crate::opaque_commands::is_name(name)
            || name == "obs-press-input-property-button"
        {
            return self == Self::Advanced;
        }
        self == Self::Production
            || !matches!(
                name,
                "obs-stage-scene"
                    | "obs-take-scene"
                    | "obs-apply-scene"
                    | "obs-capture-window"
                    | "obs-prepare-demo"
                    | "obs-recover-demo"
                    | "obs-snapshot"
                    | "obs-restore"
                    | "obs-take-start"
                    | "obs-take-stop"
                    | "obs-take-mark"
                    | "obs-take-status"
                    | "obs-record-clip"
                    | "obs-contact-sheet"
                    | "obs-trim-take"
            )
    }
    pub fn metadata(self) -> Value {
        json!({"name":self.name(),"immutable":true,
            "ownedTakeAndMediaPermitted":self==Self::Production,
            "advancedHistory":"Accepted or uncertain advanced/stream dispatch history blocks future guarded-owner admission in this retained state namespace. Current inactivity, a new MCP process or a new socket is not proof of a clean OBS incarnation. No automatic clear is provided."})
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidProfile;

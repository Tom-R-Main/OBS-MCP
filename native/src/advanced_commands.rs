// SPDX-License-Identifier: GPL-2.0-only
//! Five closed manual commands. They grant no owned Take Stop, file, recovery,
//! stage or arbitrary-RPC capability. Actual execution stays with Service.
use crate::operation_store::GenericKind;
use serde_json::{Value, json};

#[derive(Clone)]
enum Command {
    RecordStart,
    RecordToggle,
    OutputStart(String),
    OutputStop(String),
    OutputToggle(String),
}
#[derive(Clone)]
pub struct Request(Command);
impl std::fmt::Debug for Request {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("AdvancedCommand")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}
impl Request {
    pub fn is_name(name: &str) -> bool {
        matches!(
            name,
            "obs-start-record"
                | "obs-toggle-record"
                | "obs-start-output"
                | "obs-stop-output"
                | "obs-toggle-output"
        )
    }
    pub fn parse(name: &str, arguments: &Value) -> Option<Self> {
        arguments.as_object()?;
        Some(Self(match name {
            "obs-start-record" => Command::RecordStart,
            "obs-toggle-record" => Command::RecordToggle,
            "obs-start-output" => {
                Command::OutputStart(arguments.get("outputName")?.as_str()?.to_owned())
            }
            "obs-stop-output" => {
                Command::OutputStop(arguments.get("outputName")?.as_str()?.to_owned())
            }
            "obs-toggle-output" => {
                Command::OutputToggle(arguments.get("outputName")?.as_str()?.to_owned())
            }
            _ => return None,
        }))
    }
    pub(crate) fn kind(&self) -> GenericKind {
        match self.0 {
            Command::RecordStart => GenericKind::ManualRecordStart,
            Command::RecordToggle => GenericKind::ManualRecordToggle,
            Command::OutputStart(_) => GenericKind::NamedOutputStart,
            Command::OutputStop(_) => GenericKind::NamedOutputStop,
            Command::OutputToggle(_) => GenericKind::NamedOutputToggle,
        }
    }
    pub(crate) fn name(&self) -> &'static str {
        match self.0 {
            Command::RecordStart => "obs-start-record",
            Command::RecordToggle => "obs-toggle-record",
            Command::OutputStart(_) => "obs-start-output",
            Command::OutputStop(_) => "obs-stop-output",
            Command::OutputToggle(_) => "obs-toggle-output",
        }
    }
    pub(crate) fn title(&self) -> &'static str {
        match self.0 {
            Command::RecordStart => "Start Recording",
            Command::RecordToggle => "Toggle Recording",
            Command::OutputStart(_) => "Start Output",
            Command::OutputStop(_) => "Stop Output",
            Command::OutputToggle(_) => "Toggle Output",
        }
    }
    pub(crate) fn arguments(&self) -> Value {
        match &self.0 {
            Command::RecordStart | Command::RecordToggle => json!({}),
            Command::OutputStart(name)
            | Command::OutputStop(name)
            | Command::OutputToggle(name) => json!({"outputName":name}),
        }
    }
}

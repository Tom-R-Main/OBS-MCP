// SPDX-License-Identifier: GPL-2.0-only
//! Closed Record Clip arguments. Admission does not create a recording or
//! grant authority over whichever Take happens to be current later.
use crate::{observation, takes::StartOptions};
use serde_json::{Value, json};

pub const MAX_CLIP_SECONDS: f64 = 50.0;
#[derive(Clone, Debug)]
pub struct Chapter {
    pub at_seconds: f64,
    pub name: String,
}
#[derive(Clone, Debug)]
pub struct Request {
    duration_seconds: f64,
    chapters: Vec<Chapter>,
    expect_silent: bool,
}
impl Request {
    pub fn new(duration_seconds: f64, chapters: Vec<Chapter>, expect_silent: bool) -> Option<Self> {
        if !duration_seconds.is_finite()
            || duration_seconds <= 0.0
            || duration_seconds > MAX_CLIP_SECONDS
            || chapters.iter().any(|chapter| {
                !chapter.at_seconds.is_finite()
                    || chapter.at_seconds < 0.0
                    || chapter.name.is_empty()
            })
        {
            return None;
        }
        Some(Self {
            duration_seconds,
            chapters,
            expect_silent,
        })
    }
    /// The frontend still owns schema-error projection. This parser accepts
    /// exactly the validated Source fields/defaults and discards unknowns.
    pub fn parse(value: &Value) -> Option<Self> {
        let fields = value.as_object()?;
        let duration = fields.get("durationSeconds")?.as_f64()?;
        let chapters = fields.get("chapters").map_or(Some(vec![]), |value| {
            value
                .as_array()?
                .iter()
                .map(|value| {
                    Some(Chapter {
                        at_seconds: value.get("atSeconds")?.as_f64()?,
                        name: value.get("name")?.as_str()?.to_owned(),
                    })
                })
                .collect::<Option<Vec<_>>>()
        })?;
        let expect_silent = fields
            .get("expectSilent")
            .map_or(Some(false), Value::as_bool)?;
        Self::new(duration, chapters, expect_silent)
    }
    pub fn duration_seconds(&self) -> f64 {
        self.duration_seconds
    }
    /// Source performs this business refusal in original input order before
    /// preflight or effects, not after sorting the chapter schedule.
    pub fn refusal(&self) -> Option<Value> {
        let late = self
            .chapters
            .iter()
            .filter(|chapter| chapter.at_seconds >= self.duration_seconds)
            .map(|chapter| format!("\"{}\"", chapter.name))
            .collect::<Vec<_>>();
        (!late.is_empty()).then(|| {
            let duration = observation::js_string(Some(&json!(self.duration_seconds)));
            json!({"content":[{"type":"text","text":format!("Not recording: chapter(s) {} start at or after the {duration}s clip ends",late.join(", "))}],"isError":true})
        })
    }
    pub(crate) fn start_options(&self) -> StartOptions {
        StartOptions {
            expect_silent: self.expect_silent,
            ..Default::default()
        }
    }
    pub(crate) fn into_schedule(mut self) -> (f64, Vec<Chapter>) {
        // sort_by is stable: equal offsets preserve Source's input order.
        self.chapters.sort_by(|left, right| {
            left.at_seconds
                .partial_cmp(&right.at_seconds)
                .expect("validated finite chapter offsets")
        });
        (self.duration_seconds, self.chapters)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_defaults_fractional_duration_and_late_chapter_order_are_preserved() {
        let request = Request::parse(&json!({"durationSeconds":0.25,"discarded":true})).unwrap();
        let start = request.start_options();
        assert!(!start.expect_silent && start.lock && !start.require_verified_preflight);
        assert_eq!(start.min_free_disk_mb, 1024.0);
        assert!(start.chapter.is_none() && start.max_duration_seconds.is_none());
        let request = Request::parse(&json!({"durationSeconds":0.25,"chapters":[{"atSeconds":1,"name":"late"},{"atSeconds":0,"name":"first"},{"atSeconds":0.25,"name":"equal"}]})).unwrap();
        assert_eq!(
            request.refusal().unwrap()["content"][0]["text"],
            "Not recording: chapter(s) \"late\", \"equal\" start at or after the 0.25s clip ends"
        );
    }
    #[test]
    fn zero_and_equal_chapter_offsets_keep_source_stable_order() {
        let request=Request::parse(&json!({"durationSeconds":1.5,"chapters":[{"atSeconds":0.5,"name":"a"},{"atSeconds":0,"name":"zero"},{"atSeconds":0.5,"name":"b"}]})).unwrap();
        assert!(request.refusal().is_none());
        let (duration, chapters) = request.into_schedule();
        assert_eq!(duration, 1.5);
        assert_eq!(
            chapters.into_iter().map(|c| c.name).collect::<Vec<_>>(),
            ["zero", "a", "b"]
        );
    }
}

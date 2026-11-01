// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::media_verification::{
    Cancellation, ErrorKind, ProcessOptions, StdoutSink, capture_bounded_media_process,
    run_streamed_media_process,
};
use std::{ffi::OsString, sync::Arc, time::Duration};

fn args(script: &str) -> Vec<OsString> {
    vec!["-e".into(), script.into()]
}
fn options() -> ProcessOptions {
    ProcessOptions {
        timeout: Duration::from_secs(5),
        ..ProcessOptions::default()
    }
}

#[tokio::test]
async fn capture_preserves_binary_and_returns_same_owned_result() {
    let mut job = capture_bounded_media_process(
        "node",
        &args("process.stdout.write(Buffer.from([0,255,195,169,10,0]));"),
        options(),
        6,
    )
    .unwrap();
    let pid = job.pid();
    let first = job.finish().await.unwrap();
    assert_eq!(&*first, &[0, 255, 195, 169, 10, 0]);
    let second = job.finish().await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(job.pid(), pid);
}

#[tokio::test]
async fn abandoning_an_await_preserves_single_capture() {
    let mut job = capture_bounded_media_process(
        "node",
        &args("setTimeout(()=>process.stdout.write('one spawn'),150);"),
        options(),
        32,
    )
    .unwrap();
    let pid = job.pid();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), job.finish())
            .await
            .is_err()
    );
    assert_eq!(&*job.finish().await.unwrap(), b"one spawn");
    assert_eq!(job.pid(), pid);
}

#[tokio::test]
async fn overflow_is_terminal_and_never_publishes_partial_capture() {
    let mut job = capture_bounded_media_process(
        "node",
        &args("process.stdout.write('too long');setInterval(()=>{},1000);"),
        options(),
        3,
    )
    .unwrap();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Output);
    assert_eq!(error.cleanup_issue(), None);
    assert_eq!(job.finish().await.unwrap_err(), error);
    assert!(job.is_finished());
}

#[derive(Default)]
struct CountingSink {
    stdout: usize,
    stderr: usize,
    maximum_chunk: usize,
}
impl StdoutSink for CountingSink {
    type Output = (usize, usize, usize);
    fn consume(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stdout += bytes.len();
        self.maximum_chunk = self.maximum_chunk.max(bytes.len());
        Ok(())
    }
    fn consume_stderr(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stderr += bytes.len();
        self.maximum_chunk = self.maximum_chunk.max(bytes.len());
        Ok(())
    }
    fn finish(self) -> Result<Self::Output, String> {
        Ok((self.stdout, self.stderr, self.maximum_chunk))
    }
}

#[tokio::test]
async fn streams_both_pipes_without_collecting_video() {
    let mut job = run_streamed_media_process(
        "node",
        &args(
            "process.stdout.write(Buffer.alloc(196613));process.stderr.write(Buffer.alloc(98311));",
        ),
        options(),
        CountingSink::default(),
    )
    .unwrap();
    let (stdout, stderr, maximum_chunk) = job.finish().await.unwrap();
    assert_eq!((stdout, stderr), (196613, 98311));
    assert!((1..=8192).contains(&maximum_chunk));
}

#[tokio::test]
async fn flooded_pipes_cannot_starve_unpolled_deadline() {
    let mut job = run_streamed_media_process("node", &args("function flood(){process.stdout.write(Buffer.alloc(8192));process.stderr.write(Buffer.alloc(8192));setImmediate(flood)}flood();"), ProcessOptions { timeout: Duration::from_millis(150), ..options() }, CountingSink::default()).unwrap();
    tokio::time::sleep(Duration::from_millis(950)).await;
    assert!(
        job.is_finished(),
        "deadline cleanup depended on polling finish"
    );
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert_eq!(error.cleanup_issue(), None);
}

struct RejectAtEof;
impl StdoutSink for RejectAtEof {
    type Output = ();
    fn consume(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
    fn finish(self) -> Result<(), String> {
        Err("incomplete frame at EOF".into())
    }
}
#[tokio::test]
async fn parser_must_finalize_before_success_is_visible() {
    let mut job = run_streamed_media_process(
        "node",
        &args("process.stdout.write('partial frame');"),
        options(),
        RejectAtEof,
    )
    .unwrap();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Output);
    assert_eq!(error.primary_message(), "incomplete frame at EOF");
}

#[tokio::test]
async fn invalid_capture_limits_do_not_spawn() {
    for limit in [0, 32 * 1024 * 1024 + 1] {
        let error = match capture_bounded_media_process("no-such-executable", &[], options(), limit)
        {
            Err(error) => error,
            Ok(_) => panic!("invalid limit admitted a process"),
        };
        assert_eq!(error.kind(), ErrorKind::InvalidOptions);
    }
}

#[tokio::test]
async fn dropping_a_joined_phase_does_not_cancel_the_shared_operation() {
    let cancellation = Cancellation::default();
    for expected in [
        b"encode".as_slice(),
        b"probe".as_slice(),
        b"decode".as_slice(),
    ] {
        let phase = ProcessOptions {
            cancellation: cancellation.clone(),
            ..options()
        };
        let script = format!(
            "process.stdout.write({:?});",
            String::from_utf8_lossy(expected)
        );
        let mut job = capture_bounded_media_process("node", &args(&script), phase, 16).unwrap();
        assert_eq!(&*job.finish().await.unwrap(), expected);
        drop(job);
        assert!(
            !cancellation.is_cancelled(),
            "a joined phase revoked its successor's permission"
        );
    }
}

#[tokio::test]
async fn inherited_stdout_requires_eof_after_the_direct_child_exits() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("capture-pipe-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let marker = root.join("writer-exited");
    let script = r#"const {spawn}=require('node:child_process');
const child=spawn(process.execPath,['-e',"setTimeout(()=>require('node:fs').writeFileSync(process.argv[1],'done',{flag:'wx'}),1100)",process.argv[1]],{stdio:['ignore','inherit','ignore']});child.unref();"#;
    let mut command_args = args(script);
    command_args.push(marker.clone().into_os_string());
    let mut job = capture_bounded_media_process("node", &command_args, options(), 32).unwrap();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cleanup);
    assert_eq!(
        error.cleanup_issue(),
        Some("media stdout pipe did not close after direct child exit")
    );
    let until = tokio::time::Instant::now() + Duration::from_secs(4);
    while !marker.exists() {
        assert!(
            tokio::time::Instant::now() < until,
            "fixture writer did not exit on its timer"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::remove_dir_all(root).unwrap();
}

struct RejectStderr;
impl StdoutSink for RejectStderr {
    type Output = ();
    fn consume(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
    fn consume_stderr(&mut self, _: &[u8]) -> Result<(), String> {
        Err("analysis stderr exceeded its bound".into())
    }
    fn finish(self) -> Result<(), String> {
        panic!("failed parser must not publish a result")
    }
}
#[tokio::test]
async fn stderr_parser_refusal_stops_and_joins_the_owned_producer() {
    let mut job = run_streamed_media_process(
        "node",
        &args("process.stderr.write('overflow');setInterval(()=>{},1000);"),
        options(),
        RejectStderr,
    )
    .unwrap();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Output);
    assert_eq!(
        error.primary_message(),
        "analysis stderr exceeded its bound"
    );
    assert_eq!(error.cleanup_issue(), None);
}

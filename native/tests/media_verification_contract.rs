// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::media_verification::{
    Cancellation, ErrorKind, MediaProcess, ProcessOptions, run_bounded_media_process,
    strict_decode, strict_decode_args,
};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::{Instant, sleep};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);
struct FixtureDir(PathBuf);
impl FixtureDir {
    fn new() -> Self {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "media-contract-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for FixtureDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn args(parts: &[&str]) -> Vec<OsString> {
    parts.iter().map(OsString::from).collect()
}
fn node(script: &str, timeout: Duration) -> MediaProcess {
    run_bounded_media_process(
        "node",
        &args(&["-e", script]),
        ProcessOptions {
            timeout,
            ..ProcessOptions::default()
        },
    )
    .unwrap()
}
async fn wait_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "fixture child did not become ready"
        );
        sleep(Duration::from_millis(10)).await;
    }
}
#[cfg(unix)]
fn alive(pid: u32) -> bool {
    Command::new("/bin/kill")
        .args(["-0", "--", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}
#[cfg(unix)]
async fn wait_gone(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(4);
    while alive(pid) {
        assert!(
            Instant::now() < deadline,
            "direct child remained alive after its bounded job"
        );
        sleep(Duration::from_millis(20)).await;
    }
}
fn resistant(dir: &FixtureDir, timeout: Duration) -> MediaProcess {
    let script = r#"
const fs=require('node:fs');
process.on('SIGTERM',()=>fs.appendFileSync(process.argv[2],'term\n'));
fs.appendFileSync(process.argv[2],'spawn\n');
fs.writeFileSync(process.argv[1],String(process.pid));
setInterval(()=>{},1000);
"#;
    run_bounded_media_process(
        "node",
        &[
            OsString::from("-e"),
            OsString::from(script),
            dir.path("ready").into_os_string(),
            dir.path("events").into_os_string(),
        ],
        ProcessOptions {
            timeout,
            ..ProcessOptions::default()
        },
    )
    .unwrap()
}
fn ts_result(script: &str) -> Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output=Command::new("node").arg("--input-type=module").arg("-e").arg(r#"
import {runBoundedMediaProcess} from './build/tools/media-verification.js';
try {await runBoundedMediaProcess(process.execPath,['-e',process.env.MEDIA_PROCESS_TEST_SCRIPT]);console.log(JSON.stringify({ok:true}));}
catch(error){console.log(JSON.stringify({ok:false,message:error.message}));}
"#).env("MEDIA_PROCESS_TEST_SCRIPT",script).current_dir(root).output().unwrap();
    assert!(
        output.status.success(),
        "TypeScript media oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[tokio::test]
async fn actual_typescript_success_and_failure_tail_projection_match() {
    let scripts = [
        "console.log('ignored stdout');console.error('ignored successful stderr');",
        "process.stderr.write('x'.repeat(5000)+' final decoder error \\n');process.exitCode=7;",
        "process.stderr.write('abc🙂'.repeat(1000));process.exitCode=2;",
        "process.kill(process.pid,'SIGTERM');",
        "process.kill(process.pid,'SIGUSR2');",
    ];
    for script in scripts {
        let expected = ts_result(script);
        let mut job = node(script, Duration::from_secs(5));
        let result = job.finish().await;
        let actual = match result {
            Ok(()) => json!({"ok":true}),
            Err(error) => {
                assert_eq!(error.kind(), ErrorKind::Decode);
                assert_eq!(error.cleanup_issue(), None);
                json!({"ok":false,"message":error.to_string()})
            }
        };
        assert_eq!(actual, expected);
        assert_eq!(
            job.finish().await.is_ok(),
            expected["ok"].as_bool().unwrap()
        );
        #[cfg(unix)]
        assert!(!alive(job.pid()), "result preceded direct child wait");
    }
}

#[tokio::test]
async fn spawn_failure_and_pre_cancel_refuse_without_retry() {
    let absent = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("no-such-media-verification-command");
    let failure = match run_bounded_media_process(&absent, &[], ProcessOptions::default()) {
        Err(error) => error,
        Ok(_) => panic!("missing command spawned"),
    };
    assert_eq!(failure.kind(), ErrorKind::Spawn);
    assert_eq!(
        failure.primary_message(),
        format!("spawn {} ENOENT", absent.display())
    );
    let dir = FixtureDir::new();
    let cancellation = Cancellation::default();
    cancellation.cancel();
    let script = "require('node:fs').writeFileSync(process.argv[1],'unexpected')";
    let result = run_bounded_media_process(
        "node",
        &[
            OsString::from("-e"),
            OsString::from(script),
            dir.path("spawned").into_os_string(),
        ],
        ProcessOptions {
            cancellation,
            ..ProcessOptions::default()
        },
    );
    let failure = match result {
        Err(error) => error,
        Ok(_) => panic!("pre-cancelled job spawned"),
    };
    assert_eq!(failure.primary_message(), "Media verification cancelled");
    assert!(!dir.path("spawned").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_sends_one_term_escalates_and_waits_for_resistant_direct_child() {
    let dir = FixtureDir::new();
    let mut job = resistant(&dir, Duration::from_secs(10));
    wait_file(&dir.path("ready")).await;
    let pid = job.pid();
    let at = Instant::now();
    job.cancel();
    let failure = job.finish().await.unwrap_err();
    assert_eq!(failure.kind(), ErrorKind::Cancelled);
    assert_eq!(failure.to_string(), "Media verification cancelled");
    assert!(
        at.elapsed() >= Duration::from_millis(450),
        "ignored TERM was not given its500ms grace"
    );
    assert!(!alive(pid));
    assert_eq!(
        fs::read_to_string(dir.path("events")).unwrap(),
        "spawn\nterm\n"
    );
    assert_eq!(job.finish().await.unwrap_err(), failure);
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_frontend_finish_keeps_same_child_for_later_cancel_and_join() {
    let dir = FixtureDir::new();
    let mut job = resistant(&dir, Duration::from_secs(10));
    wait_file(&dir.path("ready")).await;
    let pid = job.pid();
    {
        let finish = job.finish();
        tokio::pin!(finish);
        tokio::select! {
            result=&mut finish=>panic!("resistant child finished early: {result:?}"),
            _=sleep(Duration::from_millis(30))=>{},
        }
    }
    assert_eq!(job.pid(), pid);
    assert!(alive(pid));
    job.cancel();
    assert_eq!(
        job.finish().await.unwrap_err().primary_message(),
        "Media verification cancelled"
    );
    assert!(!alive(pid));
    assert_eq!(
        fs::read_to_string(dir.path("events")).unwrap(),
        "spawn\nterm\n"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn whole_job_deadline_kills_and_reaps_while_frontend_finish_is_unpolled() {
    let dir = FixtureDir::new();
    let mut job = resistant(&dir, Duration::from_millis(900));
    wait_file(&dir.path("ready")).await;
    let pid = job.pid();
    {
        let finish = job.finish();
        tokio::pin!(finish);
        tokio::select! {
            result=&mut finish=>panic!("resistant child finished early: {result:?}"),
            _=sleep(Duration::from_millis(20))=>{},
        }
    }
    // No call to finish in this interval: actor deadline and wait run anyway.
    wait_gone(pid).await;
    assert!(job.is_finished());
    assert_eq!(
        fs::read_to_string(dir.path("events")).unwrap(),
        "spawn\nterm\n"
    );
    assert_eq!(
        job.finish().await.unwrap_err().to_string(),
        "Media verification timed out"
    );
    assert_eq!(job.pid(), pid);
}

#[tokio::test]
async fn stderr_flood_cannot_starve_the_absolute_deadline_or_grow_the_retained_tail() {
    let mut job = node(
        "process.on('SIGTERM',()=>{});function flood(){process.stderr.write('x'.repeat(8192));setImmediate(flood)}flood();",
        Duration::from_millis(400),
    );
    let at = Instant::now();
    let failure = job.finish().await.unwrap_err();
    assert_eq!(failure.primary_message(), "Media verification timed out");
    assert!(at.elapsed() < Duration::from_secs(4));
}

#[test]
fn strict_arguments_are_the_authoritative_typescript_vector_with_one_opaque_path_argument() {
    let path = OsString::from("folder/a clip; $(literal).mkv");
    assert_eq!(
        strict_decode_args(&path),
        [
            "-nostdin",
            "-v",
            "error",
            "-xerror",
            "-err_detect",
            "explode",
            "-protocol_whitelist",
            "file,pipe",
            "-format_whitelist",
            "matroska,webm,mov,mp4,m4a,3gp,3g2,mj2,wav,avi,flv,mpegts,ogg,nut,yuv4mpegpipe",
            "-i",
            "folder/a clip; $(literal).mkv",
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-f",
            "null",
            "-"
        ]
        .map(OsString::from)
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output = Command::new("node")
        .arg("--input-type=module")
        .arg("-e")
        .arg(
            r#"
import cp from 'node:child_process';
import {EventEmitter} from 'node:events';
import {syncBuiltinESMExports} from 'node:module';
let captured;
cp.spawn=(command,args,options)=>{
  captured={command,args,stdio:options.stdio};
  const child=new EventEmitter();child.stderr=new EventEmitter();
  child.kill=()=>true;queueMicrotask(()=>child.emit('close',0,null));return child;
};
syncBuiltinESMExports();
const {strictDecode}=await import('./build/tools/media-verification.js');
await strictDecode('folder/a clip; $(literal).mkv');
console.log(JSON.stringify(captured));
"#,
        )
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "strict TypeScript argv oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let captured: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(captured["command"], "ffmpeg");
    assert_eq!(captured["stdio"], json!(["ignore", "ignore", "pipe"]));
    assert_eq!(
        captured["args"],
        json!(
            strict_decode_args(&path)
                .iter()
                .map(|arg| arg.to_str().unwrap())
                .collect::<Vec<_>>()
        )
    );
}

#[cfg(unix)]
#[tokio::test]
async fn inherited_stderr_after_leader_exit_is_bounded_and_explicitly_uncertain() {
    let dir = FixtureDir::new();
    // This intentionally demonstrates the direct-child boundary. The inherited
    // writer exits itself on a short timer; the verifier must not claim it was
    // stopped/reaped, or signal the already reaped leader's PID.
    let script = r#"
const {spawn}=require('node:child_process');
const child=spawn(process.execPath,['-e',"setTimeout(()=>require('node:fs').writeFileSync(process.argv[1],'done'),1100)",process.argv[1]],{stdio:['ignore','ignore','inherit']});
child.unref();
"#;
    let mut job = run_bounded_media_process(
        "node",
        &[
            OsString::from("-e"),
            OsString::from(script),
            dir.path("writer-exited").into_os_string(),
        ],
        ProcessOptions {
            timeout: Duration::from_secs(5),
            ..ProcessOptions::default()
        },
    )
    .unwrap();
    let at = Instant::now();
    let error = job.finish().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cleanup);
    assert_eq!(
        error.cleanup_issue(),
        Some("stderr pipe did not close after direct child exit")
    );
    assert!(!alive(job.pid()));
    assert!(at.elapsed() < Duration::from_secs(3));
    wait_file(&dir.path("writer-exited")).await;
}

#[tokio::test]
async fn installed_ffmpeg_strictly_decodes_valid_video_and_rejects_a_corrupt_header() {
    if !Command::new("ffmpeg")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
    {
        eprintln!("installed FFmpeg unavailable; strict command shape is covered separately");
        return;
    }
    let dir = FixtureDir::new();
    let path = dir.path("actual video; literal.y4m");
    let mut valid = b"YUV4MPEG2 W2 H2 F1:1 Ip A1:1 C420jpeg\nFRAME\n".to_vec();
    valid.extend([128_u8; 6]);
    valid.extend(b"FRAME\n");
    valid.extend([100_u8; 6]);
    fs::write(&path, &valid).unwrap();
    let mut good = strict_decode(&path, ProcessOptions::default()).unwrap();
    good.finish().await.unwrap();
    let corrupted = dir.path("corrupt.y4m");
    fs::write(&corrupted, &valid[..4]).unwrap();
    let mut bad = strict_decode(&corrupted, ProcessOptions::default()).unwrap();
    let failure = bad.finish().await.unwrap_err();
    assert_eq!(failure.kind(), ErrorKind::Decode);
    assert!(
        failure
            .primary_message()
            .starts_with("Media decode failed (")
    );
    assert_eq!(failure.cleanup_issue(), None);
}

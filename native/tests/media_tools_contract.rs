// SPDX-License-Identifier: GPL-2.0-only
use obs_mcp_native::{
    media_tools::{Config, DirectoryFuture, Operation, RecordingDirectory, Request},
    media_verification::{self, Cancellation, ProcessOptions},
};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::Notify,
    time::{Instant, timeout},
};
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "media-tools-contract-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&path).unwrap();
        private(&path);
        Self(path.canonicalize().unwrap())
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn config(&self) -> Config {
        let work = self.path("work");
        if !work.exists() {
            fs::create_dir(&work).unwrap();
            private(&work);
        }
        Config::new(&self.0, &tool("ffmpeg"), &tool("ffprobe"), &work).unwrap()
    }
    fn clean(&self) {
        assert!(fs::read_dir(self.path("work")).unwrap().next().is_none());
        assert!(!fs::read_dir(&self.0).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".partial")
        }));
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn private(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
}
fn tool(name: &str) -> PathBuf {
    ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"]
        .iter()
        .map(|p| Path::new(p).join(name))
        .find(|p| p.is_file())
        .unwrap()
}
fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}
async fn ffmpeg(arguments: &[OsString]) {
    let mut job = media_verification::run_bounded_media_process(
        tool("ffmpeg"),
        arguments,
        ProcessOptions {
            timeout: Duration::from_secs(20),
            ..ProcessOptions::default()
        },
    )
    .unwrap();
    job.finish().await.unwrap();
}
async fn clip(root: &Scratch, audio: &str) -> PathBuf {
    let output = root.path("source.mkv");
    let mut make = args(&[
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "color=c=red:s=160x90:r=10:d=4",
        "-f",
        "lavfi",
        "-i",
        "color=c=blue:s=160x90:r=10:d=4",
    ]);
    if audio != "none" {
        make.extend(args(&[
            "-f",
            "lavfi",
            "-i",
            if audio == "tone" {
                "sine=frequency=440:sample_rate=48000:duration=8"
            } else {
                "anullsrc=r=48000:cl=mono"
            },
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:sample_rate=48000:duration=8",
        ]));
    }
    make.extend(args(&[
        "-filter_complex",
        "[0:v:0][1:v:0]concat=n=2:v=1:a=0[v]",
        "-map",
        "[v]",
    ]));
    if audio != "none" {
        make.extend(args(&["-map", "2:a:0", "-map", "3:a:0", "-c:a", "aac"]));
    }
    make.extend(args(&[
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-pix_fmt",
        "yuv420p",
        "-t",
        "8",
    ]));
    make.push(output.as_os_str().to_owned());
    ffmpeg(&make).await;
    output
}
struct Directory {
    root: PathBuf,
    calls: Arc<AtomicUsize>,
    hold: usize,
    entered: Arc<AtomicUsize>,
    release: Arc<Notify>,
    wrong: Option<PathBuf>,
}
impl Directory {
    fn new(root: &Path) -> Arc<Self> {
        Arc::new(Self {
            root: root.to_owned(),
            calls: Arc::new(AtomicUsize::new(0)),
            hold: 0,
            entered: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(Notify::new()),
            wrong: None,
        })
    }
}
impl RecordingDirectory for Directory {
    fn current_directory(&self) -> DirectoryFuture {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let root = if call == self.hold {
            self.wrong.clone().unwrap_or_else(|| self.root.clone())
        } else {
            self.root.clone()
        };
        let hold = call == self.hold;
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            if hold {
                entered.store(call, Ordering::SeqCst);
                release.notified().await;
            }
            Ok(root)
        })
    }
}
fn owner(
    root: &Scratch,
    provider: Arc<dyn RecordingDirectory>,
    name: &str,
    arguments: Value,
) -> Operation {
    Operation::new(
        root.config(),
        provider,
        Request::parse(name, &arguments).unwrap(),
        Cancellation::default(),
    )
    .unwrap()
}
async fn wait_entered(provider: &Directory, call: usize) {
    let until = Instant::now() + Duration::from_secs(10);
    while provider.entered.load(Ordering::SeqCst) != call {
        assert!(Instant::now() < until, "provider was not reached");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
fn oracle(root: &Scratch, name: &str, arguments: &Value) -> Value {
    let script = r#"
import{McpServer,InMemoryTransport}from'@modelcontextprotocol/server';import{Client}from'@modelcontextprotocol/client';import{initialize}from'./build/tools/trim.js';
const root=process.argv[1],name=process.argv[2],args=JSON.parse(process.argv[3]),trace=[];
const adapter={async sendRequest(type){trace.push(type);if(type!=='GetRecordDirectory')throw Error('unexpected read');return{recordDirectory:root}}};
const server=new McpServer({name:'media-tools-oracle',version:'1'}),client=new Client({name:'media-tools-client',version:'1'});initialize(server,adapter);const[a,b]=InMemoryTransport.createLinkedPair();
try{await Promise.all([server.connect(a),client.connect(b)]);const result=await client.callTool({name,arguments:args});process.stdout.write(JSON.stringify({result,trace}));}finally{await client.close();await server.close()}
"#;
    let output = Command::new("node")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap())
        .args(["--input-type=module", "-e", script])
        .arg(&root.0)
        .arg(name)
        .arg(arguments.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn assert_success(result: &Value) {
    assert!(result.get("isError") != Some(&json!(true)), "{result}");
}

#[tokio::test]
async fn actual_contact_sheet_mcp_oracle_matches_explicit_default_sidecar_legend_and_every_content_field()
 {
    let root = Scratch::new();
    clip(&root, "silent").await;
    for arguments in [
        json!({"path":"source.mkv","times":[0.5,4.5,1.5],"columns":2,"width":80}),
        json!({"path":"source.mkv"}),
    ] {
        let expected = oracle(&root, "obs-contact-sheet", &arguments);
        let provider = Directory::new(&root.0);
        let mut operation = owner(&root, provider.clone(), "obs-contact-sheet", arguments);
        let result = operation.finish().await;
        assert_success(&result);
        assert_eq!(result, expected["result"]);
        assert_eq!(operation.finish().await, result);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        root.clean();
    }
    fs::write(root.path("source.take.json"),json!({"chapters":[{"atSeconds":0,"name":"Red"},{"atSeconds":"4","name":"Blue"},{"atSeconds":"bad","name":"ignore"}]}).to_string()).unwrap();
    let arguments = json!({"path":"source.mkv"});
    let expected = oracle(&root, "obs-contact-sheet", &arguments);
    let mut operation = owner(
        &root,
        Directory::new(&root.0),
        "obs-contact-sheet",
        arguments,
    );
    let result = operation.finish().await;
    assert_eq!(result, expected["result"]);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .ends_with("1. 1s Red\n2. 5s Blue")
    );
    root.clean();
}

#[tokio::test]
async fn actual_trim_mcp_oracle_matches_default_plan_audio_first_track_chapters_remap_and_apply_report()
 {
    let root = Scratch::new();
    clip(&root, "silent").await;
    fs::write(
        root.path("source.take.json"),
        json!({"chapters":[{"atSeconds":0,"name":"Red=result;"},{"atSeconds":4,"name":"Blue"}]})
            .to_string(),
    )
    .unwrap();
    let options = json!({"path":"source.mkv","minIdleSeconds":1,"keepSeconds":1,"holdAfterChapterSeconds":0,"holdBeforeChapterSeconds":0});
    for arguments in [json!({"path":"source.mkv"}), options.clone()] {
        let expected = oracle(&root, "obs-trim-take", &arguments);
        let mut operation = owner(&root, Directory::new(&root.0), "obs-trim-take", arguments);
        let result = operation.finish().await;
        assert_success(&result);
        assert_eq!(result, expected["result"]);
        root.clean();
    }
    let mut apply = options;
    apply["apply"] = json!(true);
    apply["outputPath"] = json!("edited.mp4");
    let expected = oracle(&root, "obs-trim-take", &apply);
    assert_success(&expected["result"]);
    assert_eq!(expected["result"]["structuredContent"]["committed"], true);
    let original = fs::read(root.path("source.mkv")).unwrap();
    let sdk_export = fs::read(root.path("edited.mp4")).unwrap();
    fs::remove_file(root.path("edited.mp4")).unwrap();
    let provider = Directory::new(&root.0);
    let mut operation = owner(&root, provider.clone(), "obs-trim-take", apply);
    let result = operation.finish().await;
    assert_success(&result);
    assert_eq!(result, expected["result"]);
    assert_eq!(fs::read(root.path("edited.mp4")).unwrap(), sdk_export);
    assert_eq!(fs::read(root.path("source.mkv")).unwrap(), original);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    assert_eq!(operation.finish().await, result);
    // Embedded chapters win over a take-log sidecar, including a conflicting
    // sidecar written after this export. This owner only reads the sidecar.
    fs::write(
        root.path("edited.take.json"),
        json!({"chapters":[{"atSeconds":0,"name":"must not replace embedded chapters"}]})
            .to_string(),
    )
    .unwrap();
    let arguments = json!({"path":"edited.mp4","width":80});
    let expected = oracle(&root, "obs-contact-sheet", &arguments);
    let mut sheet = owner(
        &root,
        Directory::new(&root.0),
        "obs-contact-sheet",
        arguments,
    );
    assert_eq!(sheet.finish().await, expected["result"]);
    root.clean();
}

#[tokio::test]
async fn actual_audible_first_audio_prevents_silent_video_cuts_and_matches_source() {
    let root = Scratch::new();
    clip(&root, "tone").await;
    let arguments = json!({"path":"source.mkv","minIdleSeconds":1,"holdBeforeChapterSeconds":0,"holdAfterChapterSeconds":0});
    let expected = oracle(&root, "obs-trim-take", &arguments);
    let mut operation = owner(&root, Directory::new(&root.0), "obs-trim-take", arguments);
    let result = operation.finish().await;
    assert_eq!(result, expected["result"]);
    assert_eq!(result["structuredContent"]["audioConsidered"], true);
    assert_eq!(result["structuredContent"]["cuts"], json!([]));
    root.clean();
}

#[tokio::test]
async fn business_refusals_match_source_without_changing_files() {
    let root = Scratch::new();
    clip(&root, "none").await;
    let cases = [
        ("obs-contact-sheet", json!({"path":"source.mkv","times":[]})),
        (
            "obs-contact-sheet",
            json!({"path":"source.mkv","times":[100]}),
        ),
        (
            "obs-trim-take",
            json!({"path":"source.mkv","apply":true,"outputPath":"source.mkv","minIdleSeconds":1,"keepSeconds":0,"holdBeforeChapterSeconds":0,"holdAfterChapterSeconds":0}),
        ),
        (
            "obs-trim-take",
            json!({"path":"source.mkv","apply":true,"outputPath":"old.mp4","minIdleSeconds":1,"keepSeconds":0,"holdBeforeChapterSeconds":0,"holdAfterChapterSeconds":0}),
        ),
    ];
    fs::write(root.path("old.mp4"), b"preserve old output").unwrap();
    for (name, arguments) in cases {
        let expected = oracle(&root, name, &arguments);
        let mut operation = owner(&root, Directory::new(&root.0), name, arguments);
        let result = operation.finish().await;
        assert_eq!(result, expected["result"]);
        assert_eq!(result["isError"], true);
        root.clean();
    }
    assert_eq!(
        fs::read(root.path("old.mp4")).unwrap(),
        b"preserve old output"
    );
}

#[tokio::test]
async fn abandoned_initial_directory_await_is_retained_without_a_second_provider_or_media_job() {
    let root = Scratch::new();
    clip(&root, "none").await;
    let provider = Arc::new(Directory {
        hold: 1,
        ..Arc::try_unwrap(Directory::new(&root.0)).ok().unwrap()
    });
    let mut operation = owner(
        &root,
        provider.clone(),
        "obs-contact-sheet",
        json!({"path":"source.mkv","times":[0.5],"width":80}),
    );
    assert!(
        timeout(Duration::from_millis(30), operation.finish())
            .await
            .is_err()
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    provider.release.notify_one();
    let result = operation.finish().await;
    assert_success(&result);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    root.clean();
}

#[tokio::test]
async fn malformed_media_bounded_sidecars_and_pre_cancel_are_refusals_without_publication() {
    let root = Scratch::new();
    fs::write(root.path("bad.mkv"), b"not a media container").unwrap();
    fs::write(root.path("old.mp4"), b"preserve previous output").unwrap();
    for tool in ["obs-contact-sheet", "obs-trim-take"] {
        let mut operation = owner(
            &root,
            Directory::new(&root.0),
            tool,
            json!({"path":"bad.mkv","apply":true,"outputPath":"old.mp4"}),
        );
        let result = operation.finish().await;
        assert_eq!(result["isError"], true);
        assert_eq!(operation.finish().await, result);
        root.clean();
    }
    clip(&root, "none").await;
    fs::write(root.path("source.take.json"), b"malformed JSON").unwrap();
    let arguments = json!({"path":"source.mkv","times":[0.5],"width":80});
    let expected = oracle(&root, "obs-contact-sheet", &arguments);
    let mut operation = owner(
        &root,
        Directory::new(&root.0),
        "obs-contact-sheet",
        arguments.clone(),
    );
    assert_eq!(operation.finish().await, expected["result"]);
    let sidecar = vec![b' '; 128];
    fs::write(root.path("source.take.json"), &sidecar).unwrap();
    let mut config = root.config();
    config.max_sidecar_bytes = 64;
    let mut operation = Operation::new(
        config,
        Directory::new(&root.0),
        Request::parse("obs-contact-sheet", &arguments).unwrap(),
        Cancellation::default(),
    )
    .unwrap();
    let result = operation.finish().await;
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("sidecar exceeds")
    );
    assert_eq!(fs::read(root.path("source.take.json")).unwrap(), sidecar);
    let cancellation = Cancellation::default();
    cancellation.cancel();
    let provider = Directory::new(&root.0);
    let mut operation = Operation::new(
        root.config(),
        provider.clone(),
        Request::parse("obs-contact-sheet", &arguments).unwrap(),
        cancellation,
    )
    .unwrap();
    assert_eq!(operation.finish().await["isError"], true);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fs::read(root.path("old.mp4")).unwrap(),
        b"preserve previous output"
    );
    root.clean();
}

#[test]
fn source_argument_defaults_unknown_fields_and_debug_redaction_are_explicit() {
    let request = Request::parse(
        "obs-trim-take",
        &json!({"path":"fixture-sensitive-name","unknown":"ignored"}),
    )
    .unwrap();
    assert!(!format!("{request:?}").contains("fixture-sensitive-name"));
    for arguments in [
        json!({"path":"source.mkv","times":null}),
        json!({"path":"source.mkv","width":79}),
        json!({"path":"source.mkv","columns":9}),
        json!({"path":"source.mkv","times":[-1]}),
    ] {
        assert!(Request::parse("obs-contact-sheet", &arguments).is_err());
    }
    for arguments in [
        json!({"path":"source.mkv","outputPath":null}),
        json!({"path":"source.mkv","minIdleSeconds":0}),
        json!({"path":"source.mkv","apply":"true"}),
    ] {
        assert!(Request::parse("obs-trim-take", &arguments).is_err());
    }
}

#[cfg(unix)]
#[test]
fn configuration_retains_ffprobe_and_work_directory_identity_before_operation_admission() {
    use std::os::unix::fs::PermissionsExt;
    for replacement in ["ffprobe", "work"] {
        let root = Scratch::new();
        root.config();
        let probe = root.path("approved-ffprobe");
        fs::copy(tool("ffprobe"), &probe).unwrap();
        fs::set_permissions(&probe, fs::Permissions::from_mode(0o700)).unwrap();
        let config = Config::new(&root.0, &tool("ffmpeg"), &probe, &root.path("work")).unwrap();
        if replacement == "ffprobe" {
            fs::rename(&probe, root.path("retired-ffprobe")).unwrap();
            fs::write(
                &probe,
                b"#!/bin/sh\nprintf forbidden > \"$(dirname \"$0\")/spawned\"\n",
            )
            .unwrap();
            fs::set_permissions(&probe, fs::Permissions::from_mode(0o700)).unwrap();
        } else {
            fs::rename(root.path("work"), root.path("retired-work")).unwrap();
            fs::create_dir(root.path("work")).unwrap();
            private(&root.path("work"));
            fs::write(root.path("work/human-file"), b"human replacement directory").unwrap();
        }
        let provider = Directory::new(&root.0);
        let operation = Operation::new(
            config,
            provider.clone(),
            Request::parse("obs-contact-sheet", &json!({"path":"source.mkv"})).unwrap(),
            Cancellation::default(),
        );
        assert!(
            operation.is_err(),
            "replacement {replacement} was silently admitted"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert!(!root.path("spawned").exists());
        if replacement == "ffprobe" {
            assert!(fs::read_to_string(&probe).unwrap().contains("forbidden"));
            assert!(root.path("retired-ffprobe").exists());
        } else {
            assert_eq!(
                fs::read(root.path("work/human-file")).unwrap(),
                b"human replacement directory"
            );
            assert!(root.path("retired-work").is_dir());
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn phase_admission_refuses_replaced_private_work_directory_after_operation_creation() {
    let root = Scratch::new();
    clip(&root, "none").await;
    let provider = Arc::new(Directory {
        hold: 1,
        ..Arc::try_unwrap(Directory::new(&root.0)).ok().unwrap()
    });
    let mut operation = owner(
        &root,
        provider.clone(),
        "obs-contact-sheet",
        json!({"path":"source.mkv","times":[0.5]}),
    );
    assert!(
        timeout(Duration::from_millis(20), operation.finish())
            .await
            .is_err()
    );
    fs::rename(root.path("work"), root.path("retired-work")).unwrap();
    fs::create_dir(root.path("work")).unwrap();
    private(&root.path("work"));
    fs::write(root.path("work/human-file"), b"preserve human directory").unwrap();
    provider.release.notify_one();
    let result = operation.finish().await;
    assert_eq!(result["isError"], true);
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("work directory identity changed")
    );
    assert_eq!(
        fs::read(root.path("work/human-file")).unwrap(),
        b"preserve human directory"
    );
    assert!(
        fs::read_dir(root.path("retired-work"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn fresh_directory_publication_fence_preserves_conflicts_and_cancellation_after_frontend_abandon()
 {
    for fault in ["directory", "cancel", "collision", "source"] {
        let root = Scratch::new();
        let outside = Scratch::new();
        clip(&root, "none").await;
        let provider = Arc::new(Directory {
            hold: 3,
            wrong: if fault == "directory" {
                Some(outside.0.clone())
            } else {
                None
            },
            ..Arc::try_unwrap(Directory::new(&root.0)).ok().unwrap()
        });
        let mut operation = owner(
            &root,
            provider.clone(),
            "obs-trim-take",
            json!({"path":"source.mkv","apply":true,"outputPath":"edited.mp4","minIdleSeconds":1,"keepSeconds":0.25,"holdBeforeChapterSeconds":0,"holdAfterChapterSeconds":0}),
        );
        let mut frontend = Box::pin(operation.finish());
        tokio::select! {_=wait_entered(&provider,3)=>{},result=&mut frontend=>panic!("operation completed before publication fence: {result}")};
        drop(frontend);
        assert!(!root.path("edited.mp4").exists());
        if fault == "cancel" {
            operation.cancel();
        }
        if fault == "collision" {
            fs::write(root.path("edited.mp4"), b"human winner").unwrap();
        }
        if fault == "source" {
            fs::write(root.path("source.mkv"), b"human replaced source").unwrap();
        }
        provider.release.notify_one();
        let result = operation.finish().await;
        assert_eq!(result["isError"], true, "{fault}: {result}");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
        if fault == "collision" {
            assert_eq!(fs::read(root.path("edited.mp4")).unwrap(), b"human winner");
        } else {
            assert!(!root.path("edited.mp4").exists());
        }
        root.clean();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn native_authority_refuses_external_symlink_hardlink_and_replaced_roots_but_allows_nested_files()
 {
    use std::os::unix::fs::symlink;
    let root = Scratch::new();
    let outside = Scratch::new();
    let source = clip(&root, "none").await;
    symlink(&source, root.path("alias.mkv")).unwrap();
    fs::hard_link(&source, root.path("shared.mkv")).unwrap();
    for path in ["alias.mkv", "shared.mkv", "../outside.mkv"] {
        let mut operation = owner(
            &root,
            Directory::new(&root.0),
            "obs-contact-sheet",
            json!({"path":path,"times":[0.5]}),
        );
        assert_eq!(operation.finish().await["isError"], true);
        root.clean();
    }
    fs::remove_file(root.path("shared.mkv")).unwrap();
    fs::create_dir(root.path("nested")).unwrap();
    fs::copy(&source, root.path("nested/clip.mkv")).unwrap();
    let mut operation = owner(
        &root,
        Directory::new(&root.0),
        "obs-contact-sheet",
        json!({"path":"nested/clip.mkv","times":[0.5],"width":80}),
    );
    assert_success(&operation.finish().await);
    let config = root.config();
    let moved = outside.path("moved-root");
    fs::rename(&root.0, &moved).unwrap();
    fs::create_dir(&root.0).unwrap();
    private(&root.0);
    let result = Operation::new(
        config,
        Directory::new(&root.0),
        Request::parse("obs-contact-sheet", &json!({"path":"source.mkv"})).unwrap(),
        Cancellation::default(),
    );
    assert!(result.is_err());
    fs::remove_dir(&root.0).unwrap();
    fs::rename(&moved, &root.0).unwrap();
    root.clean();
}

#[cfg(unix)]
#[tokio::test]
async fn abandoned_sampler_await_keeps_one_resistant_child_until_cancellation_and_join() {
    use std::os::unix::fs::PermissionsExt;
    let root = Scratch::new();
    clip(&root, "none").await;
    let helper = root.path("sampler.cjs");
    fs::write(&helper,r#"#!/usr/bin/env node
const fs=require('node:fs'),p=require('node:path'),root=p.dirname(process.argv[1]);process.on('SIGTERM',()=>{});fs.appendFileSync(p.join(root,'spawns'),'spawn\n');fs.writeFileSync(p.join(root,'ready'),String(process.pid));setInterval(()=>{},1000);
"#).unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
    root.config();
    let config = Config::new(&root.0, &helper, &tool("ffprobe"), &root.path("work")).unwrap();
    let mut operation = Operation::new(
        config,
        Directory::new(&root.0),
        Request::parse("obs-trim-take", &json!({"path":"source.mkv"})).unwrap(),
        Cancellation::default(),
    )
    .unwrap();
    let mut frontend = Box::pin(operation.finish());
    let ready = root.path("ready");
    let wait = async {
        let until = Instant::now() + Duration::from_secs(5);
        while !ready.exists() {
            assert!(Instant::now() < until);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    tokio::select! {_=wait=>{},result=&mut frontend=>panic!("sampler ended early: {result}")};
    drop(frontend);
    operation.cancel();
    let result = timeout(Duration::from_secs(3), operation.finish())
        .await
        .unwrap();
    assert_eq!(result["isError"], true);
    assert_eq!(operation.finish().await, result);
    assert_eq!(fs::read_to_string(root.path("spawns")).unwrap(), "spawn\n");
    let pid = fs::read_to_string(ready).unwrap();
    assert!(
        !Command::new("/bin/kill")
            .args(["-0", "--", pid.trim()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    root.clean();
}

// SPDX-License-Identifier: MIT
//! Approved-root replay snapshots. Decoders inspect only an exclusive private
//! copy; the named source and its bytes are checked again before publication.
use crate::media_verification::{self, LocalMediaInput, ProcessOptions, local_media_input_options};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, Metadata, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

const MAX_ENTRIES: usize = 2048;
const MAX_BYTES: u64 = 512 * 1024 * 1024;
#[derive(Clone)]
pub struct Verifier {
    root: PathBuf,
    root_identity: Metadata,
    work: PathBuf,
    work_identity: Metadata,
    ffmpeg: PathBuf,
    ffmpeg_identity: Metadata,
    ffprobe: PathBuf,
    ffprobe_identity: Metadata,
}
#[derive(Clone)]
pub struct Baseline {
    names: BTreeSet<PathBuf>,
}
pub struct Snapshot {
    original: PathBuf,
    source: File,
    before: Metadata,
    path: PathBuf,
    identity: Metadata,
    hash: String,
    bytes: u64,
    cleanup_allowed: bool,
}
#[derive(Debug)]
pub struct VerifyError {
    pub message: String,
    pub cleanup_unconfirmed: bool,
}
pub struct VerifiedSnapshot {
    snapshot: Snapshot,
    evidence: Value,
}
impl From<String> for VerifyError {
    fn from(message: String) -> Self {
        Self {
            message,
            cleanup_unconfirmed: false,
        }
    }
}
impl From<&str> for VerifyError {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl From<media_verification::MediaError> for VerifyError {
    fn from(error: media_verification::MediaError) -> Self {
        Self {
            cleanup_unconfirmed: error.cleanup_issue().is_some(),
            message: error.to_string(),
        }
    }
}
#[cfg(unix)]
fn same(a: &Metadata, b: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}
#[cfg(not(unix))]
fn same(_: &Metadata, _: &Metadata) -> bool {
    false
}
fn unchanged(a: &Metadata, b: &Metadata) -> bool {
    same(a, b) && a.len() == b.len() && a.modified().ok() == b.modified().ok()
}
fn independent(m: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        m.is_file() && !m.file_type().is_symlink() && m.nlink() == 1
    }
    #[cfg(not(unix))]
    {
        let _ = m;
        false
    }
}
fn normalized(path: &Path) -> bool {
    path.is_absolute()
        && !path
            .components()
            .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
}
fn identity(path: &Path) -> Result<Metadata, String> {
    fs::symlink_metadata(path).map_err(|_| "Replay filesystem identity is unavailable".into())
}
fn budget(deadline: Instant, cancel: &crate::stage_transport::Cancellation) -> Result<(), String> {
    if Instant::now() >= deadline || cancel.is_cancelled() {
        Err("Replay verification cancelled or original deadline expired".into())
    } else {
        Ok(())
    }
}
impl Verifier {
    pub fn new(root: &Path, ffmpeg: &Path, ffprobe: &Path, work: &Path) -> Result<Self, String> {
        let checked = |path: &Path, directory: bool| -> Result<Metadata, String> {
            let info = identity(path)?;
            if !normalized(path)
                || path.canonicalize().ok().as_deref() != Some(path)
                || info.file_type().is_symlink()
                || (directory && !info.is_dir())
                || (!directory && !info.is_file())
            {
                return Err("Replay configuration requires canonical existing directories and media executables".into());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if !directory && info.permissions().mode() & 0o111 == 0 {
                    return Err("Replay media executable is not executable".into());
                }
            }
            Ok(info)
        };
        Ok(Self {
            root: root.to_owned(),
            root_identity: checked(root, true)?,
            work: work.to_owned(),
            work_identity: checked(work, true)?,
            ffmpeg: ffmpeg.to_owned(),
            ffmpeg_identity: checked(ffmpeg, false)?,
            ffprobe: ffprobe.to_owned(),
            ffprobe_identity: checked(ffprobe, false)?,
        })
    }
    fn check(&self) -> Result<(), String> {
        for (path, before) in [
            (&self.root, &self.root_identity),
            (&self.work, &self.work_identity),
            (&self.ffmpeg, &self.ffmpeg_identity),
            (&self.ffprobe, &self.ffprobe_identity),
        ] {
            let current = identity(path)?;
            if !same(before, &current)
                || current.file_type().is_symlink()
                || (before.is_file() && !unchanged(before, &current))
            {
                return Err("Replay root, workspace or media executable identity changed".into());
            }
        }
        Ok(())
    }
    pub async fn baseline(
        self: &Arc<Self>,
        deadline: Instant,
        cancel: crate::stage_transport::Cancellation,
    ) -> Result<Baseline, String> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            fn walk(
                path: &Path,
                depth: usize,
                names: &mut BTreeSet<PathBuf>,
                deadline: Instant,
                cancel: &crate::stage_transport::Cancellation,
            ) -> Result<(), String> {
                budget(deadline, cancel)?;
                if depth > 8 {
                    return Err("Replay baseline directory depth limit exceeded".into());
                }
                for entry in
                    fs::read_dir(path).map_err(|_| "Replay directory inventory is unavailable")?
                {
                    budget(deadline, cancel)?;
                    let path = entry
                        .map_err(|_| "Replay directory inventory is incomplete")?
                        .path();
                    if names.len() >= MAX_ENTRIES {
                        return Err("Replay baseline inventory exceeds 2048 entries".into());
                    }
                    names.insert(path.clone());
                    let info = identity(&path)?;
                    if info.file_type().is_symlink() || (!info.is_file() && !info.is_dir()) {
                        return Err("Replay inventory contains an unsupported entry".into());
                    }
                    if info.is_dir() && !info.file_type().is_symlink() {
                        walk(&path, depth + 1, names, deadline, cancel)?;
                    }
                }
                Ok(())
            }
            this.check()?;
            let probe = this.root.join(format!(
                ".replay-writable-{}",
                crate::runtime_identity::uuid_v4()
                    .map_err(|_| "Replay probe identity unavailable")?
            ));
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(&probe)
                .map_err(|_| "Approved replay directory is not writable")?;
            let probe_identity = file
                .metadata()
                .map_err(|_| "Replay writable probe identity unavailable")?;
            let writable = file.write_all(b"0").and_then(|_| file.sync_all());
            drop(file);
            if !same(&identity(&probe)?, &probe_identity) {
                return Err("Replay writable probe identity changed; cleanup withheld".into());
            }
            fs::remove_file(&probe).map_err(|_| "Replay writable probe cleanup is unconfirmed")?;
            writable.map_err(|_| "Approved replay directory writable check failed")?;
            let mut names = BTreeSet::new();
            walk(&this.root, 0, &mut names, deadline, &cancel)?;
            this.check()?;
            Ok(Baseline { names })
        })
        .await
        .map_err(|_| "Replay inventory worker did not join")?
    }
    fn source(&self, path: &Path) -> Result<Metadata, String> {
        self.check()?;
        if !normalized(path) || !path.starts_with(&self.root) || path == self.root {
            return Err("Replay file is outside the approved recording root".into());
        }
        let mut directory = self.root.clone();
        let relative = path
            .strip_prefix(&self.root)
            .map_err(|_| "Replay path is outside the approved root")?;
        let parts = relative.components().collect::<Vec<_>>();
        for part in parts.iter().take(parts.len().saturating_sub(1)) {
            directory.push(part.as_os_str());
            let info = identity(&directory)?;
            if !info.is_dir()
                || info.file_type().is_symlink()
                || directory.canonicalize().ok().as_deref() != Some(directory.as_path())
            {
                return Err("Replay parent is not a canonical independent directory".into());
            }
        }
        let info = identity(path)?;
        if !independent(&info) || info.len() == 0 || info.len() > MAX_BYTES {
            return Err(
                "Replay must be a nonempty independent regular file of at most 512 MiB".into(),
            );
        }
        Ok(info)
    }
    pub async fn snapshot(
        self: &Arc<Self>,
        path: PathBuf,
        baseline: Baseline,
        id: String,
        deadline: Instant,
        cancel: crate::stage_transport::Cancellation,
    ) -> Result<Snapshot, VerifyError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            budget(deadline, &cancel)?;
            if baseline.names.contains(&path) {
                return Err("Replay filename already existed before the save attempt; overwrite freshness is unverified".into());
            }
            let before = this.source(&path)?;
            let mut source = File::open(&path).map_err(|_| "Replay file cannot be opened")?;
            if !unchanged(&before, &source.metadata().map_err(|_| "Replay descriptor is unavailable")?) {
                return Err("Replay source changed before snapshot".into());
            }
            // Clone before creating a private file so this failure has no cleanup duty.
            let retained_source = source.try_clone().map_err(|_| "Replay descriptor clone failed")?;
            let copy = this.work.join(format!("replay-{id}.snapshot"));
            let mut options = OpenOptions::new();
            options.create_new(true).write(true).read(true);
            #[cfg(unix)] {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut target = options.open(&copy).map_err(|_| "Private replay snapshot could not be created")?;
            let copy_identity = target.metadata().map_err(|_| VerifyError {
                message: "Replay snapshot identity unavailable; cleanup withheld".into(),
                cleanup_unconfirmed: true,
            })?;
            let mut snapshot = Snapshot {
                original: path,
                source: retained_source,
                before: before.clone(),
                path: copy,
                identity: copy_identity,
                hash: String::new(),
                bytes: 0,
                cleanup_allowed: true,
            };
            let copied = (|| -> Result<(), VerifyError> {
                let mut hash = Sha256::new();
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    budget(deadline, &cancel)?;
                    let size = source.read(&mut buffer).map_err(|_| "Replay snapshot read failed")?;
                    if size == 0 { break; }
                    snapshot.bytes += size as u64;
                    if snapshot.bytes > MAX_BYTES { return Err("Replay snapshot exceeds 512 MiB".into()); }
                    target.write_all(&buffer[..size]).map_err(|_| "Replay snapshot write failed")?;
                    hash.update(&buffer[..size]);
                }
                snapshot.hash = format!("{:x}", hash.finalize());
                snapshot.identity = target.metadata().map_err(|_| "Replay snapshot identity unavailable")?;
                if snapshot.bytes != before.len() { return Err("Replay grew or shrank during snapshot".into()); }
                this.recheck(&mut snapshot, deadline, &cancel)?;
                Ok(())
            })();
            // No helper can use this descriptor, and close it before unlinking.
            drop(target);
            match copied {
                Ok(()) => Ok(snapshot),
                Err(error) => Err(snapshot.retire_error(error)),
            }
        })
        .await
        .map_err(|_| VerifyError {
            message: "Replay snapshot worker did not join; cleanup is unconfirmed".into(),
            cleanup_unconfirmed: true,
        })?
    }
    fn recheck(
        &self,
        snapshot: &mut Snapshot,
        deadline: Instant,
        cancel: &crate::stage_transport::Cancellation,
    ) -> Result<(), String> {
        let current = self.source(&snapshot.original)?;
        if !unchanged(&current, &snapshot.before)
            || !unchanged(
                &snapshot
                    .source
                    .metadata()
                    .map_err(|_| "Replay descriptor unavailable")?,
                &snapshot.before,
            )
        {
            return Err("Replay source identity, size or modification time changed".into());
        }
        snapshot
            .source
            .seek(SeekFrom::Start(0))
            .map_err(|_| "Replay descriptor seek failed")?;
        let mut hash = Sha256::new();
        let mut bytes = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            budget(deadline, cancel)?;
            let size = snapshot
                .source
                .read(&mut buffer)
                .map_err(|_| "Replay recheck failed")?;
            if size == 0 {
                break;
            }
            bytes += size as u64;
            if bytes > MAX_BYTES {
                return Err("Replay grew during verification".into());
            }
            hash.update(&buffer[..size]);
        }
        if bytes != snapshot.bytes
            || format!("{:x}", hash.finalize()) != snapshot.hash
            || !unchanged(&self.source(&snapshot.original)?, &snapshot.before)
        {
            return Err("Replay bytes changed during verification".into());
        }
        let copy = identity(&snapshot.path)?;
        if !independent(&copy) || !unchanged(&copy, &snapshot.identity) {
            return Err("Private replay snapshot changed".into());
        }
        Ok(())
    }
    pub async fn verify(
        self: &Arc<Self>,
        mut snapshot: Snapshot,
        deadline: Instant,
        cancel: crate::stage_transport::Cancellation,
    ) -> Result<VerifiedSnapshot, VerifyError> {
        let result = self.verify_snapshot(&mut snapshot, deadline, cancel).await;
        match result {
            Ok(evidence) => Ok(VerifiedSnapshot { snapshot, evidence }),
            Err(error) => Err(snapshot.retire_error(error)),
        }
    }
    async fn verify_snapshot(
        &self,
        snapshot: &mut Snapshot,
        deadline: Instant,
        cancel: crate::stage_transport::Cancellation,
    ) -> Result<Value, VerifyError> {
        budget(deadline, &cancel)?;
        self.check()?;
        let options = || ProcessOptions {
            timeout: deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(1)),
            ..Default::default()
        };
        let args = ["-v", "error"]
            .map(std::ffi::OsString::from)
            .into_iter()
            .chain(local_media_input_options(LocalMediaInput::Recording))
            .chain([
                "-i".into(),
                snapshot.path.as_os_str().to_owned(),
                "-show_format".into(),
                "-show_streams".into(),
                "-of".into(),
                "json".into(),
            ])
            .collect::<Vec<_>>();
        let mut probe = media_verification::capture_bounded_media_process(
            &self.ffprobe,
            &args,
            options(),
            1024 * 1024,
        )
        .map_err(VerifyError::from)?;
        let data = tokio::select! {result=probe.finish()=>result,_=cancel.cancelled()=>{probe.cancel();probe.finish().await}};
        let data = data.map_err(VerifyError::from)?;
        drop(probe);
        budget(deadline, &cancel)?;
        let data: Value =
            serde_json::from_slice(&data).map_err(|_| "Replay probe returned invalid JSON")?;
        let duration = data["format"]["duration"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| data["format"]["duration"].as_f64())
            .filter(|n| n.is_finite() && *n > 0.0)
            .ok_or("Replay duration is missing, nonpositive or invalid")?;
        let streams = data["streams"]
            .as_array()
            .ok_or("Replay streams are unavailable")?;
        let video = streams
            .iter()
            .filter(|s| s["codec_type"] == "video")
            .collect::<Vec<_>>();
        if streams.len() > 16
            || video.len() != 1
            || video[0]["width"].as_u64().unwrap_or(0) == 0
            || video[0]["height"].as_u64().unwrap_or(0) == 0
            || video[0]["width"].as_u64().unwrap_or(u64::MAX) > 8192
            || video[0]["height"].as_u64().unwrap_or(u64::MAX) > 8192
            || video[0]["width"]
                .as_u64()
                .and_then(|w| video[0]["height"].as_u64().and_then(|h| w.checked_mul(h)))
                .is_none_or(|pixels| pixels > 33_554_432)
        {
            return Err(
                "Replay must contain exactly one video stream with positive dimensions".into(),
            );
        }
        let audio = streams
            .iter()
            .filter(|s| s["codec_type"] == "audio")
            .count();
        if audio > 6 {
            return Err("Replay verification supports at most six audio streams".into());
        }
        let mut decode = media_verification::run_bounded_media_process(
            &self.ffmpeg,
            &media_verification::strict_decode_args(&snapshot.path),
            options(),
        )
        .map_err(VerifyError::from)?;
        let decoded = tokio::select! {result=decode.finish()=>result,_=cancel.cancelled()=>{decode.cancel();decode.finish().await}};
        decoded.map_err(VerifyError::from)?;
        drop(decode);
        budget(deadline, &cancel)?;
        let evidence = json!({"path":snapshot.original,"sha256":snapshot.hash,"bytes":snapshot.bytes,"durationSeconds":duration,"audioStreams":audio,"videoStreams":1});
        Ok(evidence)
    }
    /// The source descriptor and private copy remain owned across final OBS
    /// corroboration. Rehash the original only after that asynchronous boundary.
    pub async fn finish(
        self: &Arc<Self>,
        verified: VerifiedSnapshot,
        deadline: Instant,
        cancel: crate::stage_transport::Cancellation,
    ) -> Result<Value, VerifyError> {
        let VerifiedSnapshot {
            mut snapshot,
            evidence,
        } = verified;
        let this = self.clone();
        let (mut snapshot, result) = tokio::task::spawn_blocking(move || {
            let result = this.recheck(&mut snapshot, deadline, &cancel);
            (snapshot, result)
        })
        .await
        .map_err(|_| VerifyError {
            message: "Replay recheck worker did not join; cleanup is unconfirmed".into(),
            cleanup_unconfirmed: true,
        })?;
        match result {
            Ok(()) => {
                snapshot.cleanup()?;
                Ok(evidence)
            }
            Err(error) => Err(snapshot.retire_error(error.into())),
        }
    }
    /// Retire a verified copy when final OBS corroboration refuses publication.
    /// Its cleanup fate must be combined with the caller's primary error.
    pub fn discard(&self, verified: VerifiedSnapshot) -> Result<(), VerifyError> {
        let mut snapshot = verified.snapshot;
        snapshot.cleanup()
    }
}
impl Snapshot {
    fn cleanup(&mut self) -> Result<(), VerifyError> {
        // Disable retries before even looking up the path: failed identity or
        // unlink evidence must remain sticky if permissions later recover.
        let allowed = std::mem::replace(&mut self.cleanup_allowed, false);
        if !allowed {
            return Err(VerifyError {
                message: "Replay snapshot cleanup withheld because helper or filesystem cleanup is unconfirmed".into(),
                cleanup_unconfirmed: true,
            });
        }
        let current = identity(&self.path).map_err(|_| VerifyError {
            message: "Replay snapshot cleanup identity is unavailable; cleanup withheld".into(),
            cleanup_unconfirmed: true,
        })?;
        if !same(&current, &self.identity) || !independent(&current) {
            return Err(VerifyError {
                message: "Replay snapshot cleanup identity is unconfirmed; cleanup withheld".into(),
                cleanup_unconfirmed: true,
            });
        }
        fs::remove_file(&self.path).map_err(|_| VerifyError {
            message: "Replay snapshot cleanup is unconfirmed".into(),
            cleanup_unconfirmed: true,
        })
    }
    fn retire_error(&mut self, mut error: VerifyError) -> VerifyError {
        if error.cleanup_unconfirmed {
            self.cleanup_allowed = false;
        }
        if let Err(cleanup) = self.cleanup() {
            error.message = format!("{}; {}", error.message, cleanup.message);
            error.cleanup_unconfirmed = true;
        }
        error
    }
}
// Snapshot deliberately has no filesystem-mutating Drop. All normal return
// paths explicitly retire it and report cleanup fate. Abandonment (e.g. worker
// panic) retains the file; no destructor may retry after uncertainty.

#[cfg(all(test, unix))]
mod cleanup_tests {
    use super::*;

    struct Fixture {
        root: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join(format!(
                    "replay-cleanup-{}",
                    crate::runtime_identity::uuid_v4().unwrap()
                ));
            fs::create_dir_all(&root).unwrap();
            Self { root }
        }
        fn snapshot(&self) -> Snapshot {
            let original = self.root.join("original");
            let path = self.root.join("snapshot");
            fs::write(&original, b"source").unwrap();
            fs::write(&path, b"private").unwrap();
            Snapshot {
                source: File::open(&original).unwrap(),
                before: identity(&original).unwrap(),
                original,
                identity: identity(&path).unwrap(),
                path,
                hash: String::new(),
                bytes: 7,
                cleanup_allowed: true,
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn confirmed_cleanup_removes_only_owned_snapshot() {
        let fixture = Fixture::new();
        let mut snapshot = fixture.snapshot();
        snapshot.cleanup().unwrap();
        assert!(!snapshot.path.exists());
        assert!(snapshot.original.exists());
    }

    #[test]
    fn cleanup_identity_uncertainty_retains_original_even_after_identity_restored() {
        let fixture = Fixture::new();
        let mut snapshot = fixture.snapshot();
        let held = fixture.root.join("held");
        fs::rename(&snapshot.path, &held).unwrap();
        fs::write(&snapshot.path, b"foreign").unwrap();
        let error = snapshot.retire_error("primary verification failed".into());
        assert!(error.cleanup_unconfirmed);
        assert!(error.message.starts_with("primary verification failed;"));
        assert_eq!(fs::read(&snapshot.path).unwrap(), b"foreign");
        // Restore the exact original inode; neither a repeated cleanup nor Drop
        // may turn recovered access into authority to erase retained evidence.
        fs::remove_file(&snapshot.path).unwrap();
        fs::rename(&held, &snapshot.path).unwrap();
        assert!(snapshot.cleanup().unwrap_err().cleanup_unconfirmed);
        let path = snapshot.path.clone();
        drop(snapshot);
        assert_eq!(fs::read(path).unwrap(), b"private");
    }

    #[test]
    fn unconfirmed_helper_cleanup_never_attempts_snapshot_unlink() {
        let fixture = Fixture::new();
        let mut snapshot = fixture.snapshot();
        let error = snapshot.retire_error(VerifyError {
            message: "decoder failed with an unjoined pipe".into(),
            cleanup_unconfirmed: true,
        });
        assert!(error.cleanup_unconfirmed);
        let path = snapshot.path.clone();
        drop(snapshot);
        assert_eq!(fs::read(path).unwrap(), b"private");
    }

    #[test]
    fn confirmed_error_retirement_cleans_copy_and_preserves_primary_failure() {
        let fixture = Fixture::new();
        let mut snapshot = fixture.snapshot();
        let error = snapshot.retire_error("invalid probe JSON".into());
        assert_eq!(error.message, "invalid probe JSON");
        assert!(!error.cleanup_unconfirmed);
        assert!(!snapshot.path.exists());
    }
}

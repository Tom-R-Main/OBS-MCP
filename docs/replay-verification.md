# Verified replay files

`obs-save-replay-verified` requests one save from an already-running replay buffer and verifies a new local file. It never starts or stops an output and never retries a save. The existing `obs-save-replay-buffer` remains an acknowledgement-only command.

```json
{"name":"obs-save-replay-verified","arguments":{"timeoutSeconds":15}}
```

Use the Production profile with an approved private state directory, approved recording root, FFmpeg and ffprobe. Native discovery binds existing installed executables; no tool is downloaded. Advanced, read-only mode and unavailable host adapters exclude this workflow. It retains the existing separation between guarded file/Take owners and standalone Stream controls.

## What a verified result establishes

The result separates `requestAccepted`, `saveEventObserved`, `newPathObserved`, `strictDecode` and `fileStableDuringVerification`. A verified `file` contains its path, SHA-256, byte count, duration and audio/video stream counts. Duration must be finite and positive. The file must have one video stream, at most six audio streams and at most 16 streams total. Video dimensions must be positive, no larger than 8,192 per axis and at most 33,554,432 pixels. Strict decoding covers the video and every audio stream; these admission limits do not establish a process RSS bound.

OBS's `ReplayBufferSaved` event contains a filename, without a request identifier. `GetLastReplayBufferReplay` rereads that same global filename. Consequently `requestAttribution` always remains `unverified`, including when all file checks pass. A save already in flight or another controller can produce the observed file. The tool establishes that a new local file was observed and verified during this attempt; it cannot prove that this request caused it.

The save must be acknowledged. A lost acknowledgement remains Unknown even if a playable file appears. A known acknowledgement remains accepted if later file checks fail. Neither outcome authorizes automatic replay.

## Freshness and resource ownership

Before dispatch, the workflow checks capabilities and active replay status, tests directory writability with an exclusive temporary probe, records the previous replay path and inventories the approved root. Inventory is limited to 2,048 entries and eight nested directory levels; incomplete or overflowing inventories refuse.

The shipping Rust service preallocates a saved-event subscription on its sole OBS observer, then arms it inside the final socket dispatch guard. It binds the physical event cursor, connection and original deadline. Events from before that boundary do not qualify. Malformed, oversized or multiple eligible events refuse. The final last-replay path must agree with the event.

A candidate must be absent from the initial inventory and differ from the previous replay path. Existing filenames, including overwritten or replaced files, cannot receive verified status. Paths must remain under the approved root, without symlink traversal or shared hardlinks. Empty files and files larger than 512 MiB refuse.

The approved root limits this service's file inspection; it does not change OBS's save destination or naming settings. OBS naming can overwrite an existing replay file before this workflow receives its filename. Refusing verification afterward cannot prevent or undo that overwrite. Configure suitable replay naming and output directories in OBS. This workflow changes neither setting and makes no no-clobber promise for OBS's Save operation. Trim export has its separate no-clobber publication checks.

Verification reads an identity-checked source descriptor into an exclusive private snapshot while hashing it. Probe and decode inspect that snapshot. The descriptor and copy remain owned across final OBS corroboration; the source bytes and named-file identity are checked again immediately before completing the result. These are observations at checked boundaries, not a filesystem sandbox against a hostile same-user process or a promise that the file can never change afterward.

The original 1–50 second budget covers initialization admission, the save, saved-event wait and verification. Cancellation does not abandon a dispatched acknowledgement or an owned helper. Cleanup may take its bounded joining grace beyond the work deadline. An unconfirmed helper/pipe shutdown retains its private snapshot and unfinished journal; another verified save refuses rather than discarding evidence or reusing its permission.

## Restart and failure evidence

Generic transport receipts keep their existing schema and `postconditionObserved:false`. Separate version-1 replay observations live under `replay-verifications` in the approved state directory. Immutable `ID.started.json` and `ID.result.json` records bind the operation to its endpoint hash; an exclusive `active` directory prevents overlapping verified saves in that namespace.

A process interruption can leave `active` and started evidence behind. Restart does not clear them or repeat the save. Inspect OBS and retained evidence before choosing a separate recovery action. This tranche deliberately supplies no automatic reset, adoption or journal-clearing command. A finalized verification observation is historical evidence, not a capability to reuse or overwrite that file.

## Verification scope

The public tests use the actual native MCP entry point in both protocol eras, an owned loopback OBS fixture and generated multitrack media. They challenge early/held acknowledgements, missing/stale/foreign/multiple events, unsafe paths, changing files, failed decoders, lost replies, cancellation, EOF and restart. No live replay buffer, desktop, microphone or platform stream is used.

The retained TypeScript implementation is a development oracle. Its direct in-memory MCP endpoint is tested in the legacy era. It observes event-loop receipt order and generation rather than Rust's physical cursor, and its interrupted-operation barriers are per client rather than durable runtime journals. Its connection establishment also retains its existing handshake deadline. Those limits do not qualify the shipping Rust owner by proxy.

File verification does not certify editorial usefulness, correct camera/window capture, silence, AV synchronization, color fidelity, configured replay length or power-loss durability. Real OBS and destination checks remain separate.

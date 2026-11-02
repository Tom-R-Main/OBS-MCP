# OBS MCP

[![CI](https://github.com/Tom-R-Main/OBS-MCP/actions/workflows/ci.yml/badge.svg)](https://github.com/Tom-R-Main/OBS-MCP/actions/workflows/ci.yml)

OBS MCP helps agents and operators inspect OBS Studio, plan scene changes, and supervise recordings. Preview a change before taking it to program, watch a bounded take as it records, and retain operation receipts when a command's outcome needs review.

Start with `obs-observe`, resolve a layout with `obs-plan-scene-template`, then stage and inspect the preview before calling `obs-take-scene`. The observation tool and `obs://status` resource share their consistency checks and missing-evidence contract. See [production workflows and Linux support](docs/production-assistant.md) for examples, platform boundaries, and verification limits.

The server uses MCP 2026-07-28 and accepts legacy 2025-era clients. Tool discovery remains available while OBS is closed.

## Native runtime and qualification

The executable runs the retained Rust service directly over stdio or loopback HTTP. The native MCPB layout contains no Node runtime, JavaScript SDK or TypeScript server payload. TypeScript remains a development oracle.

Production is the default immutable profile and exposes up to 164 ordinary tools with approved host/media adapters. Advanced exposes 163 and excludes guarded Take, media, Stage and recovery owners. Their union contains 178 ordinary names; stdio dynamic mode can add three group controllers. Filters, read-only mode and available adapters narrow discovery. See [native profiles](docs/native-profiles.md).

On 2 November 2026, the optimized native candidate passed the normal native gate: 1,045 default-feature Rust cases, 213 all-feature bin/library cases, four resolver process contracts, and 1,002 public cases with no skips. The public suite includes 17 actual HTTP-entry and 47 TCP cases. The development unit suite passed 735 cases. These suites overlap; their counts are not a unique-test total.

The matching macOS MCPB passed three extracted-package checks, including localhost resolution with a Node trap, then a bounded live OBS journey on that extracted executable. Its 2.837-second silent A/B fixture clip passed strict decoding and sampled-content checks; the screenshots/contact sheet were reviewed, recovery preserved a conflict, and the original state snapshots matched after restoration and cleanup. Linux qualification is in progress without a platform/runtime acceptance claim; production TLS, final-head CI and client activation remain pending. Native runtime cutover does not yet provide full Source transport parity: encrypted `wss:` endpoints that previously used the SDK route now refuse in default builds. [Cutover status](docs/full-rust-cutover.md) binds these observations to exact artifact hashes; [native distribution](docs/native-distribution.md) gives setup and rebuild commands.

## How it works

```text
MCP clients  <-- stdio or loopback HTTP -->  Rust OBS MCP  <-- WebSocket v5 -->  OBS Studio
```

OBS MCP runs as a separate MCP server process using the native executable. It does not patch OBS or require an OBS plugin; it uses the WebSocket server built into OBS Studio. MCP handles tool discovery, schemas, results, and approval hints. OBS WebSocket handles the live application state.

## What is covered

The 178-name ordinary catalog spans both profiles; it is not a promise of 178 simultaneous tools in one process.

- Descriptions for all 147 request types in the pinned OBS WebSocket protocol; profile policy controls dispatch
- Explicit read-only, destructive, idempotent, and open-world annotations on every tool
- Runtime capability checks against the requests advertised by the connected OBS instance
- Structured MCP results for automation, with readable text retained for people and older clients
- Source screenshots returned as MCP image content instead of clipped base64 text
- Protocol inspection through `obs-describe-request`
- Advanced-profile raw, batch, vendor and hotkey tools with explicit policy and durable guarded-reentry barriers
- A host-specific `.mcpb` built from checked native assets and corresponding Rust source, with profile-conditional inventory

Use the explicit tools for normal workflows. Advanced raw tools accept the bundled request names and retain destructive/open-world policy even when a wrapped request is a read. They do not borrow Production Take, Stop, file or recovery authority.

## Requirements

- A qualified native executable for your host (or the pinned Rust toolchain to build it)
- An existing approved private state directory; optional recording root and trusted FFmpeg/ffprobe for local media workflows
- OBS Studio 28 or newer with the WebSocket server enabled
- An MCP client that supports MCP 2026-07-28 or the compatible legacy protocol

OBS includes WebSocket v5. Enable it under **Tools > WebSocket Server Settings**. Newer OBS requests are available only when the connected version advertises them; unsupported calls return a descriptive error.

## Install from source

Build the runtime directly from its checked native inputs; Node and a TypeScript
build are unnecessary:

```sh
cd native
RUSTUP_AUTO_INSTALL=0 cargo build --locked --offline --release --bin obs-mcp-native
```

The pinned toolchain and Cargo sources must already be available. A missing
cache is an explicit prerequisite, not permission for an automatic fetch.
Create and approve a safe state directory inside your chosen project, then
configure the absolute native executable and `OBS_MCP_STATE_DIR` in your MCP
client. [Native distribution](docs/native-distribution.md) gives the complete
setup, optional recording/media paths and immutable profile behavior. No home
directory is initialized or history erased as part of source qualification.

## Verify the connection

Open OBS, start a new agent session, and ask:

> Check the OBS connection. Report the OBS version, current scene, stream status, and recording status. Do not change anything.

The client should request only read-only tools and return live values from OBS. If discovery works while OBS is closed, start OBS and request the read-only connection check again. Native operation history never authorizes automatic mutation replay; a lost owned session may require a fresh MCP process while retained unknown effects remain refused.

## Install the MCPB package

With the explicit development archive tools installed, `node scripts/pack.mjs`
builds `dist/obs-studio-native.mcpb` for the build host. It compiles from checked
native assets, keeps corresponding Rust source/configuration and notices, and
uses no staging dependency install or Source server build. Node/MCPB CLI are
development packaging tools; the installed extension contains no Node, JavaScript
SDK or server payload.

The MCPB setup requires an existing approved private state directory and exposes
profile and optional recording/media paths. Discovery works while OBS is offline
after valid local startup. The 2 November macOS candidate passed extraction and
a bounded live journey; a changed artifact or another platform needs its own
checks. Package contents alone prove neither production TLS nor Linux desktop
support. See [native distribution](docs/native-distribution.md).

## Configuration

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `OBS_WEBSOCKET_URL` | No | `ws://localhost:4455` | Address of the OBS WebSocket server |
| `OBS_WEBSOCKET_PASSWORD` | Only when OBS authentication is enabled | None | Password configured in OBS |
| `OBS_MCP_READ_OBS_CONFIG` | No | `false` | When `OBS_WEBSOCKET_PASSWORD` is unset, read the password OBS saved in its own `obs-websocket/config.json`. Only for a server on the same machine and user account as OBS |
| `OBS_MCP_MAX_SCREENSHOT_BYTES` | No | `4194304` | Maximum decoded size of a screenshot returned through MCP; the hard ceiling is 6 MiB |
| `OBS_MCP_TOOLSETS` | No | all tools | Comma-separated tool groups to register: `all`, `core`, or any group listed under [Choosing tools](#choosing-tools) |
| `OBS_MCP_TOOLS` | No | none | Comma-separated tool names to register in addition to `OBS_MCP_TOOLSETS` |
| `OBS_MCP_READ_ONLY` | No | `false` | Register only read-only tools; overrides both lists |
| `OBS_MCP_DYNAMIC_TOOLSETS` | No | `false` | Start with only the `OBS_MCP_TOOLSETS` groups (default `general`) and let the model turn others on and off (see [Choosing tools](#choosing-tools)) |
| `OBS_MCP_CONFIRM_LIVE` | No | `false` | Ask before starting a stream/output or stopping or toggling a stream, recording, or output (see [Confirming live actions](#confirming-live-actions)) |
| `OBS_MCP_HTTP_PORT` | No | none (stdio) | Serve MCP over Streamable HTTP at `http://127.0.0.1:<port>/mcp` instead of stdio, so several agents share one server (see [Sharing one server](#sharing-one-server)); `0` picks a free port |
| `OBS_MCP_HTTP_TOKEN` | No | none | With `OBS_MCP_HTTP_PORT`, require `Authorization: Bearer <token>` on every request |
| `OBS_MCP_STATE_DIR` | An existing approved namespace is required | Prefer an explicit canonical path | Endpoint-bound snapshots, recovery journals and receipts; startup does not create an unsolicited namespace or clear history |
| `OBS_MCP_NATIVE_PROFILE` | No | `production` | Immutable `production` or `advanced` capability profile |
| `OBS_MCP_NATIVE_RECORDING_ROOT` | For Production recording/file workflows | None | Existing approved recording directory; does not grant recording or file authority |
| `OBS_MCP_NATIVE_FFMPEG` / `OBS_MCP_NATIVE_FFPROBE` | For media workflows | Trusted executable on PATH | Explicit existing media-tool bindings; the package installs neither |

Passwords and HTTP tokens are not printed. Stdio reserves stdout for MCP; HTTP keeps it empty and announces its loopback endpoint on stderr. Default builds refuse encrypted `wss:` OBS endpoints before connection. Conditional macOS Network.framework and Linux GnuTLS backends remain behind `platform-tls-evaluation`; see [transport status](docs/native-distribution.md#dns-and-encrypted-obs-endpoints).

## Tool surface

The profile-conditional tool catalog covers:

- server status, version information, statistics, hotkeys, and studio mode
- scenes, groups, sources, filters, and scene items
- input settings, audio tracks, volume, mute state, media controls, and deinterlacing
- recording, streaming, replay buffer, virtual camera, and output settings
- transitions, transition overrides, the transition cursor, and the T-Bar
- canvases, screenshots, profiles, scene collections, and persistent data
- a recording preflight (`obs-preflight`) that catches the conditions under which OBS silently refuses or botches a recording
- `obs-audit-scene`, which inspects one selected scene for missing or hidden required items, incomplete observations, and structural problems without changing OBS
- `obs-record-clip`, which preflights, records up to 50 seconds with chapter markers, stops, and checks the file's length and audio with ffprobe/ffmpeg when they are installed. While it records, it watches the take (see [Watching a take](#watching-a-take))
- `obs-take-start`, `obs-take-mark`, `obs-take-status`, and `obs-take-stop` for watched recordings, with chapters and duration policy (see [Watching a take](#watching-a-take))
- `obs-trim-take`, which cuts dead time out of a recording, and `obs-contact-sheet`, which tiles frames from it into one image (see [Trimming a take](#trimming-a-take))
- `obs-apply-scene`, which takes a description of a scene and makes OBS match it (see [Describing a scene](#describing-a-scene))
- `obs-snapshot` and `obs-restore`, which save the state of inputs and scene items and later undo changes to them (see [Undoing changes](#undoing-changes))
- `obs-capture-window`, which plans or selects macOS or Linux X11 windows from an existing capture inventory, or fits an existing user-authorized Wayland portal capture
- input and transform changes that return the resulting settings, size, and an optional screenshot, warning when a source renders at 0×0
- protocol description and Advanced raw/batch tools, which send several requests in one message, in order or one per rendered frame (so a change to two sources lands on the same frame), with `Sleep` between them. OBS's parallel mode is not offered: in obs-websocket 5.7, concurrent parallel batches deadlock its WebSocket server until OBS restarts

Tools that send a single OBS request declare an `outputSchema` generated from the pinned protocol, so clients receive typed `structuredContent` with the documented response fields. Fields are optional and nullable because OBS omits newer fields in older versions and returns undocumented nulls.

Tool discovery works without an OBS connection. Calls that need OBS return an MCP error until the WebSocket connection is ready.

### Choosing tools

Every available tool in the selected profile is registered by default. Clients load these definitions into the model's context, so a smaller set is cheaper and easier to choose from. Filters cannot enable a tool excluded by its profile or missing adapter.

| Group | Covers |
|---|---|
| `general` | status, version, statistics, hotkeys, vendor requests, custom events, sleep, control of a shared server |
| `scenes` | scenes, canvases, program and preview scene, `obs-apply-scene`, `obs-audit-scene` |
| `scene-items` | scene items, groups, transforms, ordering, locking, blend modes, `obs-snapshot`, and `obs-restore` |
| `sources` | source screenshots and active state |
| `inputs` | inputs, input settings and properties, audio, deinterlacing, and `obs-capture-window` |
| `media` | media input playback |
| `filters` | source filters |
| `transitions` | transitions, overrides, the T-Bar |
| `record` | recording, chapters, `obs-preflight`, `obs-record-clip`, the `obs-take-*` tools, `obs-trim-take`, and `obs-contact-sheet` |
| `stream` | streaming and captions |
| `outputs` | virtual camera, replay buffer, and generic outputs |
| `config` | profiles, scene collections, video settings, persistent data |
| `ui` | studio mode, dialogs, projectors |
| `protocol` | `obs-describe-request`, the generic `obs-call-request`, and `obs-batch` |

`core` expands to `general`, `scenes`, `scene-items`, `sources`, `inputs`, and `record`. Add `OBS_MCP_READ_ONLY=true` for inspection-only discovery. Counts depend on profile and adapters. Unknown groups or names refuse at startup.

With `OBS_MCP_DYNAMIC_TOOLSETS=true`, every group is registered but only the groups in `OBS_MCP_TOOLSETS` (by default just `general`) and the tools in `OBS_MCP_TOOLS` start enabled. Three more tools manage the rest: `obs-list-toolsets` shows each group and how many of its tools are on, and `obs-enable-toolset` and `obs-disable-toolset` switch groups for the session. Each change sends `notifications/tools/list_changed`, so clients that support it (Claude Code, VS Code, Cursor) reload the list. Read-only mode still applies: enabling a group adds only its read-only tools.

### Resources and prompts

Resources give clients that support them a live view without tool calls. Each one duplicates a read-only tool, since many clients read resources rarely or not at all.

| URI | Contents | Updated on |
|---|---|---|
| `obs://status` | connection, recording, streaming, program scene | output and scene changes |
| `obs://scenes` | `GetSceneList` | scenes added, removed, renamed, or switched |
| `obs://scene/{sceneName}/items` | `GetSceneItemList` | items added, removed, reordered, shown, hidden, locked |
| `obs://scene/{sceneName}/screenshot` | 960px JPEG of the scene | (read on demand) |
| `obs://take/current` | the running take: audio levels, skipped frames, black or unchanging picture, chapters, warnings | a warning or chapter |

Legacy stdio clients receive updates for subscribed URIs; modern clients choose updates on their `subscriptions/listen` stream. Legacy HTTP requests do not retain initialize offers or subscription state as authority for later exchanges.

Three prompts appear as slash commands in clients that show MCP prompts: `record-demo` (capture a window silently, preflight, record a clip), `software-demo` (the bounded demo recipe below), and `pre-stream-check` (a read-only go/no-go review).

### Confirming live actions

With `OBS_MCP_CONFIRM_LIVE=true`, `obs-start-stream`, generic `StartStream`/`StartOutput` requests, `obs-stop-stream`, `obs-toggle-stream`, `obs-stop-record`, `obs-toggle-record`, `obs-take-stop`, `obs-stop-output`, and `obs-toggle-output` ask before they run, as do `obs-call-request` and `obs-batch` when they carry one of those requests. The generic output tools are included because the stream and the recording are outputs too. Clients that support elicitation (Claude Code, VS Code, Cursor) show the user a confirmation, and `confirm: true` from the model does not skip it. Other clients get an error asking the model to get the user's agreement and call again with `confirm: true`. These tools are also marked destructive, so clients that ask before destructive tools already prompt without this setting.

Confirmation never enables a profile-excluded tool or bypasses a durable barrier. Native grants are single-use and bound to the caller and original request; re-enabling a hidden tool cannot revive a consumed grant.

## Where it fits

OBS MCP is well suited to operational work that already has a clear outcome: inspecting a scene collection, creating or arranging sources, changing audio state, switching scenes, taking screenshots, checking output status, or carrying out a repeatable recording setup.

It does not replace visual or editorial judgment. Framing, color, transition timing, audio balance, and the decision to go live still need a person watching and listening to the result.

## Safety and approvals

Every tool publishes all four MCP behavior hints. Read-only inspection is separated from state changes, and actions with broader consequences are marked destructive or open-world as appropriate.

Get explicit user permission before capture, recording, streaming, file publication or destructive changes. Keep qualification bounded to an approved collection and fixture. Tool hints and a successful preflight are not permission. Pay particular attention to tools that:

- start or stop streaming, recording, virtual camera, replay buffer, or another output
- switch profiles or scene collections
- trigger hotkeys or interact with OBS UI elements
- invoke vendor requests
- save files or use Advanced `obs-call-request`

In Advanced, `obs-call-request` and `obs-batch` accept only request types in the bundled OBS protocol, but their payloads are intentionally generic. With `OBS_MCP_CONFIRM_LIVE=true`, they ask before sending `StartStream`, `StartOutput`, `StopStream`, `ToggleStream`, `StopRecord`, `ToggleRecord`, `StopOutput`, or `ToggleOutput`. Both are always advertised as destructive and open-world so a client does not silently treat an unfamiliar operation as safe.

### Watching a take

`obs-record-clip` records up to 50 seconds in one call. For anything longer, or a demo whose steps happen while it records, use a take:

1. `obs-take-start` runs the preflight, starts recording, and waits for OBS to confirm. Only one take runs at a time.
2. `obs-take-mark` adds a named chapter before each step, both in the file (Hybrid MP4/MOV) and in the take log.
3. `obs-take-status` reports what the take has seen so far.
4. `obs-take-stop` stops recording, checks the file, and reports.

While a locked take records, profile, collection and output-setting changes refuse. Use the owned Take Stop for that recording. Explicit Stop admission is separately bounded; unavailable storage or uncertain ownership can still refuse it. `lock: false` changes the take lock, not immutable profile rules or durable barriers. Advanced raw/batch tools cannot be used to bypass those rules in a guarded Production process.

While a take records, the server watches OBS instead of waiting for the file:

- **Audio**: it subscribes to OBS's `InputVolumeMeters` events for the length of the take and records each input's loudest level after volume and mute. An input on a recorded track that rises above −60 dBFS is logged, and with `expectSilent` it is a problem. Muted inputs and inputs on unrecorded tracks are not.
- **Frames**: it polls `GetStats` every 2 seconds and warns when OBS misses frames rendering or the encoder skips them.
- **Picture**: once a second it takes a 96-pixel-wide PPM screenshot of the program scene. Two black samples in a row raise a warning (a capture that lost its window or permission looks like this). Stretches of 3 seconds or more where the picture holds still are recorded as stills, which mark dead time to trim. A spinner or a shimmering "thinking" label counts as still; text that appears and stays does not (see below).

The result lists the findings, and when OBS records to this machine the server writes them next to the recording as `<name>.take.json`, with chapter times and OBS events (scene switches, mutes, output state) in seconds from the start. `obs://take/current` shows the same data while the take runs.

### Bounded software demos

The `software-demo` MCP prompt and [obs-demo agent skill](.agents/skills/obs-demo/SKILL.md) use the [version 1 recipe](docs/recipes/software-demo.v1.json). The skill remains in the development checkout and native MCPB source bundle. Open this checkout in your agent host and explicitly load the linked skill, or request the `software-demo` prompt through MCP. Merely installing an MCPB does not install a skill into every host. Automatic host discovery and real rendered OBS output have not been qualified by the hermetic checks.

V1 uses local OBS on macOS or Linux, an existing current scene, and one enabled, preconfigured window capture. Linux supports XComposite on X11 and an existing portal-authorized PipeWire capture on Wayland. A portal selection still needs visual inspection for window identity. Video-only Linux capture sources have no mute or audio tracks to change; preflight checks the complete recording audio path. The capture must appear exactly once in a collection without groups or nested scenes. The tools require stable OBS object identifiers and complete state; they refuse unsupported setups rather than create or retarget sources. Streaming, recording, virtual camera, replay buffer, and every listed generic output must be inactive during preparation and recovery. Missing or failed output status blocks the operation. OBS code 604 explicitly reporting an unavailable virtual camera or replay buffer is accepted as an unavailable feature; named output statuses must still establish inactivity.

1. Plan with `obs-prepare-demo` and exact `sceneCollection`, `sceneName`, and `inputName`. After authorization, repeat with `apply: true`. It mutes the capture, clears its audio tracks, fits it to the canvas, and returns an `operationId` when changes require recovery.
2. Inspect a screenshot for the intended window and framing. Run silent preflight and require every check named in the recipe to pass. A required unknown or missing check blocks this workflow.
3. Start a take with `maxDurationSeconds` (120 by default in the prompt, at most 3600), mark steps, and stop it when finished. The server attempts to stop its own take at the deadline and retains the result for `obs-take-stop` to read. The deadline requires a running server; process death and sleep prevent an unattended cleanup guarantee.
4. Inspect the final file and contact sheet. Chapters may be in the `.take.json` sidecar. After trimming, verify the exported copy and its remapped chapters. Use the local verifier below to bind measurements to the delivered file hash.
5. Plan `obs-recover-demo` with the retained `operationId`; apply recovery when requested. It restores only the preparation's changed fields after checking collection/object identity and expected values. A conflict stops recovery. It does not lock OBS's UI or other processes; incomplete recovery needs operator inspection. Existing `obs-restore` remains a manual tool for legacy snapshots.

```sh
node scripts/verify-demo.mjs --file /path/demo.mp4 --reference /path/expected.png --seconds 30 --chapter Intro --checkpoint 10=/path/step-two.png --report /path/demo.verification.json
```

The verifier uses local `ffmpeg` and `ffprobe`; it never contacts OBS. It reports strict full-file decoding separately from metadata and sampled pictures. A decode timeout is unavailable evidence. It checks duration within 2 seconds, a video stream, every audio track at or below -60 dB, requested chapters, and reference frames scaled to 160×90 grayscale (mean absolute error at most 12). These frame comparisons test selected checkpoints, not semantic recognition or every frame. Use reference images from the intended scene. Missing tools or evidence produce `unavailable`; failed requirements produce `fail`. The report must be a new file. Synthetic-media tests cover the verifier; a real recording still needs visual review.

### Trimming a take

`obs-trim-take` finds dead time: stretches where the picture holds still (and, if the recording has sound, where it is also silent). It samples the video twice a second at 96 pixels wide and looks for steps: pixels whose median over the three samples before a moment differs from their median over the three after it. Text that appears and stays is a step. A spinner, a shimmering "thinking" label, or a blinking cursor comes and goes, so waiting on one still counts as dead time. On a 1080p ChatGPT recording, this separated waiting from streaming answers where ffmpeg's `freezedetect` could not with any single noise threshold.

By default, a still stretch of 4 seconds or more loses its middle, keeping 1 second at each side. Nothing is cut from 3 seconds before to 1.5 seconds after each chapter start, or in the last 3 seconds, so each step's finished result stays on screen long enough to read. Chapters come from the file, or from the take log when the file has none, and are moved to their new times.

Without `apply: true` it returns only the plan: the cuts, the new length, and the new chapter times. With it, the tool validates a staged copy before publishing `<name>.trimmed.mp4` next to the original. Validation checks the planned duration, video dimensions, audio stream count, remapped chapters, and strict full-file decoding. Publication refuses an existing destination. Cancellation before publication discards the staged copy; after publication, optional contact-sheet failure retains the output path and SHA-256 in the result.

Exports retain the first audio track and use H.264 with `yuv420p`. Other audio tracks are omitted; choose a separate workflow when they must remain independent. High bit depth and explicitly tagged PQ/HLG HDR inputs are refused; no implicit tone mapping occurs. Missing color tags remain unknown, so a successful export does not establish color fidelity. The contact sheet shows frames on either side of each seam for review.

`obs-contact-sheet` tiles frames from a recording into one JPEG: one second after each chapter start by default, or at the times you give. Both tools work only on files inside OBS's recording directory, and need `ffmpeg` and `ffprobe` on `PATH`.

### Auditing a scene

`obs-audit-scene` reads one scene in the current collection without changing OBS. The selected scene can be off program. It is available in the `scenes` group and in read-only mode:

```json
{
  "sceneCollection": "Demo collection",
  "sceneName": "Demo",
  "requiredItems": [{ "sourceName": "Application" }],
  "includeScreenshot": false
}
```

For a secondary canvas, add `canvasUuid` from `obs-get-canvas-list`. The audit uses that canvas for scene discovery, item and group reads, transform rereads, and the optional screenshot. Omitting it keeps OBS's default-canvas behavior. The report includes the supplied canvas UUID in its target.

The report lists individual source occurrences with their container, source UUID, scene-item ID, and containment path. A required source that is missing or hidden fails its check. Repeated occurrences need a more specific selector: add `sourceUuid`, `containerUuid`, or `sceneItemId` to the required item. Ambiguous selectors remain unknown.

Checks report `pass`, `fail`, or `unknown`, with observation times and evidence. Missing fields, unreadable state, cycles, traversal limits, unsupported geometry, or detected changes during collection make the affected evidence unknown. Nested scene and group membership can be inspected, but composed geometry is not qualified. The report covers a sampling interval; it is not an atomic snapshot or a promise that OBS will stay unchanged.

Zero-size media sources remain unknown: a filename extension does not prove that a file has no video. Inputs whose source kind establishes audio-only capture are exempt from visual dimension checks.

Set `includeScreenshot: true` to request a bounded scene image for inspection. An unavailable image is reported as unknown. The tool does not save screenshots or reports to disk. A structural pass leaves `renderedContent` as `unverified`: it does not establish the intended capture content, readability, freedom from occlusion, audio policy, or readiness to record. Continue to use the recording preflight and inspect the rendered picture separately.

### Describing a scene

`obs-apply-scene` takes the scene you want instead of the steps to build it:

```json
{
  "sceneName": "Review",
  "sources": [
    { "name": "Background", "kind": "color_source_v3", "settings": { "color": 4278190080 }, "locked": true },
    { "name": "ChatGPT Window", "kind": "screen_capture", "settings": { "type": 1 }, "muted": true, "audioTracks": [], "fit": "canvas" }
  ],
  "order": true,
  "apply": true
}
```

Sources are listed back to front. The tool reuses existing inputs (including ones shown in other scenes), creates missing scenes, inputs, and items, and changes only the settings, visibility, lock, mute, volume, audio tracks, transforms, and order that differ. `fit: "canvas"` scales a source to fit the canvas, centered. `removeOthers: true` removes unlisted items from the scene; their inputs remain.

Without `apply: true` it returns the plan. When applying, it creates what is missing in one request batch, then plans again with the new items' IDs and makes the remaining changes in a second. It then plans a third time to confirm OBS matches, and warns about any listed source that renders at 0×0. For an existing scene it first saves a snapshot, so `obs-restore` can undo the changes to existing sources.

### Undoing changes

`obs-snapshot` saves the settings, mute, volume, and audio tracks of inputs, and the transform, visibility, lock, and order of scene items: by default the program scene and the inputs in it. `obs-restore` compares OBS with a snapshot and sends only what differs, in one request batch; `dryRun: true` lists the changes first. It cannot recreate a removed input or item, and leaves items added since in place; it reports both. `obs-capture-window` takes a snapshot automatically before it changes an existing capture.

Snapshots are saved in the server's state directory (the last 20 you take, plus the last 10 taken automatically before a change, readable only by you), so `obs-restore` works after a restart and from another agent's session. Each server lists only the snapshots of the OBS instance it connects to. They contain input settings, such as file paths and browser source URLs. OBS's own scene collection file is not a substitute: OBS rewrites it while running.

### Sharing one server

By default, each stdio client starts its own server and OBS owner. To share one retained service, start the native executable in HTTP mode:

```sh
OBS_MCP_STATE_DIR=/absolute/canonical/approved-state \
OBS_MCP_HTTP_PORT=8765 \
native/target/release/obs-mcp-native
# Point each MCP client at http://127.0.0.1:8765/mcp?client=its-name
```

The server binds only to `127.0.0.1` and checks Host and Origin headers. Set an opaque `OBS_MCP_HTTP_TOKEN` through approved secret configuration to require bearer authentication; without it, local programs can reach the server. HTTP uses static tool groups and ignores `OBS_MCP_DYNAMIC_TOOLSETS`.

The port accepts trimmed decimal digits from 0 through 65535, including leading zeros. `0` chooses a free port. An absent, empty or JavaScript-whitespace-only value selects stdio; signed, fractional, exponent and hexadecimal forms refuse. HTTP ignores stdin EOF and joins its service and drivers on SIGINT or SIGTERM. Closing one client does not shut down peer work or a Take.

While agents share a server, `obs-claim-control` gives one of them control of OBS for a set time (30 minutes by default). Until it calls `obs-release-control` or the time runs out, tools that change OBS refuse calls from other clients. Read-only inspection and explicitly admitted Stop routes retain their own policy and capacity limits. `obs-control-status` shows who holds control. `obs-take-start` claims control for the length of the take. Clients are told apart by name: the `client` parameter of the URL they connect with, or else the name a 2026-07-28 client sends with each request. Clients using the 2025 protocol over HTTP send each request without a name, so give each agent its own `?client=` name. An unnamed client cannot claim control and is refused while another client holds it. Two agents given the same name share control. The names are not credentials; control is for agents cooperating on one machine, not for keeping out someone who can reach the server.

## Troubleshooting

**Tools appear, but OBS calls fail:** make sure OBS is open and its WebSocket server is enabled. Discovery is local; the shared OBS connection starts lazily. A failed or lost owned session can require a fresh MCP process. Keep its receipts and do not replay an uncertain mutation.

**Authentication fails:** copy the password from **Tools > WebSocket Server Settings** into `OBS_WEBSOCKET_PASSWORD`, then restart the MCP server process. When the server runs on the same machine as OBS, `OBS_MCP_READ_OBS_CONFIG=true` reads the saved password instead, so it never has to be copied into an agent's configuration. The server logs which file it used, never the password.

**The agent cannot see the tools:** restart the agent session or the MCP host so it creates a fresh stdio connection and reloads the tool list.

**A request is unsupported:** the connected OBS build did not advertise that WebSocket request. Use an operation supported by that version or update OBS.

**The process exits or emits protocol errors:** inspect stderr. stdio stdout is reserved for MCP traffic; HTTP stdout stays empty. Keep ordinary logs on stderr.

## Development

After approving the project-scoped dependency fetch, install the pinned development tooling and run the native gate:

```sh
npm ci --ignore-scripts --no-audit
npm run native:check
```

The gate uses the checked Rust toolchain/assets and explicit TypeScript/SDK oracles. It runs default and all-feature Rust checks, path-crate and resolver lifetime controls, then both MCP eras, actual HTTP entry and TCP controls against a pinned optimized executable and fake OBS. License and migration checks also run. This command does not record live OBS. `npm run check` remains the development Source-oracle gate; Node tooling is not part of the native runtime.

Test the file users will actually install with:

```bash
npm run test:package
```

That command builds the host-specific `dist/obs-studio-native.mcpb`, extracts it, and checks its manifest, corresponding source and native entry. Verify profile-conditional inventories rather than treating the 178-name union as one runtime. The recorded macOS artifact passed all three package checks; later artifacts and other platforms need their own receipts.

### Live OBS tests

The development live suite is separate from CI and native release acceptance. It exercises the explicit Source oracle; the extracted native Mac journey recorded above has its own receipts. Changed executables, other platforms and additional live workflows need the relevant authorized checks. By default this development suite is skipped. With OBS open and the standard WebSocket environment variables already available to the process, run the read-only checks with:

```bash
OBS_MCP_LIVE_TEST=1 npm run test:obs-live
```

The read-only lane checks the OBS version, scenes, active scene collection, streaming state, recording state, resources, and read-only request batches through the compiled MCP stdio server. On the machine running OBS, add `OBS_MCP_READ_OBS_CONFIG=1` instead of exporting the password.

There is also a narrowly bounded mutation test. Use a disposable scene collection, select it in OBS first, and make sure streaming and recording are stopped. Then run:

```bash
OBS_MCP_LIVE_TEST=1 \
OBS_MCP_LIVE_MUTATION=1 \
OBS_MCP_LIVE_SCENE_COLLECTION="OBS MCP Test" \
npm run test:obs-live
```

The suite refuses to mutate if the selected collection does not exactly match `OBS_MCP_LIVE_SCENE_COLLECTION` or if either output is active. It creates one uniquely named scene and removes it in a `finally` block.

Add `OBS_MCP_LIVE_RECORD=1` to the mutation command to also record a one-second clip. It checks that `obs-start-record` reports success only after OBS confirms the output is active, then stops the recording in a `finally` block. The clip is saved to the profile's recording directory and captures whatever the selected collection shows, so select a collection whose sources are safe to record.

Pull-request CI checks the dependency policy before installation. Native-default delivery must run the hermetic development gate on a supported pinned Node toolchain and separately exercise the extracted executable with Node unavailable. Historical Node20 matrix results are not native-only distribution acceptance. Dependency auditing checks the lockfile in a separate weekly workflow so registry advisories do not make otherwise reproducible pull-request checks flaky.

### Dependency safety

Direct dependencies use exact versions. `npm run check:dependencies` checks that the lockfile matches the manifest, uses npm registry HTTPS tarballs with SHA-512 integrity, and contains no production dependency install scripts. The check runs offline and requires no installed packages:

```bash
node scripts/check-dependencies.mjs
```

The private development project disables dependency lifecycle scripts through `.npmrc`; native MCPB staging installs no npm dependencies. Explicit commands such as `npm run build` still run. Dependabot proposes weekly npm and GitHub Actions updates for review. CI actions are pinned to verified commit hashes. The residual development advisory and published SDK license findings are recorded in [`docs/dependency-security.md`](docs/dependency-security.md).

For a dependency update, inspect its release notes and changes to the lockfile, retain exact direct versions, then run `npm run check` and `npm run test:package`. Check current advisories with `npm audit --package-lock-only --ignore-scripts --audit-level=low`. An audit reports known registry advisories; the offline policy check validates the dependency graph's packaging constraints.

The private development toolchain is pinned in package.json/package-lock.json. The current Vitest5 range supports Node22.12+,24 or26+, independently of the installed native runtime, which requires no Node.

### TypeScript policy

The development oracle builds with TypeScript 7.0.2 and does not import the TypeScript compiler API. If a future generator, linter, or build tool needs programmatic compiler access, keep that tool on TypeScript 6 while the application compiler remains on TypeScript 7. TypeScript 7.0 does not ship a programmatic API; Microsoft documents the side-by-side TypeScript 6 compatibility package in the [TypeScript 7.0 release notes](https://devblogs.microsoft.com/typescript/announcing-typescript-7-0/#running-side-by-side-with-typescript-6-0).

## Historical protocol baseline

On October 14, 2026, this revision was compared with the `modelcontextprotocol`, `mcp-typescript-sdk`, and `obs-studio` checkouts under `/Volumes/External SSD/referencerepos`. Those checkouts were read without modification. The exact reference sources are:

- MCP specification: [`5ea99602`](https://github.com/modelcontextprotocol/modelcontextprotocol/commit/5ea996028df322ce97d1c906731cf8c05a6e268e)
- MCP TypeScript server and client SDKs: `2.3.1`; reference checkout [`9bc40e90`](https://github.com/modelcontextprotocol/typescript-sdk/commit/9bc40e903fde15534ec06ff4c1347d47000dbe16)
- OBS Studio: [`f082210c`](https://github.com/obsproject/obs-studio/commit/f082210c177a03572cace88c168d0f32d8cde39e)
- OBS WebSocket: [`e28f8497`](https://github.com/obsproject/obs-websocket/commit/e28f84975984340e84fc4879cfc05741cc0afde1), the revision pinned by that OBS Studio checkout

The refreshed OBS WebSocket revision has the same 147 requests, 60 events, and 7 enums as the previous baseline; its protocol JSON is byte-for-byte identical. MCP 2026-07-28 remains the released protocol baseline.

The generated protocol files live under [`docs/`](docs/). Tests compare every request used by the tool modules with the bundled protocol, so obsolete request names and missing wrappers fail locally.

## Project history

This project began with [Roy Shilkrot's `obs-mcp`](https://github.com/royshil/obs-mcp), including upstream work by [Zeke Sikelianos](https://github.com/zeke). It also preserves [Jag-k's](https://github.com/jag-k/obs-mcp) MCPB packaging and `registerTool` migration work. Their original commits and authorship remain in the Git history.

## License

OBS MCP is licensed under GPL-2.0-only. The native MCPB layout retains corresponding Rust source, native assets, pinned build inputs and notices. TypeScript remains development oracle code. See [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).

## Native implementation notes

[Native architecture](docs/native-architecture.md) describes retained service, journal, connection and shutdown ownership. [Full Rust cutover](docs/full-rust-cutover.md) records the optimized, package and bounded Mac evidence alongside the remaining release gates. Earlier 83/166/167 checkpoint configuration, timings and Mac recordings are artifact-bound historical evidence; their compatibility/opt-in commands are superseded by the current native setup.

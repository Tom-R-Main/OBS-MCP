# Production assistant workflows

Production observation, layout planning, staging, capture planning, durable operation receipts, and validated export publication share one MCP interface across macOS and Linux. Native capture still depends on the OBS installation and desktop session. Cue sheets and an operator panel remain later roadmap units.

## Observe

Call `obs-observe` with `{}` or read `obs://status`. Both return the same versioned production contract: connection generation, collection, canvas dimensions, exact program/preview identities, Studio Mode, standard and named outputs, control holder, active take identity, recovery metadata, observation interval, missing evidence, and next checks.

Reads are bounded to 256 requests, two seconds per request, and twelve seconds per observation after connecting. Relevant events, disconnects, reconnects, or disagreements between repeated reads invalidate the sample. Output counters and take elapsed time are deliberately excluded from consistency comparisons because they advance during normal production. A result of `incomplete` contains explicit missing evidence; `unknown` invalidates the production conclusions. Neither establishes readiness or rendered content correctness.

The existing status resource fields `connection`, `record`, `stream`, and `currentProgramSceneName` remain available on a successful observation. Resource consumers must handle `status:unknown` and missing fields when evidence is invalidated.

## Resolve a portable layout

`obs-plan-scene-template` is read-only. Templates name logical roles, accepted local source kinds, canvas dimensions, literal transforms, visibility, locks, and ordering. Bindings separately identify exact source names, UUIDs, container UUIDs and scene item IDs. No fuzzy matching authorizes a change.

```json
{
  "sceneCollection": "Interview",
  "sceneName": "Two people",
  "template": {
    "version": 1,
    "name": "Interview split",
    "canvas": { "width": 1920, "height": 1080 },
    "roles": [{
      "role": "presenterCamera",
      "inputKinds": ["av_capture_input", "v4l2_input"],
      "transform": { "positionX": 0, "positionY": 0 },
      "enabled": true, "locked": true, "index": 0
    }]
  },
  "bindings": [{
    "role": "presenterCamera", "sourceName": "Local camera",
    "sourceUuid": "<local-source-uuid>",
    "containerUuid": "<local-scene-uuid>", "sceneItemId": 1
  }]
}
```

Obtain local identities with the inventory tools. The result distinguishes renamed sources from replacements, incompatible kinds, missing/duplicate bindings, duplicate occurrences, nested layouts, and canvas mismatches. Proposed changes never trigger OBS mutation requests. This release supports flat layouts and the default canvas. Explicit `canvasUuid` selections are blocked because the pinned video-settings and transition contracts cannot establish scoped behavior. `obs-audit-scene` remains available for named-canvas structural inspection.

## Stage, inspect, take

1. Enable Studio Mode deliberately, then call `obs-stage-scene` with the exact `sceneCollection` and `sceneName`.
2. Inspect the returned preview screenshot. The receipt includes exact preview/program identities, shared source UUIDs, and a `stageId` valid for ten minutes in this server process. Staging does not edit layout, source settings, filters, or audio. An input shared with program remains shared.
3. Call `obs-take-scene` with that `stageId`. It consumes the receipt before asynchronous work, rechecks the connection, Studio Mode, preview/program, membership, transforms, settings, filters and available audio fields, and sends one transition request.
4. Read the receipt: `requestAccepted` is distinct from `intendedSceneObservedOnProgram`. `renderedComposition` remains `unverified` until a person or agent inspects it. Program verification waits up to five seconds; longer transitions can return an unknown outcome.

A human changing preview or a staged dependency blocks the take. A disconnect or uncertain transition response returns `unknown`, consumes the receipt, and directs the caller to observe program before doing anything else. No automatic transition replay occurs. A new stage supersedes the previous receipt. Restarting the server drops actionable stage tokens; durable operation metadata remains available through `obs-observe`. Cancellation stops undispatched work; an in-flight effect needs observation.

Dependencies are bounded to 128 occurrences and eight nested levels. Conservative admission may reject repeated nested scenes. Structural fingerprints cannot freeze live camera pixels, browser content, or the operator's UI. Rechecks reduce races without making OBS operations atomic. Existing control leases, read-only tool filters, and optional live confirmation apply to these tools.

## Plan a capture before changing it

Call `obs-capture-window` with `apply:false`, an exact scene/input name, and a window title or ID. Discovery uses an existing compatible capture source and returns the selected identity and inventory source. Missing or ambiguous matches cause no creation, retargeting, audio change, transform change, or snapshot. If no safe inventory source exists, configure a capture in OBS first. An unconfigured XComposite source is never seeded merely to enumerate windows.

For macOS, only the integer window list is queried. Native ScreenCaptureKit display/application string lists can contain null placeholders that crash the native serializer; selecting a display does not remove that risk. Application auto-discovery is refused. `displayUuid` remains accepted for compatibility and is ignored for window capture.

After reviewing the plan, call with `apply:true` (the compatibility default). The tool resolves and rechecks identities before applying. Live pixels and window IDs can change after observation; inspect the returned image. For a preconfigured source that should keep its target, use `obs-prepare-demo`: its plan and field-owned recovery journal cover fitting and silencing without retargeting.

## Inspect operation outcomes

Stage transitions and recording takes return an `operationId`. Receipts persist intent before dispatch and distinguish request acceptance from an observed postcondition. `obs-observe.operationReceipts` exposes bounded metadata even if the OBS observation itself is unavailable. The store contains hashed endpoint/collection identities, operation identifiers, phases and timestamps; it excludes source settings, images, raw endpoint URLs and exception text.

A dispatch intent surviving restart remains unknown. A current scene that happens to match cannot prove which earlier request caused it. Consumed stage IDs cannot replay; inspect the current program before making a fresh intentional stage. Unknown outcomes are shown before completed receipts, with `total` and `truncated` fields when the inventory exceeds one response. Unreadable history reports unknown rather than an empty list.

Recording stop confirmation waits for both the stopped event and inactive status within a bounded observation period. It sends one stop request in that attempt. An uncertain transport reply prevents another stop dispatch; a later terminal event and inactive status can reconcile the outcome. File checks, rendered-content review, and cleanup remain separate evidence; an observed stop receipt does not certify the media.

## Linux and macOS support

| Workflow | macOS | Linux X11 | Linux Wayland |
| --- | --- | --- | --- |
| Observe, audit, templates, stage/take | Same contract | Same contract | Same contract |
| Window selection by title | ScreenCaptureKit | XComposite `capture_window` | Desktop portal picker owned by the user |
| Application selection | ScreenCaptureKit application capture | Resolves one matching window | User chooses the target in the portal |
| Guarded demo fit/recovery | Existing window capture | Existing XComposite capture | Existing PipeWire grant and nonzero dimensions |
| Take monitoring, chapters, recording, inspection, trim | Shared implementation | Shared implementation | Shared implementation |
| Settings/state discovery | Application Support | XDG directories, Flatpak config fallback | XDG directories, Flatpak config fallback |

Use `obs-capture-window` with `apply:false` and a `window` title on macOS or Linux X11 to review the binding first. Backend choice uses OBS's advertised kinds, rather than the MCP server's desktop environment; OBS can run on another machine. On Wayland, configure and authorize the PipeWire capture in OBS first, then call with `backend:"portal"` and omit `window`/`application`. The tool fits and inspects that existing selection. Newer OBS uses `pipewire-screen-capture-source` for both monitors and windows: a restore token does not establish which one was selected, so inspect the image. The restricted workflow never retargets it.

X11 needs a working XComposite session and window manager. PipeWire needs a running desktop portal and user consent. Mac application-wide multiwindow capture has no equivalent in this X11 resolver. Linux video-only captures do not receive synthetic mute/track writes; other audio sources still require preflight. Generic device, GPU, virtual camera, and stream capabilities remain determined by the actual OBS installation.

The client accepts WebSocket v5 Hello packets without `obsStudioVersion` (as used by older OBS) and obtains capabilities from `GetVersion`. Connection generations increment only for new sessions, not subscription reidentification.

CI runs checks and extracted-package validation on Linux and macOS. Protocol fixtures cover the three capture families and production races. Live tests require an explicitly selected disposable collection and mutation opt-in. Headless Linux tests do not establish hardware capture, a real Wayland portal, or macOS rendering behavior.

## Validation on 2026-10-31

In this checkout, Debian 13 amd64 with Node 24.19 and official OBS 32.0.4 was used for real execution. `npm run check` covers build, test typechecking, unit tests, transport tests, dependency policy, and manifest validation. `npm run test:package` validates the extracted MCPB executable, including production observation and a guarded stage/take. The live suite exercises recording, resources, a portable layout plan, and verified scene transition against the disposable `OBS MCP Test` collection.

A rootless Openbox/Xvfb fixture also exercised real X11 window enumeration and target selection. This exposed an OBS 32.0.4 crash when an unconfigured XComposite source's null placeholder was serialized by obs-websocket. That investigation used an explicit no-window value `"0"` to avoid the placeholder. The successor now refuses unconfigured discovery instead of seeding a source; it resolves through an existing safe inventory before creation. Direct enumeration remains guarded, and dangerous enumeration inside batches is refused before dispatch. After the fix, OBS selected the intended fixture window without crashing.

XComposite then reported `Cannot create EGLImage` on the software-only virtual display, leaving video dimensions at 0×0. Guarded demo preparation correctly refused that capture, and recording was not attempted. This establishes target selection and safe refusal, not rendered X11 capture. Real Wayland portal behavior, physical devices/GPU, virtual camera, external streaming, and macOS execution were not validated in that Linux environment. The expanded macOS CI matrix has been configured; no remote CI result is claimed.

Before claiming complete desktop parity, run the software-demo journey on real macOS, Linux X11 and Linux Wayland sessions: inspect the intended capture, prepare, pass preflight, record, inspect the final media, and recover. Application-wide ScreenCaptureKit behavior remains an OS-specific capability; Linux window resolution never silently substitutes a full-display capture.

The subsequent macOS 26.5.1 qualification used OBS 32.2.2 and a dedicated fixture window in a disposable profile/collection. After fixing native enumeration and a stopped-event/status race, a 3.605-second silent take passed state-A/state-B reference checks, chapters, strict full decoding, stop confirmation, and guarded recovery. A conflicting operator transform was preserved before recovery was allowed. Earlier failed attempts remain recorded separately. This establishes that bounded Mac journey; real rendered X11 and Wayland qualification remain open.

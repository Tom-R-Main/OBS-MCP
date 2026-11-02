---
name: obs-demo
description: Prepare and record a bounded silent software demo using OBS-MCP's existing macOS or Linux window capture, then inspect the delivered recording.
---

# Record a software demo

Use the [version 1 recipe](../../../docs/recipes/software-demo.v1.json). Require a local OBS instance on macOS or Linux, a dedicated collection, and an already configured window capture in the current scene. Ask for the exact collection, scene, input, expected visible content, steps, and maximum duration when these are missing. This skill grants no permission to record, change OBS, or publish.

Discover the recipe's tools through MCP before acting. If tools are filtered, enable the record, scenes, and sources groups through the available discovery tools. Missing tools block this recipe. A read-only server cannot prepare or record.

1. Call `obs-prepare-demo` with the exact `sceneCollection`, `sceneName`, and `inputName`; omit `apply` for the plan. It must refuse missing/shared captures and active outputs. Review the proposed mute, track, and fit changes. Obtain authorization for the target and recording before applying. Do not substitute scene creation or capture retargeting when admission fails.
2. Apply the same preparation with `apply: true`. Retain its `operationId`. If it reports a partial result, stop and inspect recovery; do not retry the mutation to obtain a cleaner response.
3. Call `obs-get-source-screenshot` for the program scene. Inspect the image for the expected window/content and framing. A nonblack image alone does not identify the window. Save a reference image for a repeatable recording when a file-capable driver is available. Stop if the picture cannot be inspected.
4. Call `obs-preflight` with `expectSilent: true`. Every check listed as required in the recipe must pass. Do not start on `unknown`, missing required results, or required verification tooling being unavailable. Informational notes outside the required list are not failures.
5. Call `obs-take-start` with `expectSilent: true`, `lock: true`, `requireVerifiedPreflight: true`, and an explicit `maxDurationSeconds` (default 120; at most 3600). Mark each step with `obs-take-mark`, perform only authorized app interactions, and check `obs-take-status`. Stop on a warning that invalidates expected content or silence. Always attempt `obs-take-stop` for your own active take when a step fails. After an uncertain stop, read `obs-get-record-status` before another action; never blindly repeat a start or stop.
6. Check the reported verification status. `unavailable` is not verified. Chapters in V1 may be in the `.take.json` sidecar; do not promise embedded chapters. If trimming was requested, create a copy with `obs-trim-take` and inspect that final file, including remapped chapters. Preserve the original.
7. Generate a contact sheet with `obs-contact-sheet` and inspect it. With local file tooling, run `node scripts/verify-demo.mjs --file <final-video> --reference <expected-frame.png> --seconds <expected-duration> --chapter <expected-name>`; add checkpoints with `--checkpoint <seconds>=<reference.png>` to check state changes. Deliver the file, sidecar, contact sheet, and verification report with its file hash. Missing visual, audio, or file evidence must be reported as unverified.
8. Offer recovery through `obs-recover-demo` using the retained `operationId`: inspect its default plan before applying. A conflict requires operator review; do not fall back to unconditional `obs-restore`. Recovery only covers this preparation's changed fields. The general legacy restore tool remains a manual operation.

The deadline runs while the server is alive. Process death, machine sleep, and competing OBS controllers can prevent timely cleanup; this recipe does not promise unattended crash recovery. Collection and value checks detect conflicts but do not lock OBS's UI. Go-live actions are outside this recipe.

Linux captures use XComposite on X11 or an existing user-authorized PipeWire window capture on Wayland. Portal selection and permission belong to the desktop session. A persisted portal selection and nonzero dimensions do not prove the intended window is visible; inspect the picture. Video-only capture inputs have no audio fields to change; silence still requires preflight of the full output routing.

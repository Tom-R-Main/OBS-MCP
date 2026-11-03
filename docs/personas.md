# User personas

Six people who use OBS through an agent, one for each main use case. Each
has goals stated as outcomes they can check, and a live test that pursues
those goals through the native service in its default Production profile. The
test judges what OBS actually produced (a recording, a replay, what a streaming
server received, a screenshot) rather than what OBS reported.

The tests are in [`test/live/personas.test.ts`](../test/live/personas.test.ts);
see "Persona tests" in the README to run them. Media clips stand in for
cameras, games and microphones, each with its own color and tone so a frame or
an audio track shows which source it came from.

| Persona | Use case | Configuration |
| --- | --- | --- |
| Maya | Social streaming with overlays | No recording root (Production offers stream tools only without one) |
| Dev | Recording and vlogging for an editor | Recording root, FFmpeg |
| Priya | Virtual camera for meetings | No recording root |
| Kai | Gaming capture and replays | Recording root, FFmpeg, replay buffer enabled in the profile |
| Dr. Lin | Education and webinar layout switching | Recording root, FFmpeg |
| Sam | Product demo recording | Recording root, FFmpeg, a desktop window to capture |

## Maya, a streamer with overlays

Maya streams games to Twitch.

**Goals:**

1. Scenes for "Starting soon", "Live" (gameplay, webcam, a hidden follower alert) and "BRB".
2. Go live to her ingest.
3. Show the alert for a few seconds mid-stream.
4. Step away and come back.
5. End the stream cleanly.

**Journey:**

1. `obs-apply-scene` builds each scene.
2. `obs-set-stream-service-settings` points the stream at her ingest.
3. `obs-start-stream`, then `obs-set-current-scene` and `obs-set-scene-item-enabled` during the stream.
4. `obs-stop-stream` ends it.

**Checks:**

- A local RTMP server stands in for Twitch. Its received stream shows each scene at the right moment.
- The alert appears only while it was shown.
- The stream reports no skipped frames.

## Dev, a vlogger editing in Resolve

Dev records YouTube tutorials and edits them in DaVinci Resolve.

**Goals:**

1. His screen with his camera in the corner.
2. His voice and the desktop audio on separate tracks.
3. A chapter per segment.
4. A quick look at the take.

**Journey:**

1. `obs-apply-scene` sets the layout and the audio tracks.
2. `obs-preflight`.
3. `obs-take-start`, `obs-take-mark` and `obs-take-stop`.
4. `obs-contact-sheet`.

**Checks:**

- Track 1 carries both sources, track 2 only his voice, and track 3 only the desktop. Crossover is below −50 dB.
- The chapters are in the file.
- The camera sits in the bottom-right corner of the recorded frame.
- `obs-trim-take` accepts the three-track recording.

## Priya, a consultant on video calls

Priya joins client calls with OBS as her camera.

**Goals:**

1. Her camera with a branded lower third.
2. Check it before joining.
3. Turn on the virtual camera.
4. Step away behind a "back soon" card, then return.
5. Turn the camera off after the call.

**Journey:**

1. `obs-apply-scene` builds both scenes.
2. `obs-audit-scene` with required items and a screenshot.
3. `obs-start-virtual-cam`, then scene switches.
4. `obs-stop-virtual-cam`.

**Checks:**

- The audit passes, and its screenshot shows the camera.
- The card scene renders.
- The virtual camera starts and stops.
- With `OBS_MCP_LIVE_VCAM_FRAMES=1`, frames read from the "OBS Virtual Camera" device show the camera, then the card, then the camera again. This needs camera permission for the process running the tests.

## Kai, a competitive gamer

Kai plays ranked matches.

**Goals:**

1. The game full screen with a facecam.
2. His voice and the game on separate tracks.
3. The replay buffer running.
4. Save the last moments after a great play without dropping frames.

**Journey:**

1. `obs-apply-scene`.
2. `obs-start-replay-buffer`.
3. `obs-save-replay-verified`.
4. `obs-get-stats` and `obs-stop-replay-buffer`.

**Checks:**

- The replay is verified, decodes fully and sits under the recording root.
- Voice and game are on their own tracks.
- The facecam is in the corner.
- No output frames were skipped.

## Dr. Lin, a lecturer switching layouts

Dr. Lin teaches online.

**Goals:**

1. Layouts for slides, slides with presenter, and presenter.
2. Preview each change before students see it.
3. A chapter per section of the lecture.
4. Cut the silent wait for questions before uploading.

**Journey:**

1. `obs-apply-scene` builds the layouts.
2. `obs-set-studio-mode`.
3. `obs-stage-scene` then `obs-take-scene` for each change.
4. `obs-take-start`, `obs-take-mark` and `obs-take-stop`.
5. `obs-trim-take` with `apply`, then `obs-contact-sheet`.

**Checks:**

- Every stage shows the intended scene in preview with a screenshot.
- Every take reports it on program.
- The recording shows each layout in its section and has all four chapters.
- The trimmed copy is shorter and keeps all four chapters and all three audio tracks.

## Sam, a founder recording a product demo

Sam records a walkthrough of her web app for a launch page.

**Goals:**

1. Only the app's window in the shot, with no stray audio.
2. A chapter per feature.
3. The loading and idle time cut out.
4. A contact sheet to check the edit.

**Journey:**

1. `obs-create-input` adds a window capture, which `obs-capture-window` needs to discover windows.
2. `obs-capture-window` targets the app by title.
3. `obs-prepare-demo`.
4. `obs-preflight` with `expectSilent`.
5. `obs-take-start`, `obs-take-mark` and `obs-take-stop`.
6. `obs-trim-take` with `apply`, then `obs-contact-sheet`.

**Checks:**

- The capture names the app window.
- The silent preflight passes.
- The take has no black stretches.
- The chapters are in the file.
- The trimmed copy is at least 5 seconds shorter, keeps all three chapters, and shows the app's final screen after the last chapter.

A small Tk app (`test/support/demo-app.py`) stands in for the product. The test runs only on macOS, because it creates a macOS window capture (`screen_capture`), and is skipped when Python's `tkinter` is unavailable.

## Gaps found while building these journeys

On OBS 32.2.2 with the native service from main at `da76658`:

- **Fixed: multitrack trim.** `obs-trim-take` refused every OBS multitrack recording with "requires exactly one default audio track". OBS marks every recorded track default. Trim now requires at least one default track, and the export keeps each track's flag.
- **Streaming and recording in one session.** In Production, the stream tools are offered only without a recording root, and recording needs one. A streamer who also wants a local recording cannot do both from one server in Production.
- **Fixed: stopping returned early.** `obs-stop-stream` and `obs-stop-replay-buffer` returned before OBS had finished stopping, so a settings change right after stopping a stream failed. The stop tools now return only once a status read sent after the stop reports the output inactive. If that doesn't happen within 10 s, as with a long stream delay, they report the stop as sent but unconfirmed.
- **A blank stream service can't be restored.** OBS rejects empty service settings, so an agent cannot put back a profile whose stream service was never configured.
- **Window discovery needs an existing capture.** On a fresh collection, `obs-capture-window` refuses until a window capture exists. On macOS the refusal now names the `obs-create-input` call that creates one (`screen_capture`, `type: 1`). On X11 it stays a manual step, because listing windows on an unconfigured XComposite capture can crash OBS. Only the macOS path has been run live.
- **Fixed: the silence preflight overcounted.** It counted media inputs from scenes that are not on program, which OBS does not mix. It now leaves out inputs OBS reports inactive, except global audio inputs, and lists what it left out.
- **The take monitor's "stills" ignore motion.** It uses a three-sample median, which erases small moving objects, so a constantly moving game box reads as unchanged. Trim only cuts stretches that are also silent, so footage with sound is kept. A silent recording of steady small motion would be treated as idle.
- **Changing the audio sync offset doesn't take effect** on a playing media source. See the "Scenario tests" section of the README.

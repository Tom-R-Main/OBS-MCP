# OBS MCP

[![CI](https://github.com/Tom-R-Main/OBS-MCP/actions/workflows/ci.yml/badge.svg)](https://github.com/Tom-R-Main/OBS-MCP/actions/workflows/ci.yml)

OBS MCP gives MCP clients a current, inspectable interface to OBS Studio. It exposes 158 tools for scenes, sources, audio, transitions, filters, recording, streaming, canvases, and the rest of the OBS WebSocket v5 request surface.

The server is built against MCP 2026-07-28 and still accepts legacy 2025-era clients. MCP discovery stays available when OBS is closed, and the server reconnects in the background when OBS returns.

## How it works

```text
MCP client  <-- stdio -->  OBS MCP  <-- WebSocket v5 -->  OBS Studio
```

OBS MCP runs as a separate Node.js process. It does not patch OBS or require an OBS plugin; it uses the WebSocket server built into OBS Studio. MCP handles tool discovery, schemas, results, and approval hints. OBS WebSocket handles the live application state.

## What is covered

- All 147 request types in the OBS WebSocket protocol revision pinned by the current OBS Studio source tree
- Explicit read-only, destructive, idempotent, and open-world annotations on every tool
- Runtime capability checks against the requests advertised by the connected OBS instance
- Structured MCP results for automation, with readable text retained for people and older clients
- Source screenshots returned as MCP image content instead of clipped base64 text
- Protocol inspection through `obs-describe-request`
- A generic `obs-call-request` escape hatch, limited to requests in the bundled OBS protocol and marked destructive for client approval
- A reproducible `.mcpb` build with its tool inventory generated through MCP rather than private SDK internals

The explicit tools remain the normal interface. The generic request tool is there so a newly added OBS operation can be used before it receives a more ergonomic wrapper.

## Requirements

- Node.js 20.19 or newer
- OBS Studio 28 or newer with the WebSocket server enabled
- An MCP client that supports MCP 2026-07-28 or the compatible legacy protocol

OBS includes WebSocket v5. Enable it under **Tools > WebSocket Server Settings**. Newer OBS requests are available only when the connected version advertises them; unsupported calls return a descriptive error.

## Install from source

This independent repository does not yet have its own npm release. Until it does, run it from a local checkout:

```bash
git clone https://github.com/Tom-R-Main/OBS-MCP.git
cd OBS-MCP
npm ci
npm run check
```

Then point your MCP client at the built entrypoint:

```json
{
  "mcpServers": {
    "obs": {
      "command": "node",
      "args": ["/absolute/path/to/OBS-MCP/build/index.js"],
      "env": {
        "OBS_WEBSOCKET_PASSWORD": "<password-from-obs>"
      }
    }
  }
}
```

If OBS uses a non-default address, add `OBS_WEBSOCKET_URL` to the environment block.

## Verify the connection

Open OBS, start a new agent session, and ask:

> Check the OBS connection. Report the OBS version, current scene, stream status, and recording status. Do not change anything.

The client should request only read-only tools and return live values from OBS. If tool discovery works but the prompt reports that OBS is disconnected, leave the MCP server running and start OBS; reconnection happens in the background.

## Install the MCPB package

Build the desktop package locally:

```bash
npm run pack
```

The command rebuilds the server, reads the public tool list through MCP, installs production-only dependencies in an isolated staging directory, validates the manifest, and writes:

```text
dist/obs-studio.mcpb
```

Open that file in an MCPB-compatible desktop client. The package prompts for the OBS WebSocket URL and password.

## Configuration

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `OBS_WEBSOCKET_URL` | No | `ws://localhost:4455` | Address of the OBS WebSocket server |
| `OBS_WEBSOCKET_PASSWORD` | Only when OBS authentication is enabled | None | Password configured in OBS |
| `OBS_MCP_MAX_SCREENSHOT_BYTES` | No | `4194304` | Maximum decoded size of a screenshot returned through MCP; the hard ceiling is 6 MiB |
| `OBS_MCP_TOOLSETS` | No | all tools | Comma-separated tool groups to register: `all`, `core`, or any group listed under [Choosing tools](#choosing-tools) |
| `OBS_MCP_TOOLS` | No | none | Comma-separated tool names to register in addition to `OBS_MCP_TOOLSETS` |
| `OBS_MCP_READ_ONLY` | No | `false` | Register only read-only tools; overrides both lists |
| `OBS_MCP_LOG_LEVEL` | No | `info` | Diagnostic output on stderr: `debug`, `info`, `error`, or `silent` |

The server never prints the password. Connection and protocol diagnostics are written to stderr so stdout remains reserved for MCP messages.

## Tool surface

The 158 tools are organized around the OBS protocol rather than a smaller opinionated workflow:

- server status, version information, statistics, hotkeys, and studio mode
- scenes, groups, sources, filters, and scene items
- input settings, audio tracks, volume, mute state, media controls, and deinterlacing
- recording, streaming, replay buffer, virtual camera, and output settings
- transitions, transition overrides, the transition cursor, and the T-Bar
- canvases, screenshots, profiles, scene collections, and persistent data
- a recording preflight (`obs-preflight`) that catches the conditions under which OBS silently refuses or botches a recording
- `obs-record-clip`, which preflights, records up to 50 seconds with chapter markers, stops, and checks the file's length and audio with ffprobe/ffmpeg when they are installed
- `obs-capture-window` (macOS), which points a `screen_capture` input at a window or app by name, silences it, and fits it to the canvas
- input and transform changes that return the resulting settings, size, and an optional screenshot, warning when a source renders at 0×0
- protocol description and the guarded generic request fallback

Tools that send a single OBS request declare an `outputSchema` generated from the pinned protocol, so clients receive typed `structuredContent` with the documented response fields. Fields are optional and nullable because OBS omits newer fields in older versions and returns undocumented nulls.

Tool discovery works without an OBS connection. Calls that need OBS return an MCP error until the WebSocket connection is ready.

### Choosing tools

Every tool is registered by default. Clients load each registered tool's definition into the model's context, so a smaller set is cheaper and easier for the model to choose from.

| Group | Covers |
|---|---|
| `general` | status, version, statistics, hotkeys, vendor requests, custom events, sleep |
| `scenes` | scenes, canvases, program and preview scene |
| `scene-items` | scene items, groups, transforms, ordering, locking, blend modes |
| `sources` | source screenshots and active state |
| `inputs` | inputs, input settings and properties, audio, deinterlacing, and `obs-capture-window` |
| `media` | media input playback |
| `filters` | source filters |
| `transitions` | transitions, overrides, the T-Bar |
| `record` | recording, chapters, `obs-preflight`, and `obs-record-clip` |
| `stream` | streaming and captions |
| `outputs` | virtual camera, replay buffer, and generic outputs |
| `config` | profiles, scene collections, video settings, persistent data |
| `ui` | studio mode, dialogs, projectors |
| `protocol` | `obs-describe-request` and the generic `obs-call-request` |

`core` expands to `general`, `scenes`, `scene-items`, `sources`, `inputs`, and `record` (81 tools). `OBS_MCP_TOOLSETS=core OBS_MCP_READ_ONLY=true` gives a 38-tool inspection-only server. The server refuses to start when a group or tool name is unknown.

## Where it fits

OBS MCP is well suited to operational work that already has a clear outcome: inspecting a scene collection, creating or arranging sources, changing audio state, switching scenes, taking screenshots, checking output status, or carrying out a repeatable recording setup.

It does not replace visual or editorial judgment. Framing, color, transition timing, audio balance, and the decision to go live still need a person watching and listening to the result.

## Safety and approvals

Every tool publishes all four MCP behavior hints. Read-only inspection is separated from state changes, and actions with broader consequences are marked destructive or open-world as appropriate.

Pay particular attention to approvals for tools that:

- start or stop streaming, recording, virtual camera, replay buffer, or another output
- switch profiles or scene collections
- trigger hotkeys or interact with OBS UI elements
- invoke vendor requests
- save files or use the generic `obs-call-request` fallback

`obs-call-request` accepts only request types in the bundled OBS protocol, but its payload is intentionally generic. It is always advertised as destructive and open-world so a client does not silently treat an unfamiliar operation as safe.

## Troubleshooting

**Tools appear, but OBS calls fail:** make sure OBS is open and its WebSocket server is enabled. The MCP process stays available while it retries the connection.

**Authentication fails:** copy the password from **Tools > WebSocket Server Settings** into `OBS_WEBSOCKET_PASSWORD`, then restart the MCP server process.

**The agent cannot see the tools:** restart the agent session or the MCP host so it creates a fresh stdio connection and reloads the tool list.

**A request is unsupported:** the connected OBS build did not advertise that WebSocket request. Use an operation supported by that version or update OBS.

**The process exits or emits protocol errors:** inspect stderr. stdout is reserved for MCP traffic and should not be redirected into ordinary application logs.

## Development

Install the pinned dependencies, then run the complete hermetic gate:

```bash
npm ci
npm run check
```

This does not require OBS. It builds with TypeScript 7, type-checks the tests, runs the unit suite, starts the compiled stdio server against a fake OBS WebSocket endpoint, exercises modern and legacy MCP clients, and validates the manifest. The end-to-end cases cover connection recovery, request routing, clean EOF and signal shutdown, structured results, bounded screenshot payloads, and session survival after a rejected screenshot.

Test the file users will actually install with:

```bash
npm run test:package
```

That command creates `dist/obs-studio.mcpb`, extracts it into a temporary directory, validates its manifest and source contents, checks all 158 tool definitions, and calls the extracted server through MCP against fake OBS.

### Live OBS tests

The live suite is separate from `npm run check` and CI. By default it is skipped. With OBS open and the standard WebSocket environment variables already available to the process, run the read-only checks with:

```bash
OBS_MCP_LIVE_TEST=1 npm run test:obs-live
```

The read-only lane checks the OBS version, scenes, active scene collection, streaming state, and recording state through the compiled MCP stdio server.

There is also a narrowly bounded mutation test. Use a disposable scene collection, select it in OBS first, and make sure streaming and recording are stopped. Then run:

```bash
OBS_MCP_LIVE_TEST=1 \
OBS_MCP_LIVE_MUTATION=1 \
OBS_MCP_LIVE_SCENE_COLLECTION="OBS MCP Test" \
npm run test:obs-live
```

The suite refuses to mutate if the selected collection does not exactly match `OBS_MCP_LIVE_SCENE_COLLECTION` or if either output is active. It creates one uniquely named scene and removes it in a `finally` block.

Add `OBS_MCP_LIVE_RECORD=1` to the mutation command to also record a one-second clip. It checks that `obs-start-record` reports success only after OBS confirms the output is active, then stops the recording in a `finally` block. The clip is saved to the profile's recording directory and captures whatever the selected collection shows, so select a collection whose sources are safe to record.

Pull-request CI runs the hermetic gate on Node 20.19, 22, and 24, plus the packaged-artifact smoke test on Node 22. Dependency auditing runs in a separate weekly workflow so registry advisories do not make otherwise reproducible pull-request checks flaky.

### TypeScript policy

The application builds with TypeScript 7.0.2 and does not import the TypeScript compiler API. If a future generator, linter, or build tool needs programmatic compiler access, keep that tool on TypeScript 6 while the application compiler remains on TypeScript 7. TypeScript 7.0 does not ship a programmatic API; Microsoft documents the side-by-side TypeScript 6 compatibility package in the [TypeScript 7.0 release notes](https://devblogs.microsoft.com/typescript/announcing-typescript-7-0/#running-side-by-side-with-typescript-6-0).

## Protocol baselines

This revision was checked against these exact sources:

- MCP specification: [`cbd57657`](https://github.com/modelcontextprotocol/modelcontextprotocol/commit/cbd57657ec769b942263a5251afc05959fc4ce58)
- MCP TypeScript server and client SDKs: `2.0.0`
- OBS Studio: [`0043697f`](https://github.com/obsproject/obs-studio/commit/0043697fc59d791b95b3495dfa1fe180b395966f)
- OBS WebSocket: [`1ef34bf4`](https://github.com/obsproject/obs-websocket/commit/1ef34bf48110c2a18184e50e41cd0b1a855e2147), the revision pinned by that OBS Studio checkout

The generated protocol files live under [`docs/`](docs/). Tests compare every request used by the tool modules with the bundled protocol, so obsolete request names and missing wrappers fail locally.

## Project history

This project began with [Roy Shilkrot's `obs-mcp`](https://github.com/royshil/obs-mcp), including upstream work by [Zeke Sikelianos](https://github.com/zeke). It also preserves [Jag-k's](https://github.com/jag-k/obs-mcp) MCPB packaging and `registerTool` migration work. Their original commits and authorship remain in the Git history.

## License

OBS MCP is licensed under GPL-2.0-only. Published npm and MCPB packages include the corresponding TypeScript source and build scripts. See [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).

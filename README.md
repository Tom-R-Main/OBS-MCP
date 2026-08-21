# OBS MCP

[![CI](https://github.com/Tom-R-Main/OBS-MCP/actions/workflows/ci.yml/badge.svg)](https://github.com/Tom-R-Main/OBS-MCP/actions/workflows/ci.yml)

OBS MCP gives MCP clients a current, inspectable interface to OBS Studio. It exposes 155 tools for scenes, sources, audio, transitions, filters, recording, streaming, canvases, and the rest of the OBS WebSocket v5 request surface.

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

The server never prints the password. Connection and protocol diagnostics are written to stderr so stdout remains reserved for MCP messages.

## Tool surface

The 155 tools are organized around the OBS protocol rather than a smaller opinionated workflow:

- server status, version information, statistics, hotkeys, and studio mode
- scenes, groups, sources, filters, and scene items
- input settings, audio tracks, volume, mute state, media controls, and deinterlacing
- recording, streaming, replay buffer, virtual camera, and output settings
- transitions, transition overrides, the transition cursor, and the T-Bar
- canvases, screenshots, profiles, scene collections, and persistent data
- protocol description and the guarded generic request fallback

Tool discovery works without an OBS connection. Calls that need OBS return an MCP error until the WebSocket connection is ready.

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

```bash
npm ci
npm run build
npm test
npm run validate:manifest
```

Run the complete local gate with:

```bash
npm run check
```

The tests cover the OBS WebSocket handshake and request correlation, concurrent connection attempts, capability gating, exact OBS request parity, deterministic tool discovery, annotations, and both legacy and MCP 2026-07-28 negotiation.

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

OBS MCP is now maintained as an independent repository with its own protocol baseline, test suite, packaging path, and maintenance direction. The attribution above records where the work started without presenting this repository as a GitHub-network fork.

## License

OBS MCP is licensed under GPL-2.0-only. Published npm and MCPB packages include the corresponding TypeScript source and build scripts. See [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).

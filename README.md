# OBS MCP

OBS MCP gives MCP clients a current, inspectable interface to OBS Studio. It exposes 155 tools for scenes, sources, audio, transitions, filters, recording, streaming, canvases, and the rest of the OBS WebSocket v5 request surface.

The server is built against MCP 2026-07-28 and still accepts legacy 2025-era clients. MCP discovery stays available when OBS is closed, and the server reconnects in the background when OBS returns.

## What is covered

- All 147 request types in the OBS WebSocket protocol revision pinned by the current OBS Studio source tree
- Explicit read-only, destructive, idempotent, and open-world annotations on every tool
- Runtime capability checks against the requests advertised by the connected OBS instance
- Protocol inspection through `obs-describe-request`
- A generic `obs-call-request` escape hatch, limited to requests in the bundled OBS protocol and marked destructive for client approval
- A reproducible `.mcpb` build with its tool inventory generated through MCP rather than private SDK internals

The explicit tools remain the normal interface. The generic request tool is there so a newly added OBS operation can be used before it receives a more ergonomic wrapper.

## Requirements

- Node.js 20.17 or newer
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

## Tool groups

The 155 tools are organized around the OBS protocol rather than a smaller opinionated workflow:

- server status, version information, statistics, hotkeys, and studio mode
- scenes, groups, sources, filters, and scene items
- input settings, audio tracks, volume, mute state, media controls, and deinterlacing
- recording, streaming, replay buffer, virtual camera, and output settings
- transitions, transition overrides, the transition cursor, and the T-Bar
- canvases, screenshots, profiles, scene collections, and persistent data
- protocol description and the guarded generic request fallback

Tool discovery works without an OBS connection. Calls that need OBS return an MCP error until the WebSocket connection is ready.

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

The inherited implementation is licensed under GPL-2.0-only. See [`LICENSE`](LICENSE).

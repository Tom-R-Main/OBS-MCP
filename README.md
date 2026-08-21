# OBS MCP Server

An MCP server for OBS Studio that provides 155 annotated tools over the built-in OBS WebSocket protocol.

This branch is synchronized with the OBS Studio `master` protocol snapshot and the released MCP 2026-07-28 specification. Its stdio entrypoint negotiates both modern MCP 2026-07-28 clients and legacy 2025-era clients.

## Features

- Keeps MCP tool discovery available while OBS is offline and reconnects automatically
- Covers all 147 request types in the synchronized OBS WebSocket protocol snapshot
- Includes a safe protocol-description tool and an explicitly destructive generic request fallback for newly added OBS operations
- Provides tools for:
  - General operations
  - Scene management
  - Source control
  - Scene item manipulation
  - Streaming and recording
  - Transitions


## Usage

1. Make sure OBS Studio is running with WebSocket server enabled (Tools > WebSocket Server Settings). Note the password for the WS.
2. Set the WebSocket password in environment variable (if needed):

```bash
export OBS_WEBSOCKET_PASSWORD="your_password_here"
```

3. Add the MCP server to Claude desktop with the MCP server settings:

```json
{
  "mcpServers": {
    "obs": {
      "command": "npx",
      "args": ["-y", "obs-mcp@latest"],
      "env": {
        "OBS_WEBSOCKET_PASSWORD": "<password_from_obs>"
      }
    }
  }
}
```

4. Use your MCP client to control OBS.

## Installation via .mcpb package

You can also install OBS MCP as a `.mcpb` package directly in a compatible desktop client. Build `dist/obs-studio.mcpb` from this branch using the command below; a release artifact has not yet been published for this fork.

## Development

Install dependencies, verify the project, and run the server:


```bash
npm ci
npm run check
npm run start
```

Then configure Claude desktop:

```json
{
  "mcpServers": {
    "obs": {
      "command": "node",
      "args": [
        "<obs-mcp_root>/build/index.js"
      ],
      "env": {
        "OBS_WEBSOCKET_PASSWORD": "<password_from_obs>"
      }
    }
  }
}
```

### Building the .mcpb package

```bash
npm run build
npm run pack  # → dist/obs-studio.mcpb
```

`npm run pack` builds the project, collects the public tool list through MCP, installs production-only dependencies in an isolated staging directory, validates the manifest, and produces the package without retaining staging files.

## Available Tools

The server provides tools organized by category:

- General tools: Version info, stats, hotkeys, studio mode, protocol introspection
- Scene tools: List scenes, switch scenes, create/remove scenes
- Source tools: Manage sources, settings, audio levels, mute/unmute
- Scene item tools: Manage items in scenes (position, visibility, etc.)
- Streaming tools: Start/stop streaming, recording, virtual camera
- Transition tools: Set transitions, durations, trigger transitions
- Canvas, audio-track, deinterlace, group, source, and scene-item operations from the current OBS protocol

## Environment Variables

- `OBS_WEBSOCKET_URL`: WebSocket URL (default: ws://localhost:4455)
- `OBS_WEBSOCKET_PASSWORD`: Password for authenticating with OBS WebSocket (if required)

## Requirements

- Node.js 20.17+
- OBS Studio 28+ with WebSocket server enabled; current protocol additions require an OBS build that advertises those requests
- An MCP 2026-07-28 or compatible legacy MCP client

## Protocol Baselines

- MCP SDK: modular `@modelcontextprotocol/server` and `@modelcontextprotocol/client` 2.0.0, implementing MCP 2026-07-28
- MCP specification checkout: [`cbd57657`](https://github.com/modelcontextprotocol/modelcontextprotocol/commit/cbd57657ec769b942263a5251afc05959fc4ce58)
- OBS Studio checkout: [`0043697f`](https://github.com/obsproject/obs-studio/commit/0043697fc59d791b95b3495dfa1fe180b395966f)
- OBS WebSocket protocol: generated from the checkout's pinned [`1ef34bf4`](https://github.com/obsproject/obs-websocket/commit/1ef34bf48110c2a18184e50e41cd0b1a855e2147) revision
- Unsupported OBS requests are capability-gated at runtime with a descriptive error

## License

See the [LICENSE](LICENSE) file for details.

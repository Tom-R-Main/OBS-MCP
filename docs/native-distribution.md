# Native distribution

OBS MCP runs as a Rust executable. Its main entry supports stdio and loopback HTTP; the native MCPB layout contains no Node runtime, JavaScript SDK or TypeScript server payload. The TypeScript implementation remains an explicit development oracle.

Use a build qualified for your host and CPU. On 2 November 2026, the optimized candidate passed the normal native gate and 1,002 public cases with no skips. Its matching macOS MCPB passed three extraction checks and a bounded real-OBS fixture journey. Linux qualification is in progress without a platform/runtime acceptance claim; production TLS, final-head CI and client activation remain pending. See [cutover status](full-rust-cutover.md) for exact artifact hashes and evidence limits.

## Build and configure

With Rust 1.95.0 and the lockfile dependencies already available:

```sh
cd native
RUSTUP_AUTO_INSTALL=0 cargo build --locked --offline --release --bin obs-mcp-native
```

This compiles checked native assets and local path crates directly. It needs no TypeScript build, Source export, npm install or Node executable. A missing toolchain or dependency cache is a prerequisite to resolve through a separately approved fetch, not permission for an automatic install.

Choose an existing canonical state directory that you approve for journals and snapshots. For example, deliberately create a private directory in your project:

```sh
mkdir -m700 .obs-mcp-state
```

Configure absolute canonical paths in your MCP client:

```json
{
  "mcpServers": {
    "obs": {
      "command": "/absolute/path/to/OBS-MCP/native/target/release/obs-mcp-native",
      "args": [],
      "env": {
        "OBS_MCP_STATE_DIR": "/absolute/path/to/OBS-MCP/.obs-mcp-state",
        "OBS_MCP_NATIVE_PROFILE": "production",
        "OBS_WEBSOCKET_URL": "ws://localhost:4455"
      }
    }
  }
}
```

Discovery works while OBS is closed after valid local startup. Unsafe paths, symlink ancestors or replaced storage refuse explicitly. The runtime does not initialize an unsolicited home namespace or erase history after a failure. Do not delete, copy or edit journals to manufacture a clean OBS state.

`--help` and `--version` finish without an OBS connection. Other arguments refuse. `OBS_MCP_NATIVE_STATEFUL` is retired; the service is always native. `OBS_MCP_COMPAT_NODE` and `OBS_MCP_COMPAT_ENTRY` have no effect. `OBS_MCP_FORCE_COMPAT` refuses instead of starting Node.

## Share one server over HTTP

```sh
OBS_MCP_STATE_DIR=/absolute/path/to/.obs-mcp-state \
OBS_MCP_HTTP_PORT=8765 \
native/target/release/obs-mcp-native
```

Connect clients to `http://127.0.0.1:8765/mcp`. The listener binds only to `127.0.0.1`; Host and Origin checks allow only the supported loopback names. Set an opaque `OBS_MCP_HTTP_TOKEN` through your approved secret configuration to require a bearer token on every request. Tokens and OBS passwords are not printed.

The port accepts decimal digits from 0 through 65535 after JavaScript-style whitespace trimming. Leading zeros are accepted; `0` selects an ephemeral port. An absent, empty or whitespace-only value selects stdio. Signed, hexadecimal, exponent and fractional forms refuse before binding. HTTP writes its safe listening URL to stderr and keeps stdout empty. Stdin EOF ends stdio service; HTTP ignores it and uses SIGINT or SIGTERM for joined shutdown.

HTTP clients share one retained service and OBS connection. Static tool groups apply; `OBS_MCP_DYNAMIC_TOOLSETS` is ignored over HTTP. Give legacy clients a distinct `?client=name`; modern clients also supply their name through MCP metadata. Names support cooperating control leases and are not authentication credentials.

## Profiles and media prerequisites

The profile is immutable after startup. Production is the default and exposes up to 164 ordinary tools with all approved host/media adapters. Advanced exposes 163 ordinary tools and does not construct guarded Take, media, Stage or recovery owners. Their union contains 178 ordinary tool names; stdio dynamic mode can add three group controllers. Filters, read-only mode and missing adapters narrow discovery. See [native profiles](native-profiles.md) for the persistent barriers around advanced effects and guarded reentry.

Production Take, Clip and file workflows require an existing approved recording directory through `OBS_MCP_NATIVE_RECORDING_ROOT`. Media inspection and editing also need trusted existing FFmpeg and ffprobe executables, selected with `OBS_MCP_NATIVE_FFMPEG` and `OBS_MCP_NATIVE_FFPROBE` or PATH. The package installs neither program. A directory or executable binding grants no recording, file or recovery authority; operation-specific preflight and user permission remain necessary.

Get explicit permission before capturing a window, recording, streaming, saving files or changing live output. Use a disposable collection and a bounded plan for qualification. An OBS acknowledgment proves acceptance, not the final scene, rendered picture or recording contents.

## DNS and encrypted OBS endpoints

Plain `ws:` connections use owned DNS resolution when needed. Resolver helpers have bounded lifetimes and are joined during shutdown; one client leaving does not discard another client's service or Take.

Native TLS is off in the default build. A `wss:` endpoint refuses before connection rather than downgrading or launching a compatibility runtime. This narrows supported OBS transports from the former SDK route; native runtime cutover does not yet provide full Source transport parity. The `platform-tls-evaluation` feature selects the macOS Network.framework backend or, on Linux, the GnuTLS backend. These conditional backends have separate component evidence and prerequisites; enabling the feature is not a platform or production TLS acceptance claim. Linux evaluation needs GnuTLS and pkg-config. See [architecture](native-architecture.md) and [cutover gates](full-rust-cutover.md).

## Package and corresponding source

With the explicit development archive tooling available:

```sh
npm run pack:native
```

The packer builds `dist/obs-studio-native.mcpb` for its host. Node and the MCPB CLI are packaging tools, not installed runtime dependencies. Staging installs no npm dependencies and performs no Source server build or discovery launch.

The bundle layout retains corresponding Rust source, checked native assets, protocol metadata, manifests, lockfiles, pinned toolchain files and required local path-crate source at their relative Cargo paths. Target directories and Cargo caches are excluded. Build inputs, executable and extracted artifact must refer to the same checked candidate.

GPL-2.0-only, authorship and dependency notices remain intact. Extraction must exercise the executable with Node unavailable, both MCP eras, applicable profiles/transports and joined cleanup. The observed macOS candidate passed three package checks, including a real localhost self-helper connection with a Node trap. Its 7.8 MB executable and 4.9 MB archive are identified in [cutover status](full-rust-cutover.md).

That extracted executable also completed the authorized Mac Capture/Clip/recovery and manual-Restore journey. A 2.837-second silent two-state fixture clip passed strict decoding and sampled-content checks; screenshots/contact sheet were reviewed. Original, restored and post-cleanup state snapshots matched exactly, and owned helpers stopped. A fresh snapshot-list process made zero OBS TCP connections. These observations belong to that artifact and one Mac setup; they do not qualify Linux, production TLS, color fidelity or client activation. Repeat the relevant checks when changing an artifact or target.

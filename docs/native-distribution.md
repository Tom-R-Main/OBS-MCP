# Native distribution

OBS MCP runs as a Rust executable. Its main entry supports stdio and loopback HTTP; the native MCPB layout contains no Node runtime, JavaScript SDK or TypeScript server payload. The TypeScript implementation remains an explicit development oracle.

Use a build qualified for your host and CPU. Core checkpoint `d3bb516` passed ten CI jobs. The physical-Mac gate passed 1,016 public cases and 3/3 package checks, then the exact extracted executable completed the bounded real-OBS journey. A separate exact-main Linux executable passed focused cadence/HTTP/raw-stdio checks; its VM had no Source oracle or package qualification. Exact-window capture remains unqualified. The opt-in TLS variant has separate bounded Mac evaluation evidence; general production admission remains open. [Cutover status](full-rust-cutover.md) records the exact identities and limits.

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

Native TLS is off in the default core build. A `wss:` endpoint refuses before connection without downgrade or compatibility handoff. The opt-in TLS variant adds an opt-in macOS Network.framework or Linux GnuTLS evaluation backend; its Mac custom-CA read-only evidence does not establish full Source transport parity or Linux TLS admission.

### Opt-in TLS evaluation

Pass the feature selection on the pack invocation so the release build and staged build receipt preserve it:

```sh
OBS_MCP_NATIVE_BUILD_FEATURES=platform-tls-evaluation \
node scripts/pack.mjs --offline
```

This creates `dist/obs-studio-native-tls-evaluation.mcpb`; the default output remains `dist/obs-studio-native.mcpb`. The pinned toolchain, dependencies and archive tools must already be available. Linux evaluation also requires GnuTLS and pkg-config. The evaluation pack driver checks host, selected feature, source and executable identity before staging. Node is a packaging tool, not an installed runtime requirement. The core and TLS qualification artifacts have separate source and executable bindings.

The evaluation runtime optionally accepts `OBS_MCP_TLS_CA_FILE`, an absolute safe regular file containing at most 256 KiB and 32 certificate-only PEM anchors. Trust additions are process-local. Do not include private keys or rely on a symlink/changed file; certificate and peer-name validation remain enabled. No global trust store or keychain is changed. Setting the CA path does not enable a feature-off build.

The exact optimized/extracted Mac evaluation passed DNS/IP custom-CA TLS1.2/1.3 authentication/GetVersion and three early peer-trust refusals through an owned proxy. Default TLS remains off; system-root-positive, Linux TLS, general production and activation acceptance remain separate. See [cutover evidence](full-rust-cutover.md#tls-evaluation-qualification).

## Package and corresponding source

With the explicit development archive tooling available:

```sh
npm run pack:native
```

The packer builds `dist/obs-studio-native.mcpb` for its host. Node and the MCPB CLI are packaging tools, not installed runtime dependencies. Staging installs no npm dependencies and performs no Source server build or discovery launch.

The bundle layout retains corresponding Rust source, checked native assets, protocol metadata, manifests, lockfiles, pinned toolchain files and required local path-crate source at their relative Cargo paths. Target directories and Cargo caches are excluded. Build inputs, executable and extracted artifact must refer to the same checked candidate.

GPL-2.0-only, authorship and dependency notices remain intact. Extraction must exercise the executable with Node unavailable, both MCP eras, applicable profiles/transports and joined cleanup. The observed macOS candidate passed three package checks, including a real localhost self-helper connection with a Node trap. Its exact executable and pre-document-refresh archive are identified separately from the TLS evaluation in [cutover status](full-rust-cutover.md).

The extracted core executable completed the authorized Mac Capture/Clip/recovery and manual-Restore journey. Its 2.837-second silent clip passed strict decoding, sampled content and exact presented-frame cadence checks; screenshots/contact sheet were reviewed. Original, restored and final snapshots were byte-identical, all owned helpers stopped, and a fresh snapshot-list process made zero OBS TCP connections. These observations belong to that core artifact and Mac setup. Linux exact-window capture, general production TLS, color fidelity and installed-client activation retain the limits in [cutover status](full-rust-cutover.md).

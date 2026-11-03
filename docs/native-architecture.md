# Native architecture

OBS MCP has one retained Rust service per server process. Stdio serves one MCP frontend; loopback HTTP gives clients separate response and subscription contexts over the same service and OBS connection. The executable and native MCPB layout contain no JavaScript compatibility launcher or Node server payload. TypeScript and SDK 2.4 remain explicit development oracles.

Start with [distribution and configuration](native-distribution.md). [Cutover status](full-rust-cutover.md) separates merged core evidence, Linux tester limits, the opt-in Mac TLS evaluation and remaining production/activation work.

## Admission and profiles

Protocol admission owns the request's identity, era and caller facts. Modern inline metadata and legacy initialization are validated before tool preparation. Internal exchange identities do not reuse client wire IDs; counters refuse exhaustion rather than wrapping. Original Apply Scene and Capture Window inputs remain typed and owned through preparation, consent and dispatch.

Production and Advanced are immutable capability profiles. Production exposes up to 164 ordinary tools with all adapters; Advanced exposes 163 and does not construct guarded Take/media/Stage/recovery owners. The ordinary union is 178 names. Stdio dynamic mode can add three group controllers; HTTP deliberately uses static groups and does not retain legacy initialize offers as authority for later requests.

Availability, registration and enabled state are distinct. Changing visibility never grants effect authority or releases an admitted job. Confirmation grants are single-use and bound to the original caller, arguments and profile. A hidden-tool refusal consumes an admitted grant; enabling the tool again cannot revive it.

## Shared connection lifetime

The host retains one lazy bootstrap attempt with one absolute establishment deadline. Discovery, local control and unsupported-method replies remain available while it waits. Runtime keeps offline preparation ownership until it can adopt the connected session, observer and capability cache into Service. A departing frontend cancels its own admission/publication, not bootstrap custody, peer work or a Take.

The service owns the physical OBS socket, event observer, operation writer and workflow resources. Typed connection generations and nonwrapping request sequences bind responses to their owner. OBS capability discovery is cached on that same connection; a reconnect or another client's response cannot supply missing authority.

Owned DNS helpers resolve an expected peer under the same establishment deadline. Their owner retains admitted work across dropped waits and performs kill/wait/reap cleanup. TLS connectors carry connection capability while Runtime retains the outer cleanup owner. Shutdown collects Service, DNS and TLS outcomes independently: a Service error cannot skip the other joins or manufacture a successful cleanup receipt.

## OBS evidence and response deadlines

The sole observer publishes revisions before its processed event cursor. A read crosses the reply's event watermark before treating evidence as unchanged. Structural events, cancellation, connection loss and generation changes invalidate the affected sample. Planning proposes changes; structural audit does not establish rendered content or readiness to record.

A fully flushed, fixed typed observational request may retire only its response waiter at its original deadline or after abandonment. Late responses have never-reused identities and cannot satisfy another call, including an active batch. This preserves a healthy socket without retrying the read. Any incomplete write, arbitrary request, batch, effect or reserved Stop retains conservative Unknown/connection-retirement behavior. Timeout diagnostics preserve the workflow's original budget; they do not extend it.

An ACK is Accepted evidence. It does not establish observed fields, composed pixels, file contents or descendant plugin settlement. Lost or malformed effect replies remain Unknown; no readback or frontend retry silently promotes them to success.

## Durable effects and media

The endpoint-bound operation writer persists intent before effects and checks current caller policy again at actual send. Versioned receipts distinguish prepared, dispatching, acknowledged, rejected, not-dispatched and unknown facts within their own workflow contract. Apply Scene retains creation and change phases; manual Restore retains its complete serial batch. Neither continues or replays after uncertain restart.

Advanced/manual output history can permanently block guarded reentry in the approved namespace. A new process, inactive status sample, endpoint alias or copied journal is not a reset proof. Only the transport's typed NotDispatched result proves that an attempt sent no effect. [Native profiles](native-profiles.md) describes these boundaries.

Take/Clip, preparation/recovery and media owners retain their jobs through cancellation and settlement. Recording/file workflows require an approved recording root; decoding/encoding uses trusted existing FFmpeg and ffprobe. Media cleanup follows confirmed resource joins and file identity checks. A publication or cleanup failure cannot erase a committed result or authorize deletion of another owner's file.

## HTTP, capacity and shutdown

The actual entry selects stdio for an absent or JavaScript-whitespace-only HTTP port. Trimmed decimal digits select loopback HTTP; zero selects an ephemeral port. Signed, fractional, exponent and hexadecimal forms refuse. Host/Origin and optional bearer authentication run before OBS work. HTTP writes its listening URL to stderr and keeps stdout empty; stdin EOF does not end an HTTP server.

The HTTP stack uses mature HTTP framing with bounded headers, bodies, connections and writers. Ordinary work shares global admission and byte accounting; separately reserved typed Take Stop and safety Stop capacity cannot be multiplied by clients or self-declared by an unparsed body. These reservations do not promise progress through an incomplete frame, unavailable storage or unlimited Stop requests.

Each frontend has its own response and stream lifetime. One slow or disconnected consumer cannot shut down a peer's service or Take. Resource delivery freezes recipients under the routing lock and releases it before callbacks or cancellation; encoded-byte leases remain charged until actual packet ownership ends. A global shutdown stops admission and joins admitted jobs, Service and connection drivers. Item/encoded-byte bounds do not establish a bound on total process RSS or kernel latency.

## Encrypted transport status

Default core builds refuse `wss:` before connection without plaintext downgrade or SDK handoff. The TLS variant is an opt-in evaluation using Network.framework on macOS or GnuTLS on Linux. Its feature-aware pack driver preserves `OBS_MCP_NATIVE_BUILD_FEATURES=platform-tls-evaluation` through release build and staged source/executable receipts. An optional `OBS_MCP_TLS_CA_FILE` adds process-local certificate-only anchors while retaining peer/certificate validation; it makes no global trust/keychain change.

The exact optimized/extracted Mac evaluation passed four DNS/IP custom-CA TLS1.2/1.3 authenticated real-OBS GetVersion paths and three TLS1.3 trust refusals before Upgrade/Identify/upstream OBS. ALPN was observed as `http/1.1`. Child/connection owners joined, certificate material was removed and frozen identities rechecked. This is bounded custom-CA proxy-backed evidence, not a system-root positive, Linux TLS or general production admission. Default TLS remains off.

Component checks remain separate from live acceptance, and root default tests do not automatically run a path dependency's units. Linux core CI and the separate exact-main tester checks have their own scope; the tester lacked Source-oracle/package qualification. Its missing `EGL_KHR_image_pixmap` leaves real XComposite exact-window capture unqualified; whole-display XShm is a different control.

## Build and verify

```sh
# Pinned toolchain and Cargo dependencies must already be available.
cd native
RUSTUP_AUTO_INSTALL=0 cargo build --locked --offline --release --bin obs-mcp-native
```

From the development checkout, `npm run native:check` builds the native executable and runs its Rust, Source/client, license and migration checks. Node is development tooling for these oracles and archives. The ratchet binds native sources, assets, local crates and configuration; it detects drift, not translation correctness. Reseal only reviewed changes, never a failing test.

Core checkpoint `d3bb516` passed ten CI jobs. Its separate physical-Mac qualification passed 736 Source units, 21 Source transport cases, 1,050 default-feature Rust cases, 214 all-feature bin/library cases, four resolver process contracts, seven component checks and 1,016 public cases with no skips. Suites overlap, and local counts are not substituted for Linux execution. The exact core MCPB passed 3/3 checks and the bounded Mac journey with conflict-preserving recovery, manual Restore, byte-identical state restoration and offline restart.

HTTP qualification retains default SDK pooling and exact test-only driver identity/drop ownership. Known-capability Source fixtures observe their public cache before comparison; Source production behavior is unchanged by those fixture controls. The fractional-FPS export correction is merged. The separate movie cadence diagnostic distinguishes discard-marked decoder references from presented frames while still checking every packet duration.

[Cutover status](full-rust-cutover.md) binds core and evaluation evidence to their exact artifacts. Linux desktop, production TLS, archive rebinding and installed-client activation remain scoped separately. Historical 83/166/167 checkpoint recordings and synthetic timings remain artifact-bound and establish no capture or encoding speed comparison.

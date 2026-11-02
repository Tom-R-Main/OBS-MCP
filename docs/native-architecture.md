# Native architecture

OBS MCP has one retained Rust service per server process. Stdio serves one MCP frontend; loopback HTTP gives clients separate response and subscription contexts over the same service and OBS connection. The executable and native MCPB layout contain no JavaScript compatibility launcher or Node server payload. TypeScript and SDK 2.4 remain explicit development oracles.

Start with [distribution and configuration](native-distribution.md). [Cutover status](full-rust-cutover.md) records the optimized, extracted-package and bounded Mac evidence, and the remaining Linux, production TLS, final CI and activation gates.

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

Default builds refuse `wss:` before connection, with no plaintext downgrade or SDK handoff. This is a supported-transport contraction from the former Source/SDK route; native runtime cutover does not establish full Source transport parity. The optional `platform-tls-evaluation` feature selects Network.framework on macOS and GnuTLS on Linux. Linux evaluation requires its native GnuTLS/pkg-config prerequisites; macOS's backend does not require enabling GnuTLS.

Platform bindings and resolver components have their own tests and cleanup contracts. Root default tests do not automatically run a path dependency's unit tests. Component and all-feature checks supplement the default gate. Production TLS still needs feature-on optimized and extracted checks per platform: secure OBS authentication/readback, trust/identity refusals, cancellation through establishment, and joined shutdown/restart. These checks remain separate from the default native gate. Linux qualification is in progress; no platform/runtime acceptance is inferred from the Mac results.

## Build and verify

```sh
# Pinned toolchain and Cargo dependencies must already be available.
cd native
RUSTUP_AUTO_INSTALL=0 cargo build --locked --offline --release --bin obs-mcp-native
```

From the development checkout, `npm run native:check` builds the native executable and runs its Rust, Source/client, license and migration checks. Node is development tooling for these oracles and archives. The ratchet binds native sources, assets, local crates and configuration; it detects drift, not translation correctness. Reseal only reviewed changes, never a failing test.

The 2 November 2026 normal gate passed 1,045 default-feature Rust cases, 213 all-feature bin/library cases, four resolver process contracts and the separate path-crate checks. Its public suite passed 1,003 cases with no skips against the pinned optimized executable; the development unit suite passed 735 cases. These suites overlap. The matching MCPB passed extraction checks and a bounded real-OBS Mac journey, with media inspection, conflict-preserving recovery, manual Restore, state restoration and offline snapshot restart.

[Cutover status](full-rust-cutover.md) binds those results to exact executable/package hashes and records the pending Linux, production TLS, final CI and activation gates. Architecture acceptance follows behavior and resource custody. Historical 83/166/167 checkpoint recordings and synthetic timings remain artifact-bound and establish no capture or encoding speed comparison.

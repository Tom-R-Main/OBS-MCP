# Native dependency decision — 31 October 2026

This preserves the dependency decision and earlier-artifact evidence from that
date. The native-default source proposal removes the compatibility child;
unsupported encrypted transport refuses until a native TLS backend is separately
adopted and qualified. References below to selecting the TypeScript TLS runtime
are historical, not fallback behavior in the native-only proposal. See
[native distribution](native-distribution.md) for current proposal boundaries.

When this tranche was written OBS MCP was `GPL-2.0-only`; it is MIT-licensed
as of November 2026 (see [NOTICE](../NOTICE.md)). This tranche does not copy code
from the official Rust MCP SDK. The local SDK declares `Apache-2.0`; it is not
admitted as a linked dependency. Apache-2.0 was incompatible with the earlier
GPLv2 license; admitting it now would still need its own inventory review.

The same check found a problem in the predecessor native candidate, beyond the
proposed SDK: its `rustls-tls-webpki-roots` feature enabled `ring@0.17.14`, whose
manifest declares **`Apache-2.0 AND ISC`**. Its bundled `LICENSE` identifies
Apache-licensed BoringSSL code and ISC-licensed code; this is not a choice of
either license. The strict inventory gate rejected it. The predecessor had 86
resolved packages including the application, of which 68 were active for this
Mac and 18 belonged to other target projections.

The root integration removed the native TLS feature and refreshed the lockfile
offline. The reviewed graph is now:

| Scope | Packages, including application where applicable |
| --- | ---: |
| All resolved targets | 61 |
| Active `aarch64-apple-darwin` projection | 53 |
| Other targets | 8 |
| Blocking license/inventory findings | 0 |

No dependencies were added or fetched. The 25 removed packages included ring,
rustls, their TLS support crates, and their exclusive build/target support.
Encrypted `wss:` connections are selected into the existing TypeScript
compatibility runtime before native OBS dispatch. Native `ws:` remains available.
This preserves the product's encrypted-connection route while leaving **native
TLS as a blocker to the full Rust cutover**. No compatible replacement TLS
backend was present in the project cache at inspection. Choosing one requires
source/license review and transport qualification; disabling certificate
validation or quietly downgrading `wss:` to `ws:` is not an alternative.

## Reproduce the gate

From the project root:

```sh
node scripts/check-native-licenses.mjs
npm run test:unit -- test/unit/native-licenses.test.ts
```

The gate runs the pinned toolchain from `native/`, disables automatic toolchain
installation, and invokes `cargo metadata --locked --offline` using the
project-local Cargo cache when present. It checks the all-target resolved graph
and separately projects the current host target. A target-only package is still
checked; absence from the host projection does not exempt its license.

`scripts/check-native-licenses.mjs` contains the exact reviewed package, version,
source, and license-expression inventory. New versions, added or removed
packages, changed expressions, missing manifests, and unreviewed sources fail
closed. There is no automatic adoption or override flag.

Every admitted expression retains its original metadata and records an explicit
choice: for example `MIT` from `MIT OR Apache-2.0`, and `MIT AND Unicode-3.0` from
`(MIT OR Apache-2.0) AND Unicode-3.0`. The historical `MIT/Apache-2.0` spelling for
`version_check@0.9.5` is explicitly reviewed; both license files exist in that
cached source. This is a small table of observed expressions, not an SPDX parser.
An unknown conjunction or an Apache-only dependency is rejected.

This gate is metadata and inventory evidence, not a complete file-level license,
notice, patent, distribution, or legal audit. A projected Cargo graph is not
proof of a compiled artifact or platform qualification. Packaging still needs
to retain the notices and source obligations of the final shipped artifact.

## Isolated TLS evaluation — 1 November 2026

A user-approved project-scoped evaluation fetched exact Rustls 0.23.46,
Tokio-Rustls 0.26.6, rustls-graviola 0.4.0, Graviola 0.4.2 and platform-verifier
0.7.1. These are evaluation dependencies only; the production manifest, lockfile
and strict gate remain unchanged. Actual Mac TLS/WSS checks cover trusted
synthetic roots, certificate rejection, timeout/cancellation cleanup and refusal
to retry plaintext. This is neither production admission nor a cryptographic audit.

The full source scan found an Apache-2.0-only header in Graviola 0.4.2
`src/low/x86_64/sha3_keccak_f1600.rs`, despite optional ISC alternatives in the
package manifest. That module is architecture-excluded on this arm64 Mac but
declared for x86_64 builds. The package-wide ISC assumption therefore does not
satisfy this project's all-target source policy. Cargo metadata also retains a
`ring` inventory entry while selected feature trees and actual Mac compilation
artifacts show no active ring edge. Both findings require resolution under the
existing policy; neither is waived by successful handshakes. This candidate is
**not admitted**. File-level source and notice inspection remains necessary in
addition to the current metadata gate.


## Native resolver and platform TLS proposal — 2 November 2026

The earlier counts and compatibility-runtime discussion above describe historical
artifacts. The native-only proposal has no TypeScript TLS fallback. Its default
feature set keeps `platform-tls-evaluation` off: unavailable `wss:` connections
refuse instead of downgrading or starting another runtime. Enabling an evaluation
feature is not release admission.

The reviewed source adds two private project crates, both version `0.0.0` and
`MIT`: `native/crates/resolver` and `native/crates/platform-tls`. They
retain the root [license](../LICENSE) and [project authorship notices](../NOTICE.md).
Only their exact canonical manifests and source identities are admitted; a local
path or `source=null` alone grants no license-policy exception. Resolver source
uses the existing Tokio dependency. The TLS crate gates Objective-C dependencies
to macOS and has no automatic Windows TLS backend.

The macOS evaluation uses these exact registry packages:

| Package | Declared license | Cargo author metadata |
| --- | --- | --- |
| `block2 0.6.2` | MIT | Mads Marquart |
| `objc2 0.6.5` | MIT | Mads Marquart |
| `objc2-encode 4.1.0` | MIT | Mads Marquart |

Their [attribution and pinned upstream declarations](../licenses/native/objc2-bindings/ATTRIBUTION.txt)
are preserved alongside the [standard MIT grant, conditions and disclaimer](../licenses/native/objc2-bindings/MIT-GRANT.txt).
The declarations designate MIT by reference. The author field is preserved as
metadata; no copyright holder or year has been invented, and another package's
copyright line has not been reassigned to these bindings. The upstream Apple
SDK discussion is preserved verbatim. These notices do not verify or grant Apple
SDK redistribution rights.

[The native notice inventory](../licenses/native/INVENTORY.txt) records exact
versions, archive/manifest hashes, selected expression choices and preserved
license, copyright, notice and author text. It covers the isolated 66-package
TLS/resolver graph before the separate HTTP dependency addition: 63 registry
packages, with 120 existing upstream text files from 60 packages; the three
bindings use their exact upstream declarations and accompanying MIT text.
Alternative license texts are retained where supplied; the inventory records
which expression branch the checker reviewed. Final combined packaging must
merge the HTTP notices and verify every retained file against its source seal.

The checker follow-on requests `cargo metadata --locked --offline --all-features`
without a target filter for its full inventory, then separately projects default
features onto the host. Packages outside that default-host projection include
both other targets and optional features disabled on this same host. A successful
metadata gate is not proof of a linked or distributed artifact.

### System build and runtime prerequisites

The macOS evaluation dynamically links system `Network`, `Security` and
`Foundation` frameworks, the Objective-C runtime, and normal macOS libraries.
The block implementation comes from the Apple system runtime in the selected
feature set; no separate compiler-rt BlocksRuntime is bundled. A suitable Apple
SDK/linker is a build prerequisite. The notice overlay bundles no Apple SDK or
system framework. The observed Mac build does not establish a minimum supported
macOS version or qualify another architecture/OS release.

The proposed Linux TLS backend dynamically links system GnuTLS. Building that
backend requires native `pkg-config` metadata and linker/development files for
GnuTLS; the current API/protocol floor is `3.6.4`, not a security-version
recommendation. No dependency is installed automatically. An eventual Linux
artifact requires an actual ELF/runtime and transitive-library inventory,
certificate-store checks and target qualification before packaging. The local
GnuTLS `3.8.12` probe runs on macOS and is not Linux acceptance.

The installed GnuTLS evaluation README distinguishes its main LGPL-2.1-or-later
libraries from GPL-3.0-or-later applications and other components. Its complete
[upstream README, authors and LGPL text](../licenses/native/GnuTLS-3.8.12-evaluation/UPSTREAM-README.txt)
are retained as evaluation provenance, including the upstream discussion of
Nettle/GMP. This does not admit an unreviewed transitive binary closure or bundle
GnuTLS command-line tools. Final native backend admission, extracted-package
notice verification, and platform/runtime qualification remain separate gates.

## Isolated HTTP1 proposal — 2 November 2026

User-approved exact HTTP dependencies were fetched into a project-owned Cargo
home. Hyper1.11.1 (`server,http1`, defaults off), hyper-util0.1.20 (`tokio`,
defaults off), http-body1.1.0 and http-body-util0.1.5 use the existing
bytes1.12.1/Tokio1.54.1 versions. The graph adds8 package names/versions and
changes no existing version; direct httparse1.10.1 reuses its reviewed package.
The prototype's all-target graph is69 packages including the application,
61 active on this Mac and8 target-only. This precedes later TLS composition.

All26 registry packages in the dependency qualification match Cargo.lock archive
checksums and extracted source payloads. Exact graph/features/source/VCS/license
receipts live in the isolated dependency review; full added license texts and an
additive inventory are checked in under `licenses/native/`. The strict package
inventory admits only these8 exact new tuples; existing choices remain intact.
This is a source/provenance/metadata review, not an external advisory scan or
permission to remove existing project notices.

The test-only TCP proposal uses mature httparse first-head facts and exact byte
replay into Hyper. It explicitly chooses one exchange per connection plus
`Connection: close`, refuses all Expect forms with417 pending a qualified interim
response API, and fails closed on unqualified URL/authority canonicalization.
It does not claim complete Node/WHATWG equivalence or product activation.
Root-owned startup wiring, composed platform TLS, final packaging and live
qualification remain separate gates.

# Native dependency decision — 31 October 2026

OBS MCP remains `GPL-2.0-only`. This tranche does not relicense the project or
copy code from the official Rust MCP SDK. The local SDK declares `Apache-2.0`;
it is not admitted as a linked dependency. GNU's
[compatibility explanation](https://www.gnu.org/licenses/license-compatibility.en.html)
identifies Apache-2.0 as incompatible with GPLv2.

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

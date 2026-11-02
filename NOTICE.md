# OBS MCP notices

OBS MCP is maintained by Thomas Main at <https://github.com/Tom-R-Main/OBS-MCP>.

The project began with Roy Shilkrot's `royshil/obs-mcp` in March 2025 and includes upstream work by Zeke Sikelianos from July and August 2025. It also preserves Jag-k's April 2026 MCPB packaging and `registerTool` migration work. The Git history retains their original commits and authorship.

Thomas Main began the independent modernization in August 2026. Modified source files carry a notice pointing here and to the Git history for authorship and change dates.

The complete project is distributed under GPL-2.0-only. See `LICENSE` for the license terms. Previous Node packages include the corresponding TypeScript source and build scripts. Native packages carry corresponding Rust source and build inputs; the TypeScript development oracle remains in the repository.

The generated OBS protocol data is based on `obsproject/obs-websocket` commit `e28f84975984340e84fc4879cfc05741cc0afde1`, the revision pinned by the OBS Studio baseline documented in the README.

Upstream references:

- <https://github.com/royshil/obs-mcp>
- <https://github.com/zeke/obs-mcp>
- <https://github.com/jag-k/obs-mcp>


## Native source and dependency notices

The private `native/crates/resolver` and `native/crates/platform-tls` source is
part of OBS MCP under GPL-2.0-only. The root license and all preceding project
attribution remain in force. Native-only packaging must retain its Rust source,
Cargo/build inputs, and the notice files corresponding to the final artifact.

The optional macOS TLS evaluation uses `block2 0.6.2`, `objc2 0.6.5` and
`objc2-encode 4.1.0` from <https://github.com/madsmtm/objc2>. Their Cargo author
metadata names Mads Marquart. Exact upstream declarations designate MIT; their
full text and Apple SDK discussion, author metadata, and the standard MIT
permission/conditions/disclaimer are retained under
[`licenses/native/objc2-bindings`](licenses/native/objc2-bindings/ATTRIBUTION.txt).
No copyright year or holder is inferred from author metadata. These notices
neither relicense OBS MCP nor verify Apple SDK redistribution rights.

[`licenses/native/INVENTORY.txt`](licenses/native/INVENTORY.txt) identifies the
reviewed native dependency versions and retained upstream license, copyright,
notice and author texts. Preserve these files in native artifacts. This isolated
inventory precedes the separate HTTP dependency addition; final packaging must
merge and verify that additive notice set too.

The TLS evaluation feature remains off by default. Apple frameworks are system
prerequisites; no SDK/runtime is redistributed by these notice files. The GnuTLS
texts under `licenses/native/GnuTLS-3.8.12-evaluation` document the installed
local evaluation prerequisite and do not claim a qualified Linux binary or
transitive-library closure. See [native dependency boundaries](docs/native-dependencies.md).

## Native HTTP transport proposal — November 2026

The isolated native HTTP proposal adds Hyper1.11.1, hyper-util0.1.20,
http-body1.1.0, http-body-util0.1.5, atomic-waker1.1.2,
futures-channel0.3.34, httpdate1.0.3 and smallvec1.16.4.
Their exact upstream license texts are retained under
`licenses/native/registry/`; the additive inventory is
`licenses/native/HTTP-INVENTORY.txt`. The reviewed expressions permit the
recorded MIT choice. Existing httparse1.10.1 is used directly without upgrading
its reviewed version. This proposal does not alter project provenance or the
project license, and does not activate an HTTP listener in the product entry.

# Dependency security assessment

Checked on October 14, 2026. This records a remaining dependency advisory, not an audit exemption.

## Runtime license constraint

The project is GPL-2.0-only. Review of the published MCP 2.0, 2.1, and 2.2 tarballs found the same mixed LICENSE text involving Apache-2.0 despite their MIT package metadata. SDK 2.3 synchronizes its package metadata with that existing license text; it does not introduce this distribution concern. Apache's [GPL compatibility guidance](https://www.apache.org/licenses/GPL-compatibility) explains the GPLv2 incompatibility.

Returning to an earlier SDK version does not resolve the published-license discrepancy. This pass updates the source integration to SDK 2.3.1 and tests a local artifact; it does not change the project license or publish a package.

The GPL choice follows the inherited `royshil/obs-mcp` implementation. This fork introduced explicit GPL-2.0-only metadata and source notices in August 2026. OBS Studio and OBS WebSocket explicitly permit GPLv2 or later, so their licensing alone does not establish a GPLv2-only restriction. Roy's upstream ships GPLv2 license text without a separate clear later-version grant; the sample later-version wording in the license appendix is not a project-specific grant. Establish the applicable inherited permissions before changing the combined project to GPLv3 or claiming MIT availability. This is a license-alignment question for redistribution, not a blocker on source integration or private local verification.

The offline dependency guard checks pinned versions, registry sources, integrity, lockfile correspondence, and installation hooks. License compatibility requires a separate review of actual license documents; package metadata and version numbers are insufficient here. Development tools and files included in the source distribution also require their own assessment.

## Remaining advisory

The development dependency path `@anthropic-ai/mcpb@2.1.2 → node-forge@1.4.0` remains affected by high-severity [GHSA-86w9-cpqp-85rv / CVE-2026-85393](https://github.com/advisories/GHSA-86w9-cpqp-85rv). The advisory covers versions through 1.4.0 and lists no patched version. It concerns RSA PKCS#1 v1.5 signature verification accepting malformed nested DigestAlgorithm elements, which can enable signature forgery with low-exponent RSA keys.

The installed lockfile marks both packages as development dependencies. Neither is part of the server's production dependency graph. Packaging installs production dependencies with `npm ci --omit=dev --ignore-scripts --no-audit`; the MCPB tool itself runs from the development checkout.

## Reachability in this project

These findings are based on the installed `@anthropic-ai/mcpb@2.1.2` source and the project's current call sites. They describe this workflow, not every application of node-forge.

| Project operation | MCPB call path | Relevant behavior |
| --- | --- | --- |
| `npm run pack` | CLI `pack` → `packExtension` → `validateManifest` | Validates the local manifest and icon, collects local files, and creates a ZIP. It does not sign the archive or verify RSA signatures. |
| `npm run test:package` | `unpackExtension`, then `validateManifest` | Extracts the locally built artifact. Signature-footer extraction uses Buffer operations; it does not authenticate the signature or call forge verification. |
| `npm run validate:manifest` | CLI `validate` → `validateManifest` | Parses JSON, applies the manifest schema, and checks the local icon. It does not verify signatures. |
| Production MCP server | `src/index.ts` and server modules | No import or invocation of MCPB or node-forge was found. |

MCPB imports its signing module, which imports node-forge, even when these operations do not call its cryptographic functions. Importing the package therefore does not establish that the vulnerable verification operation executes. No project invocation of `signMcpbFile` or `verifyMcpbFile` was found.

An additional limitation exists in the installed verifier: MCPB's `verifyMcpbFile` calls `p7.verify`, but node-forge 1.4.0's [PKCS#7 verifier](https://github.com/digitalbazaar/forge/blob/v1.4.0/lib/pkcs7.js) throws because verification is unimplemented. MCPB catches that failure and reports the file as unsigned. This is not a patch for the RSA advisory, and the MCPB verifier should not be treated as a working authenticity boundary.

## Mitigation and follow-up

Keep the development tool out of production installations and build artifacts, keep dependency lifecycle scripts disabled, and restrict the package smoke test to the artifact built by this checkout. The current workflow does not depend on MCPB signature verification to decide whether an external bundle is trusted.

These measures limit exposure; they do not repair node-forge or make the complete development graph vulnerability-free. The weekly audit continues to report the advisory without suppression. Recheck upstream releases and replace the affected development dependency when a reviewed fix becomes available. Any new signing or verification workflow requires a fresh assessment and a functioning independent authenticity mechanism before accepting external bundles.

Read-only verification confirmed the development-only lockfile classification, exercised `validateManifest` without entering forge signing/RSA-key construction, and confirmed that the installed PKCS#7 verifier throws. Reachability for packing and unpacking was assessed from their source paths; this assessment is not an exploit proof or a cryptographic audit.

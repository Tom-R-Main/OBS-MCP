# Frame and cue asset

The frame-and-cue icon was created in this project's agent-assisted development session on 1 November 2026. Its SVG geometry was authored directly for the brief: two offset video frames and one cue point. It represents preview, program and a deliberate cue without depending on a future product name.

The master is `frame-and-cue.svg`. It contains two rounded rectangles and one circle, with fixed colors and a transparent background. It includes no fonts, embedded images, external links or third-party vector paths. The preceding icon was inspected during the replacement audit; it was not traced, sampled, embedded or transformed to create this geometry. This note records the authoring method, without claiming sole human authorship or legal clearance.

The root `icon.png` is the 512×512 raster export used by the package manifest. Render it with the existing local librsvg tool:

```sh
rsvg-convert -w 512 -h 512 assets/identity/frame-and-cue.svg -o icon.png
```

The master and this note ship with the package source. The project retains its current GPL-2.0-only declaration; see `../../LICENSE` and `../../NOTICE.md`. This asset change does not alter contributor attribution, project provenance or license terms.

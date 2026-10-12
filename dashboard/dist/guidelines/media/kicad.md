# KiCad

The mark and the signature as silkscreen footprints, for boards Christopher designs. For the rules, see the [PCB card](../applications/pcb.md) and [logo](../logo.md#by-medium).

Current version: <!-- inject:version -->3.0.0<!-- /inject -->

## Set up, once

1. In KiCad: **Preferences → Manage Footprint Libraries → add** `dist/kicad/faceplate.pretty`, nickname `faceplate`.
2. Install the fonts from `fonts/ttf/` on the system, so KiCad can set IBM Plex Mono. Without them KiCad falls back to its stroke font; the board still works, but it isn't on-brand.

The footprints are in KiCad 7 format, readable by KiCad 7 and later.

## Footprints

| Footprint | What |
|---|---|
| `Faceplate_Mark_5mm` | the mark, 5 mm wide, small geometry: heavier stroke, no setpoint line |
| `Faceplate_Mark_8mm` | the mark, 8 mm wide, with the setpoint |
| `Faceplate_Mark_12mm` | the mark, 12 mm wide, with the setpoint |
| `Faceplate_Signature_Small` | 5 mm mark + the signature line at 1 mm |
| `Faceplate_Signature_Large` | 8 mm mark + the signature line at 1.5 mm |

Sizes are the drawn width, which is what you measure on a board.

**The signature line** reads `CHRISTOPHER BIEL · ${PROJECTNAME} · REV ${REVISION} · ${ISSUE_DATE}`. KiCad fills it in from the project name and the board's title block (File → Board Setup → Title Block), so set the revision and the date there. Write the date as a year if that's all you want on the board.

**Placement:**
- usually on the back (flip the footprint; it mirrors itself)
- along a board edge and aligned to it

## Physical rules, built in

- **One colour, no opacity.** The setpoint is solid at half the curve's weight instead of translucent.
- **No line under 0.15 mm.** The build refuses to generate one. The thinnest line is the 8 mm mark's setpoint at about 0.3 mm.
- **Same geometry as the screen.** Footprints are generated from `assets/logo/`, the same simulated curve as the screen and print marks.

`dist/kicad/mark-physical.svg` is the same variant as an SVG, for CAD, laser engraving or 3D printing.

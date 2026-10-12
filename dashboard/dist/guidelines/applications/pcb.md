# PCB

Keyboards and other boards Christopher designs.

| | |
|---|---|
| **Level** | project |
| **Intensity** | — |
| **Theme** | — |
| **Medium** | physical: silkscreen, one colour |
| **Start from** | the `faceplate` KiCad library → [KiCad](../media/kicad.md) |
| **Signature** | `Faceplate_Signature_Small` or `_Large`: mark + `CHRISTOPHER BIEL · <project> · REV <x> · <date>` |

## Decided

**The full signature**, in IBM Plex Mono and uppercase, filled in from KiCad's project name and title block (`${PROJECTNAME}`, `${REVISION}`, `${ISSUE_DATE}`). Name the project with its index code, e.g. `KB-01`.

**Placement:** along a board edge and aligned to it, usually on the back.

**Physical variant of the mark:**
- one colour, no opacity: the setpoint is solid at half the curve's weight
- no line under 0.15 mm; the build refuses to generate one
- at 5 mm the mark uses the small geometry (heavier stroke, no setpoint line)

**Fonts:** KiCad 7 and later set TrueType faces; install `fonts/ttf/`.

## To decide (design session)

Whether the solid setpoint reads too heavy on a board; a front-side project glyph and name.

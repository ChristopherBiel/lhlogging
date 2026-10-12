# Details

The instrument-panel layer: small, precise marks that make a surface read as engineered. Use them as seasoning.

## Always

### Vocabulary

| Detail | Example | Use |
|---|---|---|
| Index code | `01` · `FIG. 01` · `KB-01` | numbering sections, figures, projects |
| Kicker | `PROJECTS` | mono label above a heading: a role, a kind |
| Date stamp | `06 / 2026` | dates in labels and metadata |
| Version tag | `v0.1` · `REV 10 / 2026` | documents, builds, boards, the stamp in a top bar |
| Status marker | `● OPEN FOR WORK` | a state, with a dot |
| Coordinates | `48.1°N / 11.6°E` | location, sparingly |
| Easter egg | `CB·CB·CB` | the signature grey, named; sparingly |

All set in IBM Plex Mono, uppercase, tracked out.

### Structural elements

<a id="registration-marks"></a>
**Registration marks.** Four small L-marks at the corners of an area: the system's replacement for a frame. They hold an index square, a photo, a graphic, a cell. Their weight is the level's line weight. Around a link they close into a full frame on hover, which is the one place a frame is ever drawn.

**Ruler ticks.** Every section opens with a rule across the page, and the rule carries twelve short ticks, one at the start of each column. They make the grid visible without drawing it.

**Index square.** A square in the label column holding the section's two-digit number. It is the first thing to take colour: outlined by marks at 01, tinted at 02, solid from 03, and larger with every step from 03.

**Field.** A flat area of colour behind a part of the page, running to the edge of the window. The lead has one from 02; at 04 every section may have one. → [colour](./colour.md#by-intensity)

**Graphic slot.** Every entry in a list of work has a graphic in front of it: a drawing, a plot, an image or a short looping video, always in the same landscape shape and held by registration marks. One idea per graphic, readable at thumbnail size. → [icons](./icons.md#figures)

**Accent bar.** A sage bar, the weight of the accent rule, down the left edge of a selected entry. From 02 a wash joins it.

**Plate.** A square in the signature grey that holds the mark. → [logo](./logo.md)

**Chip.** A small square tag with a keyline border, a mono label and a tint fill (sage tint, terracotta tint or signature grey). With a topical icon, it's a category tag. → [icons](./icons.md)

**Status marker.** A mono label with a dot in front: a state, in a few words.

**Spec row.** A mono key, a dotted signature-grey leader, and the value right-aligned.

## By intensity

The same details at every level; what changes is their weight and their fill.

| Level | Marks and ticks | Index square | Selected entry |
|---|---|---|---|
| **01 Line** | hairline | a number between four marks | an accent bar |
| **02 Tint** | keyline | the kind's tint, between four marks | + a wash |
| **03 Block** | keyline | the kind's solid colour, reversed number, no marks, larger | a stronger wash |
| **04 Field** | keyline; ticks only under the lead | larger again; reversed on a solid field | on a tinted field, the wash is white |

## By medium

**Web:** all of these are reference components. → [web](./media/web.md#components)

**Print and slides:** `\fpreg`, `\fpindex` and `\fptickrule` draw the marks, the index square and the rule with its ticks; the classes place them. A plot's baseline carries the same ticks, and an image in a plot is held by the same marks. → [LaTeX](./media/latex.md) · [charts](./charts.md)

**Physical:** the signature *is* the detailing. `REV`, project code and year use the same vocabulary. → [PCB card](./applications/pcb.md)

## Never

- Pile details up. They're seasoning, not the meal.
- Use a detail as decoration with nothing behind it. An index should index something; a version tag should be a real version.
- Draw a frame where four marks would do
- Put a solid block of colour on a 01 or 02 page

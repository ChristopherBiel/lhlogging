# Principles

Faceplate is TE-inspired: Swiss-grid functionalism, bold flat blocks, instrument-panel detailing, dramatic type contrast, hard square edges — tuned warm by the palette.

## The five

**1 · Square and structural.** Hard edges, visible grid, exposed keylines. `--fp-radius` is `0` and there is no rounded variant of anything. This is the single most recognisable thing about the system; a rounded corner reads as a different brand.

**2 · Bold but breathable.** Colour appears in deliberate blocks, scaled by the [Intensity Scale](./intensity.md). Not sprinkled, not everywhere — placed.

**3 · Engineered detailing.** Mono labels, index codes, occasional registration ticks. Moderate, not maximal: the detailing should read as instrumentation, not decoration.

**4 · Dramatic type contrast.** Huge Manrope ExtraBold against micro IBM Plex Mono labels. The gap between them carries most of the character.

**5 · Warmth from the palette, not the shape.** The edges stay crisp. Sage and terracotta carry the humanity. If something feels cold, the answer is colour, never a softer corner.

## Constants at every intensity

**Corners** are `0` everywhere.

**Strokes** come in two weights, plus one component dimension:

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.hairline` | `--fp-hairline` | `1px` | Subtle divider weight |
| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.keyline` | `--fp-keyline` | `2px` | Structural border weight. 2px, not 1.5px: fractional borders round inconsistently — Chrome computes 1.5px back as 1px. |

`--fp-keyline` is 2px rather than the 1.5px v1 used. 1.5px was the only fractional value in the system, and browsers round it inconsistently — Chrome computes it back as `1px`, which meant the signature keyline was the one border that could not render predictably. Two integers render crisply at every density.

**Reversed text** on sage, terracotta or ink blocks uses `--fp-text-inverse`.

**One dominant colour block** per composition at intensity 03 and above.

## Grid and spacing

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.space1` | `--fp-space-1` | `.25rem` |  |
| `semantic.space2` | `--fp-space-2` | `.5rem` |  |
| `semantic.space3` | `--fp-space-3` | `1rem` |  |
| `semantic.space4` | `--fp-space-4` | `1.5rem` |  |
| `semantic.space5` | `--fp-space-5` | `2rem` |  |
| `semantic.space6` | `--fp-space-6` | `3rem` |  |
| `semantic.space7` | `--fp-space-7` | `4rem` |  |

Base unit 8px, with a 4px half-step for tight work. Web layouts use 12 columns; expressive layouts use a 4–6 module "faceplate" grid. Align to the grid, keep margins generous, and expose keylines where structure helps the reader.

## Detailing vocabulary

Index codes (`CB-01`), date stamps (`06 / 2026`), coordinates, status markers (`● OPEN FOR WORK`), figure labels (`FIG. 01`), version tags (`v0.1`), and the `CB·CB·CB` easter egg. Use in moderation — these are seasoning.

## Data visualisation

Square framing, mono axis and legend labels, ink keylines, 2px strokes, flat fills. **No rounded corners, no gradients, no shadows.** Colours per [colour](./colour.md).

## Don't

- Round a corner
- Hardcode a hex or a font name — use the tokens
- Use `#CBCBCB` as text on white
- Mint greens
- Stretch or recolour the logo
- Use topical icons as UI controls

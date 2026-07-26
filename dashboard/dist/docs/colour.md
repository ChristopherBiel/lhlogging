# Colour

> Every value below is generated from `tokens/`. If you are reading a hex code here, it is the one the CSS ships.
>
> **Authority:** `tokens/` is the single source of truth for values. This document holds the reasoning. If they ever disagree, the tokens are right and this page has a bug — the build now refuses a doc that states a value in a table, so it should not be possible.

## The palette is closed

| | Name | Hex | Use |
|---|---|---|---|
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23FFFFFF' stroke='%231C1E21' stroke-width='1'/></svg>) | `white` | `#FFFFFF` | Page background; also inverse text on saturated fills |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23F7F8F8' stroke='%231C1E21' stroke-width='1'/></svg>) | `offWhite` | `#F7F8F8` | Panels, cards, subtle sections |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23E4E7E9' stroke='%231C1E21' stroke-width='1'/></svg>) | `hairlineGrey` | `#E4E7E9` | Rules, dividers, card borders |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%238A9097' stroke='%231C1E21' stroke-width='1'/></svg>) | `stoneGrey` | `#8A9097` | Captions, axis labels, secondary icons |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%2352585E' stroke='%231C1E21' stroke-width='1'/></svg>) | `slateGrey` | `#52585E` | Body text |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%231C1E21' stroke='%231C1E21' stroke-width='1'/></svg>) | `nearBlack` | `#1C1E21` | Headings, primary text. ~16:1 on white |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%235E7A50' stroke='%231C1E21' stroke-width='1'/></svg>) | `sage` | `#5E7A50` | Primary brand accent — rules, fills, large headings |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23455C3A' stroke='%231C1E21' stroke-width='1'/></svg>) | `sageDeep` | `#455C3A` | AA-safe sage for small text and links on white |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23DCE5D2' stroke='%231C1E21' stroke-width='1'/></svg>) | `sageTint` | `#DCE5D2` | Soft highlight backgrounds, tags, chips |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23EFF3EA' stroke='%231C1E21' stroke-width='1'/></svg>) | `sageXl` | `#EFF3EA` | Very light section wash |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23BB6240' stroke='%231C1E21' stroke-width='1'/></svg>) | `terra` | `#BB6240` | Complement — secondary accent, warm highlights |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%239E4F33' stroke='%231C1E21' stroke-width='1'/></svg>) | `terraDeep` | `#9E4F33` | AA-safe terracotta for small text on white |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23F0D9CC' stroke='%231C1E21' stroke-width='1'/></svg>) | `terraTint` | `#F0D9CC` | Soft warm highlight backgrounds |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23CBCBCB' stroke='%231C1E21' stroke-width='1'/></svg>) | `signatureGrey` | `#CBCBCB` | Reads CB·CB·CB — the monogram three times. Structural only: never text on white (1.6:1), and never white text on it. |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23D6A23C' stroke='%231C1E21' stroke-width='1'/></svg>) | `ochre` | `#D6A23C` | Data-viz series 3 |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%232C7A78' stroke='%231C1E21' stroke-width='1'/></svg>) | `teal` | `#2C7A78` | Data-viz series 4 |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%238A6A53' stroke='%231C1E21' stroke-width='1'/></svg>) | `clay` | `#8A6A53` | Data-viz series 5 |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%239BA0A4' stroke='%231C1E21' stroke-width='1'/></svg>) | `neutralGrey` | `#9BA0A4` | Data-viz series 6 (neutral slot) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23EFF3EA' stroke='%231C1E21' stroke-width='1'/></svg>) | `seq1` | `#EFF3EA` | Sequential ramp step 1 of 7 (low to high) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23D8E2CC' stroke='%231C1E21' stroke-width='1'/></svg>) | `seq2` | `#D8E2CC` | Sequential ramp step 2 of 7 (low to high) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23BFD0AC' stroke='%231C1E21' stroke-width='1'/></svg>) | `seq3` | `#BFD0AC` | Sequential ramp step 3 of 7 (low to high) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23A2BB89' stroke='%231C1E21' stroke-width='1'/></svg>) | `seq4` | `#A2BB89` | Sequential ramp step 4 of 7 (low to high) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%2382A268' stroke='%231C1E21' stroke-width='1'/></svg>) | `seq5` | `#82A268` | Sequential ramp step 5 of 7 (low to high) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%235E7A50' stroke='%231C1E21' stroke-width='1'/></svg>) | `seq6` | `#5E7A50` | Sequential ramp step 6 of 7 (low to high) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%2341562F' stroke='%231C1E21' stroke-width='1'/></svg>) | `seq7` | `#41562F` | Sequential ramp step 7 of 7 (low to high) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%239E4F33' stroke='%231C1E21' stroke-width='1'/></svg>) | `div1` | `#9E4F33` | Diverging ramp step 1 of 7 (negative to positive) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23C9876B' stroke='%231C1E21' stroke-width='1'/></svg>) | `div2` | `#C9876B` | Diverging ramp step 2 of 7 (negative to positive) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23EAD3C5' stroke='%231C1E21' stroke-width='1'/></svg>) | `div3` | `#EAD3C5` | Diverging ramp step 3 of 7 (negative to positive) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23F1F2EC' stroke='%231C1E21' stroke-width='1'/></svg>) | `div4` | `#F1F2EC` | Diverging ramp step 4 of 7 (negative to positive) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%23C3D2B0' stroke='%231C1E21' stroke-width='1'/></svg>) | `div5` | `#C3D2B0` | Diverging ramp step 5 of 7 (negative to positive) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%2388A56F' stroke='%231C1E21' stroke-width='1'/></svg>) | `div6` | `#88A56F` | Diverging ramp step 6 of 7 (negative to positive) |
| ![](data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='28' height='14'><rect width='28' height='14' fill='%2341562F' stroke='%231C1E21' stroke-width='1'/></svg>) | `div7` | `#41562F` | Diverging ramp step 7 of 7 (negative to positive) |

These are all the colours Faceplate has. Not a starting point, not a suggestion — the complete set. If your project needs a colour that isn't here, see [contributing](./contributing.md#proposing-a-token); do not add one locally.

**Why closed:** a colour added inside a project is invisible to the brand sheet, so nothing outside that project knows it exists. It also disappears the next time the package updates, taking whatever depended on it. This has happened: a project needed a fifth chart colour, added `--fp-dv-7` to its copy of the CSS, and four separate reviews called the result compliant — because each one checked the site against the modified file.

## What to reach for

**Sage is the brand accent.** Rules, fills, large headings, UI accents. It is the colour someone should remember.

**Terracotta is the complement.** A second series in a chart, a warm highlight, an alternative to sage where sage would repeat. Use it deliberately — two accents competing reads as indecision.

**The signature grey** is `#CBCBCB`, which reads as **CB·CB·CB** — the monogram three times. A deliberate hidden mark. It is **structural only**: keylines, dividers, table borders, muted panels, the data-viz neutral slot.

## Contrast rules that are actually enforced

Sage on white is about 3.5:1 and terracotta about 3.2:1 — fine for large text, UI elements and fills, **not** for small body text or links. Use the `Deep` variants there; they exist for exactly this.

The signature grey is the sharpest trap. Dark ink on `#CBCBCB` is roughly 10:1 and perfectly readable. White on it is about 1.6:1 and unreadable. Never put grey text on white, and never white text on grey.

This was a real bug: `.fp-chip--gray` set a background and let its text colour inherit, so a grey chip inside a sage band rendered white on `#CBCBCB` at 1.62:1. The chip now sets ink explicitly, at 10.30:1.

## Data visualisation

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.dv1` | `--fp-dv-1` | `#5E7A50` |  |
| `semantic.dv2` | `--fp-dv-2` | `#BB6240` |  |
| `semantic.dv3` | `--fp-dv-3` | `#D6A23C` |  |
| `semantic.dv4` | `--fp-dv-4` | `#2C7A78` |  |
| `semantic.dv5` | `--fp-dv-5` | `#8A6A53` |  |
| `semantic.dv6` | `--fp-dv-6` | `#9BA0A4` |  |

The categorical series *is* the brand palette — `dv-1` and `dv-2` are sage and terracotta, not separate values that happen to match. Use them in order.

**Reserve, don't extend.** If a chart needs colours for both categories and status, spend the palette deliberately: one project reserved terracotta for "deviation" and ochre for "swapped", leaving four hues for four categories. That is the right instinct. Running out is a signal to simplify the chart, or to propose a token — not to invent one.

## Ramps

Sequential, low to high:

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.seq1` | `--fp-seq-1` | `#EFF3EA` |  |
| `semantic.seq2` | `--fp-seq-2` | `#D8E2CC` |  |
| `semantic.seq3` | `--fp-seq-3` | `#BFD0AC` |  |
| `semantic.seq4` | `--fp-seq-4` | `#A2BB89` |  |
| `semantic.seq5` | `--fp-seq-5` | `#82A268` |  |
| `semantic.seq6` | `--fp-seq-6` | `#5E7A50` |  |
| `semantic.seq7` | `--fp-seq-7` | `#41562F` |  |

Diverging, negative to positive:

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.div1` | `--fp-div-1` | `#9E4F33` |  |
| `semantic.div2` | `--fp-div-2` | `#C9876B` |  |
| `semantic.div3` | `--fp-div-3` | `#EAD3C5` |  |
| `semantic.div4` | `--fp-div-4` | `#F1F2EC` |  |
| `semantic.div5` | `--fp-div-5` | `#C3D2B0` |  |
| `semantic.div6` | `--fp-div-6` | `#88A56F` |  |
| `semantic.div7` | `--fp-div-7` | `#41562F` |  |

Both are part of the closed set. They lived only in the brand document until v2 — real values with no token home, which meant the "single source of truth" was not actually single.

## Text on colour

Use `--fp-text-inverse` for text on any saturated or dark fill. It is white, but naming it matters: without it the system hardcoded `#fff` eight times while telling everyone else never to hardcode a hex.

## Don't

- Add a colour, in any project, for any reason
- Use `#CBCBCB` as text on white, or white text on it
- Use sage or terracotta for small text — reach for `Deep`
- Mint greens. Sage is a grey-green; mint is not in this system

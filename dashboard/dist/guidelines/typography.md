# Typography

Two typefaces, both open source (SIL OFL) and free to embed in websites, PDFs and documents.

## Always

**Manrope** carries everything readable: headings, body text, UI. It was chosen for its geometric skeleton with humanist warmth, and for its lighter texture; Sora was tried and read too heavy.

**IBM Plex Mono** is the accent, used sparingly for labels, dates, roles, metadata and code. Keep mono text short. It is an accent, not a paragraph face.

The contrast between the two is the point: large Manrope against small uppercase mono labels is one of the six [principles](./core.md).

<!-- inject:table semantic.font -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.fontSans` | `--fp-font-sans` | `"Manrope", system-ui, -apple-system, "Segoe UI", Roboto, sans-serif` |  |
| `semantic.fontMono` | `--fp-font-mono` | `"IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, monospace` |  |

<!-- /inject -->

### Weights

- **Manrope ExtraBold 800** is reserved for the wordmark and display type.
- **Bold 700 and SemiBold 600** are for headings.
- **Regular 400 and Medium 500** are for text.
- **IBM Plex Mono Medium 500**, uppercase and tracked out, is for labels.

<!-- inject:table component.heading -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `component.heading.h1Weight` | `--fp-heading-h1-weight` | `700` | Bold. ExtraBold 800 is reserved for the wordmark and display type. |
| `component.heading.h2Weight` | `--fp-heading-h2-weight` | `700` |  |
| `component.heading.h3Weight` | `--fp-heading-h3-weight` | `600` |  |
| `component.heading.h1LineHeight` | `--fp-heading-h1-line-height` | `1.15` |  |
| `component.heading.h2LineHeight` | `--fp-heading-h2-line-height` | `1.2` | Between the tight and the body leading. Specific to h2, not a scale step. |
| `component.heading.h3LineHeight` | `--fp-heading-h3-line-height` | `1.3` | As h2LineHeight — specific to h3. |
| `component.heading.h2LetterSpacing` | `--fp-heading-h2-letter-spacing` | `-.01em` |  |
| `component.heading.h3LetterSpacing` | `--fp-heading-h3-letter-spacing` | `-.01em` | Every heading tightens; h3 included. |

<!-- /inject -->

<!-- inject:table component.wordmark -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `component.wordmark.weight` | `--fp-wordmark-weight` | `800` |  |
| `component.wordmark.lineHeight` | `--fp-wordmark-line-height` | `1.05` | Just above 1, so descenders never clip and a wrapped wordmark does not collide. |

<!-- /inject -->

### The scale is closed

The web scale has seven text steps, roughly a major third apart, with body text at 16px as the reading baseline, plus three display steps (`xl`, `xxl`, `xxxl`) that continue the same ratio. **Every size the system renders is on this list.**

<!-- inject:typescale -->

| px | rem | CSS variable | Used by |
|---|---|---|---|
| 10 | `.625rem` | `--fp-text-xxs` | `.fp-chip` |
| 12 | `.75rem` | `--fp-text-xs` | `.fp-label` `.fp-kicker` `.fp-seg` `.fp-btn` |
| 14 | `.875rem` | `--fp-text-sm` | small / caption text · entry details |
| 16 | `1rem` | `--fp-text-base` | `.fp-body` |
| 20 | `1.25rem` | `--fp-text-h3` | `.fp-h3` · entry and cell titles |
| 25 | `1.5625rem` | `--fp-text-h2` | `.fp-h2` |
| 32 | `2rem` | `--fp-text-h1` | `.fp-h1` · the display line at 01 |
| 40 | `2.5rem` | `--fp-text-xl` | the display line at 02 |
| 50 | `3.125rem` | `--fp-text-xxl` | the display line at 03 |
| 64 | `4rem` | `--fp-text-xxxl` | the display line at 04 |

<!-- /inject -->

`--fp-text-sm` is the small step, for the details of an entry and for copy that should run quieter.

**The display line** is the one large line of a page: a name, a project name, a page title. There is one per page, set with `.fp-display`, and its size belongs to the [dial](#by-intensity). **The lede** is the single line under it; it may pick out a few words in colour.

### Line height and tracking

<!-- inject:table semantic.leading -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.leading` | `--fp-leading` | `1.65` |  |
| `semantic.leadingTight` | `--fp-leading-tight` | `1.15` |  |
| `semantic.leadingSnug` | `--fp-leading-snug` | `1.5` | Small text in lists: the details of an entry, the line under a cell title. |

<!-- /inject -->

Body text runs at a generous leading, because long-form reading is a real use here. H1 uses the tight leading, the display line the wordmark's, and small text in lists the snug one.

<!-- inject:table semantic.track -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.trackTight` | `--fp-track-tight` | `-.02em` | The largest heading (h1). |
| `semantic.trackDisplay` | `--fp-track-display` | `-.03em` | Display type, the steps above h1. Tracking tightens as type grows. |
| `semantic.trackHeading` | `--fp-track-heading` | `-.01em` | Sub-headings (h2, h3). Tracking tightens as type grows: heading -.01em, h1 -.02em, display -.03em. |
| `semantic.trackLabel` | `--fp-track-label` | `.12em` |  |

<!-- /inject -->

Tracking tightens as type grows, because typefaces are spaced for reading sizes and look loose when set large: headings, then h1, then display. Two values stay inside their components instead of joining the scale: the button and the chip. Both are small uppercase text, where tracking opens the letterforms up rather than closing a headline.

## By intensity

Type is one of the four things the dial moves. The display line grows a step with every level, and section titles grow once, at 02. Body text, labels and the titles of entries never change.

<!-- inject:dial type -->

| | 01 Line | 02 Tint | 03 Block | 04 Field | CSS variable |
|---|---|---|---|---|---|
| Display line | `h1 · 32px` | `xl · 40px` | `xxl · 50px` | `xxxl · 64px` | `--fp-display` |
| Display weight | `700` | `800` | `800` | `800` | `--fp-display-weight` |
| Lede | `h3 · 20px` | `h2 · 25px` | `h2 · 25px` | `h2 · 25px` | `--fp-lede` |
| Section title | `h3 · 20px` | `h2 · 25px` | `h2 · 25px` | `h2 · 25px` | `--fp-title` |
| Index number | `h3 · 20px` | `h3 · 20px` | `h2 · 25px` | `h1 · 32px` | `--fp-index-text` |

<!-- /inject -->

- **01** is quiet on purpose: the display line is the size of an h1 and only Bold. Small type and wide room are what make a 01 page feel airy.
- **From 02** the display line is ExtraBold and starts to carry the page.
- **The display line never outgrows the screen:** on a narrow screen it is capped at a share of the width.

## By medium

**Web:**
- Self-host the fonts by default. The Google Fonts CDN sends visitors' IP addresses to Google, and a Munich court held in 2022 that this needs consent. The CDN `<link>` is fine for prototypes. → [web](./media/web.md#fonts)
- If the fonts fail to load, the stacks fall back to system fonts, so layouts never break.

**Print:** the same steps, with the same ratios, on a print base of <!-- inject:pt semantic.printBase -->9.5pt<!-- /inject -->. Paper is read closer than a screen, so only the base changes, and the scale stays closed:

<!-- inject:printscale -->

| Step | Ratio | Size | Leading | LaTeX |
|---|---|---|---|---|
| xxxl | 4 | 38pt | 43.7pt | `\fpsize{xxxl}` |
| xxl | 3.125 | 29.69pt | 34.14pt | `\fpsize{xxl}` |
| xl | 2.5 | 23.75pt | 27.31pt | `\fpsize{xl}` |
| h1 | 2 | 19pt | 21.85pt | `\fpsize{h1}` |
| h2 | 1.5625 | 14.84pt | 17.81pt | `\fpsize{h2}` |
| h3 | 1.25 | 11.88pt | 15.44pt | `\fpsize{h3}` |
| base | 1 | 9.5pt | 12.83pt | `\fpsize{base}` |
| sm | 0.875 | 8.31pt | 11.22pt | `\fpsize{sm}` |
| xs | 0.75 | 7.13pt | 9.62pt | `\fpsize{xs}` |
| xxs | 0.625 | 5.94pt | 8.02pt | `\fpsize{xxs}` |

<!-- /inject -->

- Body leading is <!-- inject:value semantic.printLeading -->1.35<!-- /inject -->; headings keep their own.
- Manrope has no italic, so emphasis is SemiBold, never a faked slant.
- The dial's four sizes are steps of this scale: `\fpsize{display}`, `\fpsize{lede}`, `\fpsize{title}` and `\fpsize{index}` give whichever step the document's intensity names.
- The LaTeX package sets all of this up; compile with LuaLaTeX. → [LaTeX](./media/latex.md)
- In other tools, install both faces and embed them when exporting a PDF (OFL permits this).

**Slides:** the same steps on an 11 pt base, for a 160 mm wide 16:9 slide. Body leading is <!-- inject:value semantic.slidesLeading -->1.25<!-- /inject -->.

<!-- inject:printscale slides -->

| Step | Ratio | Size | Leading | LaTeX |
|---|---|---|---|---|
| xxxl | 4 | 44pt | 50.6pt | `\fpsize{xxxl}` |
| xxl | 3.125 | 34.38pt | 39.53pt | `\fpsize{xxl}` |
| xl | 2.5 | 27.5pt | 31.62pt | `\fpsize{xl}` |
| h1 | 2 | 22pt | 25.3pt | `\fpsize{h1}` |
| h2 | 1.5625 | 17.19pt | 20.63pt | `\fpsize{h2}` |
| h3 | 1.25 | 13.75pt | 17.88pt | `\fpsize{h3}` |
| base | 1 | 11pt | 13.75pt | `\fpsize{base}` |
| sm | 0.875 | 9.63pt | 12.03pt | `\fpsize{sm}` |
| xs | 0.75 | 8.25pt | 10.31pt | `\fpsize{xs}` |
| xxs | 0.625 | 6.88pt | 8.59pt | `\fpsize{xxs}` |

<!-- /inject -->

**Email:** assume custom fonts won't load. Use a system sans, and the typefaces only inside the logo image.

**Physical:** KiCad 7 and later can set TrueType fonts on silkscreen, so both faces can go on a board. Keep text at least ~1 mm tall (check your fab's minimum).

## Never

- Add a third typeface
- Use a size that isn't on the scale
- Set paragraphs in mono
- Use ExtraBold for headings; it belongs to the wordmark and display type
- Set a second display line on a page, or scale body text with the dial

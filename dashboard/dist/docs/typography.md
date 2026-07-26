# Typography

Two typefaces, both open-source (SIL OFL), both free to embed in websites, PDFs and documents.

**Manrope** carries everything readable — headings, body, UI. Chosen for a geometric skeleton with humanist warmth, and for its lighter texture; Sora was tried and read too heavy.

**IBM Plex Mono** is the accent, used sparingly for labels, dates, roles, metadata and code. A quiet nod to the engineering side. Keep mono text short — it is an accent, not a paragraph face.

The contrast between them is the point: huge Manrope ExtraBold against micro mono labels is one of the five [principles](./principles.md).

## The scale is closed at seven steps

| px | rem | CSS variable | Used by |
|---|---|---|---|
| 10 | `.625rem` | `--fp-text-xxs` | `.fp-chip` |
| 12 | `.75rem` | `--fp-text-xs` | `.fp-label` `.fp-kicker` `.fp-seg` `.fp-btn` |
| 14 | `.875rem` | `--fp-text-sm` | small / caption text |
| 16 | `1rem` | `--fp-text-base` | `.fp-body` |
| 20 | `1.25rem` | `--fp-text-h3` | `.fp-h3` |
| 25 | `1.5625rem` | `--fp-text-h2` | `.fp-h2` |
| 32 | `2rem` | `--fp-text-h1` | `.fp-h1` |

Roughly a major-third progression, with body at 16px as the reading baseline.

`.fp-display` and `.fp-h1` are the **same role** — one 32px step, as `02_typography.md` defined it. v1 shipped a separate 40px display size that appeared in no written spec. `.fp-display` remains as a readable alias for the largest heading.

**Every size Faceplate renders is on this list.** That was not true in v1: buttons rendered at 13px and chips at 10.4px, neither of which had a name or appeared on any scale. If someone asked "which sizes may I use?", the honest answer was "these seven, except two components that use others" — and that ambiguity is what lets a ninth size in.

`--fp-text-sm` (14px) is the small/caption step. Nothing in the reference components uses it; it is there for body copy that needs to run quieter.

## Weights

Manrope 800 for wordmark and display, 700/600 for headings, 400/500 for text. IBM Plex Mono 500 for labels, uppercase, with `--fp-track-label` tracking.

## Line height and tracking

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.leading` | `--fp-leading` | `1.65` |  |
| `semantic.leadingTight` | `--fp-leading-tight` | `1.15` |  |
| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.trackTight` | `--fp-track-tight` | `-.02em` |  |
| `semantic.trackHeading` | `--fp-track-heading` | `-.01em` | Sub-headings (h2, h3). Tracking tightens as type grows: heading -.01em, display/h1 -.02em. |
| `semantic.trackLabel` | `--fp-track-label` | `.12em` |  |

Body text runs at `--fp-leading` (1.65) — generous, because long-form reading is a real use here. Display and H1 use `--fp-leading-tight`.

Tracking tightens as type grows, because typefaces are spaced for reading sizes and look loose when set large:

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.trackTight` | `--fp-track-tight` | `-.02em` |  |
| `semantic.trackHeading` | `--fp-track-heading` | `-.01em` | Sub-headings (h2, h3). Tracking tightens as type grows: heading -.01em, display/h1 -.02em. |
| `semantic.trackLabel` | `--fp-track-label` | `.12em` |  |

`--fp-track-heading` covers h2 and h3; `--fp-track-tight` covers h1 and the wordmark. v1 omitted h3's tracking entirely, making it the only heading that did not tighten.

Two values stay component-scoped rather than joining the scale: button at `.01em` and chip at `.06em`. Both are single uses on small uppercase text, where tracking is doing a different job — opening letterforms up for legibility rather than closing a headline.

## Serving the fonts

**Self-host by default.** Both faces are OFL, so you may download the `.woff2`, serve them from your own domain, and never make an external request. `fonts.css` and [`fonts/README.md`](../fonts/README.md) cover the setup.

**This matters legally in the EU.** The Google Fonts CDN sends visitor IP addresses to Google; a Munich court held in 2022 that this requires consent. Self-hosting avoids the question entirely. The Google `<link>` is a convenience for prototypes, not a production default.

**For documents**, install Manrope and IBM Plex Mono locally and embed on PDF export — OFL permits embedding, so the file renders anywhere. For LaTeX, `faceplate.sty` currently defines colours; font setup is not yet automated.

**For email signatures**, assume custom fonts will not load and fall back to a system sans. Use the face only inside the logo image.

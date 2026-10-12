# Web

How a website or web app picks up the system. For the switches, the application cards and the design rules, see the [guidelines](../README.md).

Current version: <!-- inject:version -->3.0.0<!-- /inject -->

## Examples

`examples/index.html` shows every reference page live: one personal site at intensities 01–04, the same on a phone, a project app, and the components. Each page is built from the package, declares its switches and passes the checker; `examples/web/examples.css` holds the little a project adds of its own. Start a new page from the example nearest to it.

## Install

```bash
npm i github:ChristopherBiel/faceplate#vX.Y.Z   # pin a release tag
```

Or vendor it: copy `dist/` into the project together with `MANIFEST.sha256`, so the checker can tell when the copy has drifted.

## Import

```html
<link rel="stylesheet" href="faceplate.tokens.css">      <!-- required: the values -->
<link rel="stylesheet" href="faceplate.components.css">  <!-- optional: reference components -->
```

```css
@import "faceplate/dist/faceplate.tokens.css";
```

Add the class `fp` to a wrapper (usually `<body>`) to adopt the base type and colours. It's opt-in, so Faceplate never restyles a host project globally.

| File | |
|---|---|
| `faceplate.tokens.css` | the values; every project needs this |
| `faceplate.components.css` | reference components; optional |
| `tokens.json` | the values as data, for charting libraries and scripts |
| `latex/` | the LaTeX package, classes and fonts → [LaTeX](./latex.md) |
| `guidelines/` | these pages, matching the installed version |
| `assets/` | logo and icons |
| `MANIFEST.sha256` | checksums of every shipped file |

## Intensity

Declare it once per page, on the root, and use the same level on every page of the site:

```html
<body class="fp" data-fp-intensity="02">
```

This is a real switch. The attribute selects a set of values, the dial's tokens, and everything that reads them follows:

- **Type:** `--fp-display` `--fp-display-weight` `--fp-lede` `--fp-title` `--fp-index-text`
- **Room:** `--fp-room` `--fp-lead-top` `--fp-lead-bottom` `--fp-row` `--fp-index-size`
- **Lines:** `--fp-line` `--fp-line-color`
- **Colour:** `--fp-fill` `--fp-fill-ink` `--fp-wash` `--fp-lead-fill` `--fp-lead-ink` `--fp-lead-body` `--fp-lead-accent` `--fp-lead-em`

Without the attribute a page is at 01. The values per level are on the topic pages: [typography](../typography.md#by-intensity) · [layout](../layout.md#by-intensity) · [lines](../lines.md#by-intensity) · [colour](../colour.md#by-intensity).

**Your own components follow the dial by reading these tokens.** A custom hero sets its title in `var(--fp-display)` and closes with `var(--fp-room)`; it then steps with every level without knowing which one is set. Never branch on the level in your own CSS to change a layout: the chassis is the same at all four.

`npx faceplate check` flags:
- a value that isn't `01`, `02`, `03` or `04`
- more than one level in a file, or pages of one project at different levels
- a `.fp-band` on a page declared 01 or 02

A page that exists to show the levels side by side marks its root `data-fp-specimen`, and the checker leaves its intensities alone.

## The page chassis

Every page is the same chassis: a sheet of twelve columns, the first three a label column. → [layout](../layout.md#the-chassis)

```html
<body class="fp" data-fp-intensity="02">
<div class="fp-page"><div class="fp-page__sheet">

  <header class="fp-top">
    <a class="fp-top__id" href="/"><svg class="fp-top__mark" viewBox="0 0 64 64">…</svg></a>
    <nav class="fp-top__nav"><a href="#news"><i>01</i>News</a> …</nav>
    <span class="fp-top__note">Rev 10 / 2026</span>
  </header>

  <main>
    <section class="fp-sec fp-sec--lead">
      <div class="fp-sec__label"><figure class="fp-portrait"><img src="…" alt="…"></figure></div>
      <div class="fp-sec__body">
        <p class="fp-label">Role · Institution</p>
        <h1 class="fp-display">Name</h1>
        <p class="fp-lede">One line, with <em>a few words</em> picked out.</p>
        <p class="fp-body">A few sentences.</p>
        <ul class="fp-links"><li><a href="…">Email <span class="fp-arrow">→</span></a></li> …</ul>
      </div>
    </section>

    <section class="fp-sec fp-sec--ink fp-sec--solid" id="news">
      <header class="fp-sec__label">
        <span class="fp-index">01</span>
        <h2 class="fp-sec__title">News</h2>
        <span class="fp-sec__note">3 items</span>
      </header>
      <div class="fp-sec__body">
        <div class="fp-row"><time class="fp-row__key">10 / 2026</time><p class="fp-row__text">…</p></div>
      </div>
    </section>
  </main>

  <footer class="fp-foot"><span>© 2026 …</span><nav class="fp-foot__nav">…</nav></footer>
</div></div>
</body>
```

- **`.fp-page`** clips the fields that run past the sheet; **`.fp-page__sheet`** is the sheet. Top bar, sections and footer are its direct children, or children of a `<main>` inside it.
- **`.fp-sec`** is a section: `__label` is the label column, `__body` the body. A third child may span all twelve columns with `grid-column:1 / -1`.
- **`.fp-sec--lead`** is the lead. Its body sits on the lead field from 02.
- **The kind of a section** is a modifier: none for the work itself (sage), `--ink` for what is public and dated, `--terra` for what is made on the side. → [colour](../colour.md#kinds)
- **Its field at 04** is a second modifier: `--tint`, `--solid`, or none to stay on the paper. Below 04 it does nothing, so set it from the start.

The inline mark is `assets/logo/mark.svg`; its `currentColor` lets it turn with the field it sits on. At the project level the top bar holds the project's glyph (`.fp-top__glyph`) and name (`.fp-top__name`) instead, and the footer holds the `.fp-signature`.

**Text on a field.** Inside the chassis, colours come from the context roles (`--fp-ctx-ink`, `--fp-ctx-body`, `--fp-ctx-muted`, `--fp-ctx-rule`, `--fp-ctx-accent`, `--fp-ctx-bg`, `--fp-ctx-mark`), which a field re-points. Write your own components with them and they stay legible at 03 and 04. → [colour](../colour.md#roles)

## Components

**The components are optional.** `faceplate.components.css` is one correct implementation, not the required one. A component library can only govern what it ships; the tokens and rules govern everything. Build what you need and let the checker verify it. The chassis is the part worth taking as it is: it is what makes two of your sites look related.

**Type:** `.fp-display` `.fp-lede` `.fp-h1` `.fp-h2` `.fp-h3` `.fp-body` `.fp-mono` `.fp-label` `.fp-kicker` `.fp-wordmark`

**Chassis:** `.fp-page` · `.fp-top` · `.fp-sec` (`--lead` `--ink` `--terra` `--tint` `--solid`) · `.fp-index` · `.fp-foot`

**In a lead:** `.fp-portrait` · `.fp-links` with `.fp-arrow`

**Lists:** `.fp-row` (a key and a line) · `.fp-entry` (`--selected`) with `.fp-slot` · `.fp-acts`

**Cells:** `.fp-cells` (`--3`) · `.fp-cell` (`--stack`)

**Figures:** `.fp-fig`, and on its shapes `.fp-fig-accent` `.fp-fig-fill` `.fp-fig-ground` `.fp-fig-thin` `.fp-fig-dash` `.fp-fig-solid` → [icons](../icons.md#figures)

**Marks and closers:** `.fp-reg` · `.fp-status` · `.fp-callout` · `.fp-signature`

**Controls:** `.fp-btn` (`--solid` `--terra` `--ghost`) · `.fp-chip` (`--sage` `--terra` `--gray`) · `.fp-seg` · `.fp-link`

**Blocks:** `.fp-card` `.fp-panel` `.fp-rule` `.fp-hr` (`--dashed` `--gray`) `.fp-plate` (`--sage`) `.fp-band` (`--terra` `--ink`)

**Utilities:** `.fp-square` `.fp-c-*` `.fp-bg-*`

`examples/web/components.html` shows one of each.

**An entry:**

```html
<article class="fp-entry fp-entry--selected">
  <figure class="fp-slot"><img src="…" alt=""></figure>
  <div class="fp-entry__body">
    <h3 class="fp-entry__title"><a href="…">Title</a></h3>
    <p class="fp-entry__by">First Author, <b>Your Name</b>, Last Author</p>
    <p class="fp-entry__venue">Venue 2026 <b>Award</b></p>
    <p class="fp-entry__note">One sentence of what it shows.</p>
    <ul class="fp-acts"><li><a href="…">PDF</a></li><li><a href="…">Code</a></li></ul>
  </div>
</article>
```

- The slot takes an `<img>`, a `<video>` or an inline drawing (`<svg class="fp-fig">`), and gives each the same shape.
- `<b>` in `__by` is your own name; `<b>` in `__venue` is an award.
- `.fp-slot__play` labels a video.

**Registration marks** are built into `.fp-index`, `.fp-portrait`, `.fp-slot` and `.fp-cell`. Add `.fp-reg` to put them on anything else. Inside a link they close into a frame on hover and on keyboard focus.

**Segmented control:** `<button>`, `<a>` and `<span>` children render identically. Use whichever the semantics call for. Mark the active item with `.is-on` or `aria-selected="true"`.

```html
<div class="fp-seg">
  <button class="is-on">Grid</button>
  <button>List</button>
</div>
```

**States:**
- Hover, `:focus-visible` and `:disabled` are defined for buttons, segments, chips and links. Every hover colour comes from the palette: solid buttons darken to the deep variant, and links strengthen their underline from tint to sage.
- Focus is a keyline-weight ring, shown on `:focus-visible` only. Inside the chassis it takes the ink of the field it sits on.
- `:active` is deliberately undefined.
- Motion follows [motion](../motion.md) and stops under `prefers-reduced-motion`.

**Building your own:** use the tokens, read the dial and the context roles, keep `border-radius` at `0`, use only the two typefaces and palette colours, then run the checker.

## Fonts

Faceplate's font stacks fall back to system fonts, so layouts never break, but load the real faces:

- **Self-host (recommended).** The `.woff2` files ship in `fonts/`. Link `fonts.css` before the tokens, and copy `fonts/` along if you vendor.
- **Fontsource**, with a bundler: `npm i @fontsource-variable/manrope @fontsource/ibm-plex-mono`.
- **Google Fonts CDN**, for prototypes only. It sends visitors' IP addresses to Google, which needs consent in the EU.

## Verify

```bash
npx faceplate check
```

The checker reports:
- unknown `--fp-*` tokens
- off-palette hex values
- non-zero border radius
- other fonts
- spacing off the scale
- an intensity that doesn't exist, or more than one in a project
- icons that break the spec
- a vendored copy whose checksum no longer matches

Run it in CI. It's the only part of the system that works without anyone choosing to read anything.

## Resets

A universal reset silently defeats the components' padding:

```css
* { padding: 0 }   /* zeroes .fp-btn, .fp-chip, .fp-seg padding */
```

Faceplate ships inside `@layer`, and unlayered CSS beats layered CSS regardless of specificity. Scope the reset to the elements that need it, or exclude `[class^="fp-"]`. The checker flags this.

The same applies to any bare element rule. `a { color: inherit }` turns every `.fp-btn` and `.fp-link` the colour of its surroundings, which made a white button's text invisible in one of the examples. Style your own classes, not bare elements.

## With a coding agent

Install the Claude Code skill once per machine:

```bash
npx faceplate install-skill        # links skills/faceplate into ~/.claude/skills
```

In any repository, ask for it by name ("apply Faceplate, project level, 02"), or just ask to style something in your design. The skill:
- settles the three switches with you
- installs the right package
- starts from the nearest example
- follows the guidelines of the installed version
- runs the checker

The skill's instructions are in `skills/faceplate/SKILL.md`.

# Changelog

Releases first, then the design decisions behind them. The guidelines say what to do; this file says how it got that way.

The visual history of the reference components lives in `test/baseline.json`: the rendering gate fails on any change to how they render, so its git history is the visual changelog.

## v3.0.0 — 2026-10-12

Breaking. Guidelines in place of docs, intensity as one dial over a page chassis, and a package for every medium: web, LaTeX, plots, KiCad. The blocks below are newest first; where a later one redraws what an earlier one introduced, the later one holds.

**Upgrading from v2.1.0:**
- **Removed:** the plated treatment, that is `--fp-canvas`, `.fp-bg-canvas` and `.fp-section` with its parts. Sections are `.fp-sec` on a `.fp-page` now.
- **`data-fp-intensity` restyles the page.** It used to record intent only, so check the value each page declares. The levels are 01 Line · 02 Tint · 03 Block · 04 Field, one per artifact.
- **`.fp-display`** is the dial's display line, no longer an alias of h1.
- **Moved:** `dist/docs/` → `dist/guidelines/` (export `faceplate/guidelines/*`), and `dist/faceplate.sty` → `dist/latex/faceplate.sty`.
- **Web fonts** ship in `fonts/`; link `fonts.css`.
- Then run `npx faceplate check`.

**Intensity is one dial over a fixed chassis.** The four levels had become four page designs: two grounds, three kinds of container, and no more colour at 02 than at 01. They are now one design turned up. This is a breaking change to the web package.

- **The definition.** 01 Line · 02 Tint · 03 Block · 04 Field, and each level is the one below plus something.
  - The dial moves four things together: colour, line weight, type size and room.
  - It never moves the ground, the grid, the order of the content or body text.
  - → `guidelines/switches.md#intensity`
- **One intensity per artifact** (was: per surface). A site keeps its level on every route and an app on every screen. A deck is one artifact at 04; its title and dividers are its fields.
- **`data-fp-intensity` is a real switch.** It used to record intent only.
  - `tokens/intensity.json` holds the dial: twenty values per level, every level defining the same names.
  - The build emits them per level under `[data-fp-intensity]`, with 01 as the default, and lists them in `tokens.json`.
  - The guideline tables are generated from them (`inject:levels`, `inject:dial`).
- **The page chassis**, as reference components: a sheet of twelve columns whose first three are a label column.
  - `.fp-page` `.fp-top` `.fp-sec` `.fp-index` `.fp-foot`, the lead with `.fp-portrait` and `.fp-links`
  - lists: `.fp-row`, `.fp-entry` with its graphic `.fp-slot`, `.fp-acts`
  - `.fp-cells` and `.fp-cell`; drawings with `.fp-fig`
  - `.fp-reg` registration marks, `.fp-status`, `.fp-callout`, `.fp-signature`
  - a phone layout, and hover motion that stops under reduced motion
- **Open, not boxed.** Four registration marks stand where a frame would; they close into one on hover. Lists are divided by hairlines, not wrapped in cards.
- **The ground is white.** Removed: the plated treatment, that is `--fp-canvas`, `.fp-bg-canvas` and the section plate `.fp-section`.
- **Colour is also a code.** Sage for the work itself, ink for what is public and dated, terracotta for what is made on the side. A section's index square, drawings and 04 field take its kind's colour.
- **Context roles** (`--fp-ctx-*`): text, rules and accents that a field re-points for everything on it, so components stay legible on a solid field.
- **Type.**
  - A display step at 50px fills the gap in the scale. It takes the name `--fp-text-xxl`; the 64px step is now `--fp-text-xxxl`.
  - `.fp-display` is the dial's display line (it was an alias of h1) and `.fp-lede` the line under it.
  - New: `--fp-track-display`, `--fp-leading-snug`.
- **Room:** `--fp-space-8` and `--fp-space-9`, for what closes a section at the quiet levels.
- **Motion** has a page and tokens: two easings sampled from the mark's own step response, one duration, hover only.
- **Figures:** the drawing style at figure size, and a graphic in front of every entry. → `guidelines/icons.md#figures`
- **On paper, the same chassis and the same dial.** The LaTeX classes, the Beamer theme and the plot styles are redrawn on it.
  - **`intensity` is a real switch in LaTeX.** The build emits the dial into `faceplate.sty`, one macro per level; `\documentclass[intensity=02]{fpcv}` turns it. It used to record intent only.
  - **Paper values.** A dial token may carry a `paper` value: room and the index square sit lower on the spacing scale on a sheet than in a window. → `guidelines/layout.md#by-medium`
  - **New in the package:** `\fpindex`, `\fpreg`, `\fptickrule`, `\fpkind`, `\fpdial`, `\fpline`, `\fpdisplayfont`, the sizes `display` `lede` `title` `index`, and the context roles as `fpCtx…` colours.
  - **`fpdoc` is the chassis on A4**, and a class of its own: `\makelead`, `\fpsection`, `\fprow`, `\fpitem`. A top bar in the top margin carries the mark; the label column carries index squares and section titles; fields run to the edge of the paper.
  - **`fpcv`:** same macros as before, so an existing CV compiles unchanged. The date of an entry moved to the right end of its title line, the accent rule under the name is gone, and `\cvsection[ink]{…}` sets a kind.
  - **`fpletter`:** same macros; the footer no longer repeats the mark.
  - **Beamer theme:** the title slide is the deck's lead, dividers are fields with a reversed index square, body slides carry a solid index square beside the title. Blocks are open: registration marks, or an accent bar on a wash.
  - **Plots are open, not boxed** (pgfplots and matplotlib): the baseline is the only axis line, with ruler ticks; a hairline grid across; no frames. New: `faceplate map` and `faceplate.marks(ax)` hold an image with four registration marks.
  - `\fpspace` now reaches steps 8 and 9 and expands to a bare length, so it works inside pgf keys.
- **Checker:**
  - an intensity that isn't 01–04 is an error, since it now selects nothing
  - pages of one project at different levels are flagged
  - `data-fp-specimen` exempts a page that shows the levels side by side
- **Examples** are rebuilt on the chassis: one personal site shown at 01–04 and on a phone, a project app, the components. All content is placeholder.
- **Fixed:** the contrast figures for sage and terracotta on white were too low (they are about 4.8:1 and 4.3:1).
- **Not yet:** a page for a single paper or project, dense app screens and forms, the dark theme. On A4 at 04 only the lead is a field; the sections stay on white.


**Guidelines replace docs.** The design identity is organised around what you need when you start something, not around the CSS package.

- `docs/` became `guidelines/`:
  - **core** (what never changes) and **switches** (level, intensity, theme)
  - eight topic pages, each split into Always / By intensity / By theme / By medium / Never
  - one **application card** per kind of output: website, project app, CV, letter, slides, PCB
  - one page per medium; web for now
- **Values are filled in place.** `<!-- inject:… --> … <!-- /inject -->` regions are regenerated by the build inside the source files, so the guidelines show real values wherever they're opened. CI fails if a committed page is stale. The root README is generated the same way.
- **Shipped as** `dist/guidelines/` (was `dist/docs/`). The package export is `faceplate/guidelines/*` (was `faceplate/docs/*`).
- **Restored** the logo construction, clear space and minimum sizes, which were lost when the brand documents were folded into v2.
- **Fixed:** the README counted eight type steps (there are seven); the tracking table appeared twice; the logo clear-space rule worked out to about 2px.
- The brand brief and the verbal identity left the repo; they're in the `archive/` folder beside it. Their decisions are logged below.
- Checker messages point to the new pages, and the mixed-intensity warning uses the per-surface wording.

**LaTeX package and the CV class.**

- **`faceplate.sty`** is now generated from a template, `build/templates/faceplate.sty`. It provides:
  - every colour role
  - the fonts via fontspec (LuaLaTeX or XeLaTeX), with a warning-only fallback under pdfLaTeX
  - the closed type scale (`\fpsize`) and the spacing scale (`\fpspace`)
  - line weights, labels, kickers, the wordmark, chips
  - **the mark in TikZ**, traced from `assets/logo/mark.svg` at build time
  - options for the `intensity` and `level` switches, validated
- **Print values.**
  - New tokens: `printBase` (9.5pt) and `printLeading` (1.35).
  - The print scale is the web scale's ratios on that base. Lines and spacing convert at the CSS reference pixel (1px = 0.75pt).
  - The old `.sty` mapped px to pt 1:1, so its keyline was 2pt instead of 1.5pt.
  - Print values are also in `tokens.json` under `print`. They never become CSS variables.
- **`fpcv`**, a CV class at intensity 01:
  - a 12-column A4 grid with a 3-column label column
  - section indices and hairlines
  - paired short sections
  - a square photo
- **Manrope** ships in `latex/fonts/` (SIL OFL, the five weights in use); IBM Plex Mono comes from TeX Live.
- **`npx faceplate install-latex`** links the package, classes and fonts into TEXMFHOME.
- **Examples:** `examples/latex/` holds `cv.tex` (a template) and `specimen.tex`, compiled to committed PDFs. `npm run examples` rebuilds them.
- **Moved:** `dist/faceplate.sty` → `dist/latex/faceplate.sty`. Exports: `faceplate/latex` and `faceplate/latex/*`.

**Reference examples, display sizes, web fonts.**

- **`examples/`** holds rendered references:
  - the same page at intensities 01–04
  - your website (home at 03, a plated content page at 02)
  - a project (app screen at 02, landing page at 03)
  - the components reference, plus the LaTeX CV and specimen

  `examples/index.html` shows them side by side, live. `npm run screenshots` writes `examples/screenshots/`.
- **Every example passes the checker**, and `examples/web/examples.css` shows a project writing its own components from tokens. Its patterns are candidates for reference components once approved: signature, status marker, spec rows, tiles, tick scale, registration ticks.
- **Display sizes:** `--fp-text-xl` (2.5rem) and `--fp-text-xxl` (4rem) continue the scale's ratio, for 03–04 surfaces only. The scale is now closed at nine steps.
- **Web fonts ship in `fonts/`** (woff2, SIL OFL), so self-hosting is one `<link>`. They were previously left to each project.
- **The kicker and the section-plate index are deep sage.** Plain sage at 12px failed the small-text contrast rule. The rendering gate shows exactly those two colour changes.
- **`npm run accept`** moves the rendering baseline. The documented `npm test -- --accept` passed the flag to the checker instead, so it never worked.
- **Moved:**
  - `example.html` → `examples/web/components.html`
  - `latex/examples/` → `examples/latex/`

**Letters, slides, plots, hardware, and a skill.**

- **`fpdoc`** is the A4 base under the document classes (page, grid, rows, personal header, footer). `fpcv` now builds on it; both CVs were recompiled and are pixel-identical to before.
- **`fpletter`**: the CV's header and grid; To / Date / Re labels in the label column; the body flowing in the body column; the signature in the footer. Option `window` places the address for DIN 5008 (form B) window envelopes.
- **`beamerthemefaceplate`**:
  - the title slide and section dividers at 04 (full sage, display type)
  - body slides at 02 (kicker, title, rule; content top-aligned)
  - square bullets, mono numbers, keyline blocks
  - the mark beside the page index
- **Slide scale:** new tokens `slidesBase` (11pt) and `slidesLeading` (1.25). `faceplate.sty` takes `medium=print|slides`, and `\fpsize` reads that medium's table.
- **Plots:**
  - `faceplate-plots.sty` makes every pgfplots axis a Faceplate axis
  - `dist/python/faceplate.py` (generated) and `faceplate.mplstyle` do the same for matplotlib
  - both come from the same tokens; `examples/latex/plots.tex` compares them
- **KiCad:** `dist/kicad/faceplate.pretty` (generated from `assets/logo/`):
  - the mark at 5, 8 and 12 mm
  - two signature stamps using KiCad's title-block variables
  - `mark-physical.svg`
  - the build refuses any line under 0.15 mm
- **Claude Code skill:** `skills/faceplate/SKILL.md`; `npx faceplate install-skill` links it into `~/.claude/skills`.
- **Fonts:** all font files live in `fonts/` (woff2 for the web, `ttf/` for documents, plots and KiCad). Manrope is copied into the LaTeX bundle by the build.
- **Build safety:**
  - `dist/` is regenerated from nothing on every build
  - the build refuses iCloud conflict copies ("name 2.md") anywhere in the sources, and `.gitignore` ignores them

## v2.1.0 — 2026-07-27

Additive. The v1 sectioned look returns as an opt-in.

- **`--fp-canvas`:** a grey page mat, with the `.fp-bg-canvas` utility. It's a separate role from `--fp-border`, though both currently point to the same grey, so the page and the hairlines can be re-pointed independently.
- **`.fp-section`:** a keyline card with a header strip (sage index, title, right-aligned note) over a padded body. The section title is Bold 700, not v1's 800.
- The brand sheet builds itself in the plated style. The section plate is covered by the rendering gate.

## v2.0.1 — 2026-07-26

The first working v2 release. It replaced the premature v2.0.0 tag, and `dist/` is now committed: it is the delivered surface, and CI asserts that it reproduces byte for byte from a build.

## v2.0.0 — 2026-07-22

> **Do not use this tag.** It was cut nine commits early: it has no committed `dist/`, older tokens, and a checker without alias resolution (about 90 false positives on a real project). Use v2.0.1 or later.

v2 rebuilt the system as a token pipeline. The reasoning:

**The palette had to become closed.** A project tracked four aircraft types on a schedule. The six categorical colours had two reserved for status, and the leftover neutral failed white-text contrast at 2.64:1. So it added `--fp-dv-7: #4E6E8E` to its own copy of the CSS, with a comment and a colour-vision check. Four separate compliance reviews then passed, because each measured the project against the *modified* file. The colour existed nowhere in the brand. **The need was real; the process was wrong.** That led to:

- tokens in three tiers: palette (private, closed) → roles (public `--fp-*`) → component values
- build guards that reject raw hex values above tier 1, unknown names, duplicate variables and upward references
- `npx faceplate check` for consuming projects, with `--fp-ext-*` as a visible escape hatch
- `MANIFEST.sha256`, so an edited vendored copy is detected

**Components became optional.** In the same project, 6 Faceplate classes were used and 92 were written locally: the Gantt chart, the tables, the filter drawers, the map. A component library would have governed 6% of what it rendered. Tokens and rules became the deliverable, with the checker as the thing that scales.

**Values the docs and CSS disagreed on:**
- H1 was 700 in the written spec and shipped at 800 for months. Now 700; 800 is reserved for the wordmark and display.
- A separate 40px display size existed in no spec. Display and H1 are now one 32px step.
- Buttons rendered at 13px and chips at 10.4px, neither on the scale. Now 12px and 10px; the scale is closed at seven steps.
- H3 had no tracking, the only heading that didn't tighten. Fixed.
- The wordmark's line height of 1 could clip descenders. Now 1.05.
- The keyline was 1.5px, which Chrome computes back to 1px. It's 2px now; the accent rule went from 3px to 4px to stay twice as heavy.

**Missing pieces added:**
- `--fp-text-inverse`, because v1 hardcoded `#fff` eight times while telling everyone else never to hardcode a hex
- the sequential and diverging ramps, which had lived only in a brand document with no token
- hover, `:focus-visible` and `:disabled` states, because v1 had no focus styling at all

**Bugs fixed:**
- `.fp-chip--gray` let its text colour inherit, so inside a sage band it rendered white on `#CBCBCB` at 1.62:1. It now sets ink explicitly (10.30:1).
- The segmented control measured 26, 29 or 33px depending on the child element, and `<a>` children turned link-blue. It now renders identically for any child, and a test asserts it.
- `--fp-rule` collided with the `.fp-rule` class; it's now `--fp-rule-color`.
- `\fprule` in the LaTeX package referenced a colour that never existed; the build now checks every colour the `.sty` uses.
- A project's `* { padding: 0 }` reset zeroed every component's padding (segmented controls read "anyyesno"), because unlayered CSS beats `@layer`. The checker now flags universal resets.

**Docs stopped stating values.** Tables of values became markers filled from the tokens, and the build refuses a hand-typed value in a table.

## v1.0.0 — 2026-06-29

The first Faceplate: one CSS file, a LaTeX colour package, the brand assets and the brand sheet.

## Design decisions

The record behind the guidelines. The explorations referred to are in `archive/`, beside this repository.

**2026-06-26**
- Kicked off as a personal identity for Christopher Biel.
- Wordmark: the full name, "Christopher Biel".
- Personality: modern, warm, professional, confident, summarised as **"warm professionalism"**.

**2026-06-27**
- **Colour taste:** natural, muted earth tones and greens. Hard no to mint.
- **Palette rounds.** Rounds 1–2 put the accents on warm sand backgrounds, which read too soft. The pivot was to a **clean white base with one accent**. Round 3 picked green, and Sage won among the greens.
- **Locked:** Sage `#5E7A50` as the accent and Terracotta `#BB6240` as its complement. The data-viz palette and both ramps were built on the pair.
- **Signature grey `#CBCBCB` added:** it reads as CB·CB·CB, an easter egg kept structural.
- **Typography:** Manrope (chosen over Sora, which read too heavy) plus IBM Plex Mono for labels.
- **Design language:** Teenage Engineering-inspired, with 0px corners and moderate instrument detailing.
- **The Intensity Scale 01–04** was adopted, with 03 Bold Band as the signature level.
  (Redefined in 10 / 2026; see below.)
- **Two layers:** the portable system ("Faceplate") and the personal brand on top.

**2026-06-28**
- **Logo:** a non-initial mark, the **step response**: a real simulation, ζ = 0.5, with a delayed step and 16.3% overshoot. ζ = 1/√2 was tried first; its 4.3% overshoot was too timid.
- Logo assets were exported, and the eight topical icons were drawn.

**2026-10-06: restructure**
- **Design only.** Verbal identity (bios, tone of voice) is out of scope; the brief and the verbal identity are archived.
- **The step-response mark is the only symbol.** The "CB" letter monogram is retired; the plate holds the mark.
- **Three levels replace the two layers:** personal, project and neutral.
  - Projects keep **sage** as their accent, which ties them together, and add their own name and a glyph drawn in the icon style.
  - Christopher is credited by a **signature** that adapts to the medium.
- **Dark theme:** decided as a second switch next to intensity, so every intensity exists in light and dark. Its values are still to come.
- **Intensity is chosen per surface** (a page, a screen, a slide), not per artifact. A deck can open at 04 and continue at 02.
- **Media:** LaTeX for the CV, letters and slides; matplotlib for plots; KiCad for hardware. English only.
- **Utility icons:** Lucide, with rectangle corner radii removed.
- **Print:**
  - same ratios as the web scale on a 9.5pt base, leading 1.35
  - lines and spacing at 1px = 0.75pt
  - hairlines in the signature grey on paper
  - no italics; emphasis is SemiBold
- **Display sizes:** `xl` and `xxl`, for 03–04 surfaces only.
- **Small sage text:** kickers and indices use deep sage, everywhere.
- **Examples:** each card names a rendered example; reviewing the examples replaces reading every rule.
- **Slides:**
  - one surface per slide: title and dividers at 04, body at 02
  - an 11pt base on the same ratios
- **Letters:**
  - read as a pair with the CV
  - the envelope window is the one place function overrides the grid
- **Physical mark:**
  - one colour, solid setpoint at half weight
  - small geometry at 5 mm
  - sizes by drawn width
- **CV:**
  - 12-column grid with the label column on the left
  - the mark standing on the name's baseline
  - a square photo
  - paired short sections to stay at two pages
  - ragged right, no hyphenation

**2026-10-07 to 2026-10-11 · intensity, re-approached**
- **What was wrong.** The four levels of the reference examples read as four designs: two on white and two on a grey mat, containers that went up, down and up again, and a measured 1% of colour at both 01 and 02. Each level had been given its own device, and the devices had grown into templates.
- **The fix:** one chassis, and a dial where each level is the one below plus something.
- **Three rounds of sketches** (`explorations/intensity/`, beside this repository): ten page directions after Teenage Engineering, the Swiss grid, technical documents, motion design, wayfinding and others.
- **Chosen: "Register".** A Swiss grid with a label column, and the instrument cues back as registration marks, ruler ticks and index squares. It keeps the Teenage Engineering character without boxing everything in.
- **Decided along the way:**
  - white paper, never the grey mat: at low intensity white reads cleaner
  - rectangles around everything feel cramped and dated, so containers are open
  - no prose-first layout, no very large type, no time axis, no sidebar
  - the 04 colours come from the wayfinding sketch: a field per section, coded by kind
  - levels differ by type size and room as well as colour, after 01 and 02 proved too alike on colour alone
  - four levels stay
  - no scroll animation; hover only
- **Moved to paper (2026-10-11):** the CV, the letter, the deck and the plots, on the same chassis and dial.
  - Room on a sheet cannot be the room of a window. Converted like any other length, section room at 01 came to a third of an A4 page; the dial got paper values instead.
  - A CV's dates stand at the right end of the entry's title line. In front, as on the website's dated rows, they cost the text two of its nine columns, and a real two-page CV ran to three.
  - Half-width sections follow the phone layout: the label is a line above its body.
  - A deck's body slides do the same, because a slide is too narrow to give three columns to a label.
  - A plot keeps one axis line. Its baseline with hanging ticks is the section rule again, and an image is held by the marks that hold a photo.
- **The personal site** is a researcher's page: one text-first page, a lead that fits the first screen, dated news, entries with a graphic each, side projects last. It will probably run at 02.

# Using Faceplate

## Install

```bash
npm i github:ChristopherBiel/faceplate#v2.0.0
```

Or vendor it: copy `dist/` into your project. If you vendor, also copy `MANIFEST.sha256` so the checker can tell you when the copy has drifted.

## Import

```html
<link rel="stylesheet" href="faceplate.tokens.css">      <!-- required: the values -->
<link rel="stylesheet" href="faceplate.components.css">  <!-- optional: reference components -->
```

```css
@import "faceplate/dist/faceplate.tokens.css";
```

For documents:

```latex
\usepackage{faceplate}
```

Tokens are also available as data at `dist/tokens.json`, for charting libraries or anything that needs the values programmatically.

## What you get

| File | |
|---|---|
| `faceplate.tokens.css` | the values. Everyone needs this |
| `faceplate.components.css` | reference components. Optional — see [components](./components.md) |
| `faceplate.sty` | LaTeX colours and boxes |
| `tokens.json` | machine-readable, including the closed palette |
| `docs/` | these documents, matching the version you installed |
| `assets/` | logo and topical icons |
| `MANIFEST.sha256` | checksums for every shipped file |

## Verify

```bash
npx faceplate check
```

Reports unknown `--fp-*` tokens, off-palette hex, non-zero border radius, non-Faceplate fonts, off-scale spacing, icon conformance, and a vendored copy that no longer matches its checksum.

Run it in CI. It is the only part of this system that works without anyone choosing to read anything.

## The rules, briefly

Use the tokens; never hardcode a hex or a font name. Keep `border-radius` at `0`. Pick one [intensity](./intensity.md) per surface. Use the [six palette colours](./colour.md) and the [eight type steps](./typography.md), and nothing else.

If you need something that isn't there, read [contributing](./contributing.md) **before** adding it. Short version: don't add it locally, propose it here.

## Branded vs unbranded

Faceplate has two layers. **Layer 1** is the design system — portable, brand-agnostic, usable on anything. **Layer 2** is the personal brand — the "Christopher Biel" wordmark, the step-response mark, the role tagline, the `CB·CB·CB` monogram — and it applies **only to artifacts that represent Chris**.

On Chris's own materials, use both layers. On unrelated projects use Layer 1 alone: no wordmark, no mark, no name or tagline, and the signature grey (`--fp-gray`) is simply a neutral rather than a monogram.

## Prompts — apply Faceplate to a new repository

Paste one of these to a coding agent working in a fresh repo. Pick the branded prompt for Chris's own materials, the unbranded prompt for anything else (a product, a client site, an unrelated app). Both defer every design **value** to the tokens on purpose — do not paste hex codes or sizes into them.

### 1 · Branded — Faceplate + personal identity

> This repository is one of **Christopher Biel's own materials**, so apply Faceplate with its personal layer.
>
> **Set-up**
> - Add the package: `npm i github:ChristopherBiel/faceplate` (pin the latest release tag; see the version at the top of this doc). Or vendor its `dist/` folder together with `MANIFEST.sha256`.
> - Link `dist/faceplate.tokens.css` (required). Add `dist/faceplate.components.css` only if you want the reference components — you may also build your own.
> - **Read the shipped rules in `dist/docs/` before writing any CSS** — at least `colour.md`, `typography.md`, `principles.md`, `intensity.md`.
>
> **Design rules — non-negotiable**
> - Use `--fp-*` tokens for every colour, size, font and spacing value. **Never hardcode a hex or a font name.**
> - The palette is a **closed set** and the type scale is closed. If you need a value that isn't there, **do not add one locally** — read `dist/docs/contributing.md` and propose it upstream. (A locally-invented `--fp-dv-7` is the exact failure this system exists to prevent.)
> - `border-radius` is `0` everywhere. No rounded corners.
> - Do **not** use a universal reset like `* { padding: 0 }` — it silently zeroes Faceplate's component padding. Scope your reset, or exclude `[class^="fp-"]`.
> - Pick **one** intensity per surface and declare it on the root, e.g. `<body data-fp-intensity="03">`. 03 (Bold Band) is the signature register for a personal site.
>
> **Personal layer — include it, this is Chris's own material**
> - Wordmark **"Christopher Biel"** using `.fp-wordmark` (Manrope ExtraBold).
> - Role tagline **"Robotics & Machine Learning Engineer"** as a `.fp-label` (mono, uppercase — the compact role label reads "ROBOTICS & ML ENGINEER").
> - The step-response mark and monogram plate from `dist/assets/logo/`: `mark.svg`, `mark-on-sage.svg` (reversed, for a sage band), `mark-plate.svg`, `lockup-horizontal.svg`. Favicon: `dist/assets/logo/favicon.svg`. Follow the size and clear-space rules in `dist/docs/logo.md` — and never stretch, recolour, or reduce the mark to initials.
> - The signature grey (`--fp-gray`) is the `CB·CB·CB` monogram easter egg here — use it as the plate fill, never as text on white.
>
> When done, run `npx faceplate check` and fix everything it reports. Add it to CI.

### 2 · Unbranded — design guidelines only

> This repository is an **unrelated project** — it does **not** represent Christopher Biel. Apply Faceplate **unbranded**: Layer 1 only. No wordmark, no personal mark, no name, no tagline, and the signature grey is just a neutral (no monogram, no easter egg).
>
> **Set-up**
> - Add the package: `npm i github:ChristopherBiel/faceplate` (pin the latest release tag; see the version at the top of this doc). Or vendor its `dist/` folder together with `MANIFEST.sha256`.
> - Link `dist/faceplate.tokens.css` (required). Add `dist/faceplate.components.css` only if you want the reference components — most projects author far more of their own components than they consume, and that is expected.
> - **Read the shipped rules in `dist/docs/` before writing any CSS** — at least `colour.md`, `typography.md`, `principles.md`, `intensity.md`.
>
> **Design rules — non-negotiable**
> - Use `--fp-*` tokens for every colour, size, font and spacing value. **Never hardcode a hex or a font name.**
> - The palette is a **closed set** and the type scale is closed. If you need a value that isn't there, **do not add one locally** — read `dist/docs/contributing.md` and propose it upstream. (A locally-invented `--fp-dv-7` is the exact failure this system exists to prevent.)
> - `border-radius` is `0` everywhere. No rounded corners.
> - Do **not** use a universal reset like `* { padding: 0 }` — it silently zeroes Faceplate's component padding. Scope your reset, or exclude `[class^="fp-"]`.
> - Pick **one** intensity per surface and declare it on the root, e.g. `<body data-fp-intensity="02">`. 02 (Structured) suits most content apps; 03 for a marketing header.
>
> **Do not add the personal layer**
> - No wordmark, no step-response mark, no "Christopher Biel", no role tagline.
> - Topical icons from `dist/assets/icons/` are fine as category tags — always with a text label, never as UI controls.
>
> When done, run `npx faceplate check` and fix everything it reports. Add it to CI.

## A note on resets

A universal reset will silently defeat Faceplate's component padding:

```css
* { padding: 0 }   /* zeroes .fp-btn, .fp-chip, .fp-seg padding */
```

Faceplate ships inside `@layer`, and unlayered CSS beats layered CSS regardless of specificity. Scope your reset to the elements that need it, or exclude `[class^="fp-"]`. The checker flags this.

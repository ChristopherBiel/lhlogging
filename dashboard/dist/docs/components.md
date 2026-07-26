# Reference components

> **These are optional.** `faceplate.components.css` is one correct implementation, not the required one. You are free to ignore it entirely and build your own — the rules in these docs are what you must follow, and `npx faceplate check` is what verifies you did.

## Why optional

A design system that ships components governs only the components it shipped. In a real project audited during the v2 work, **6 Faceplate classes were used and 92 were authored locally** — the gantt chart, the tables, the filter drawers, the map. An implementation-first system would have governed 6% of what that project rendered.

So components are a convenience for the genuinely universal primitives. Tokens and rules are the actual deliverable.

## Using them

```html
<link rel="stylesheet" href="faceplate.tokens.css">      <!-- required -->
<link rel="stylesheet" href="faceplate.components.css">  <!-- optional -->
```

Add class `fp` to a wrapper to adopt base type and colours. It is opt-in so Faceplate never restyles a host project globally.

## What's in the box

**Type** — `.fp-display` `.fp-h1` `.fp-h2` `.fp-h3` `.fp-body` `.fp-mono` `.fp-label` `.fp-kicker` `.fp-wordmark`

**Structure** — `.fp-card` `.fp-panel` `.fp-rule` `.fp-hr` (`--dashed` `--gray`) `.fp-plate` (`--sage`) `.fp-band` (`--terra` `--ink`)

**Controls** — `.fp-btn` (`--solid` `--terra` `--ghost`) · `.fp-chip` (`--sage` `--terra` `--gray`) · `.fp-seg` · `.fp-link`

**Utilities** — `.fp-square` `.fp-c-*` `.fp-bg-*`

## The segmented control takes any child

```html
<div class="fp-seg">
  <button class="is-on">Grid</button>
  <button>List</button>
</div>
```

`<span>`, `<button>` and `<a>` all render **identically** — appearance, borders, weight, colour, underline and line-height are normalised. Use whichever the semantics call for: `<button>` for actions, `<a>` for navigation, `<span>` for display.

This was not true in v1, where the same control measured 26.06px, 29.06px or 33.06px depending on the tag, and `<a>` children took the browser link blue. Two pages in one project patched that by hand, separately. `test/seg-agnostic.mjs` now asserts all three stay identical.

Mark the active item with `.is-on` or `aria-selected="true"`.

## States

Hover, `:focus-visible` and `:disabled` are defined for buttons, segments, chips and links. Every hover colour is one the palette already had — solid buttons darken to `sageDeep`, terracotta to `terraDeep`, links strengthen their underline from `sageTint` to `sage`.

Focus uses a keyline-weight ink ring at `--fp-focus-ring-offset`, on `:focus-visible` only, so it appears for keyboard users and not on click. v1 had no focus styling at all.

`:active` is deliberately not defined yet.

## If you build your own

Use the tokens, keep `border-radius` at `0`, use only the two typefaces and only palette colours. Then run the checker. It does not know what a gantt chart is and does not need to — it checks the values, which is exactly what scales to the 92 components nobody shipped you.

**One thing to watch:** a universal reset like `* { padding: 0 }` will silently zero the padding of any Faceplate component. Faceplate lives in `@layer`, and unlayered CSS wins over layered CSS regardless of specificity. This has bitten a real project — segmented controls rendered as `anyyesno` with the labels run together. Scope your reset, or exclude `[class^="fp-"]` from it.

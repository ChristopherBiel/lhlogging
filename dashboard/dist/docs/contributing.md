# When Faceplate doesn't cover your case

Read this **before** adding anything locally. It is the shortest page here and the one that matters most.

## The rule

**Never add a token, colour, size or stroke inside a consuming project.** Not in a stylesheet, not in a component, and above all not in your copy of `faceplate.css` — that file is generated and checksummed, and editing it makes the system agree with itself while diverging from the brand.

## Why this rule exists

A project tracked four aircraft types on a schedule. The categorical palette has six colours, two were legitimately reserved for status, and the leftover neutral failed white-text contrast at 2.64:1. So it needed a fifth hue and there genuinely wasn't one.

What happened next: `--fp-dv-7: #4E6E8E` was added to the project's copy of `faceplate.css`, with a comment explaining the reasoning and noting it had been validated for colour-vision deficiency. Reasonable-looking. Four separate compliance reviews then passed, because each measured the project against the *modified* file. The colour existed nowhere in the brand — not in the brand sheet, not in the palette document, not in this repository — and nobody could tell.

**The need was real. The process was wrong.** That distinction is the whole point of this page.

## What to do instead

**1 · Check you actually need it.** Most gaps are a component reaching for a colour that already has a role. Look at [colour](./colour.md) and [typography](./typography.md) first.

**2 · Consider whether the design should change.** Running out of categorical colours often means the chart is carrying too much. Six series is usually a signal to split the view, not to find a seventh hue.

**3 · If you still need it, propose it.** Open an issue on this repository with:

- what you are building and what it needs
- what you tried from the existing palette and why it failed — measured, not asserted (contrast ratio, ΔE against neighbours)
- a proposed value, if you have one

A new primitive is a brand decision. It gets made here, reviewed, added to `tokens/palette.json`, and reflected in the brand sheet and every generated output at once. Then every project can use it, and every project's checker knows about it.

**4 · If you are blocked and cannot wait**, use the extension namespace:

```css
--fp-ext-my-project-steel: #4E6E8E;
```

`npx faceplate check` reports `--fp-ext-*` as **"extension — requires sign-off"** rather than failing. This exists so a gap is *visible* rather than hidden. It is a flare, not a permit: extensions are expected to become proposals or to be removed.

## What the checker enforces

The build refuses:

- a raw hex in a component token — a colour must reference a role, which must reference the closed palette
- a template reaching past the roles into the private palette
- a token name that doesn't exist
- two tokens emitting the same CSS variable
- a token depending on a higher tier

And in a consuming project, `npx faceplate check` reports unknown `--fp-*` tokens, off-palette hex, non-zero border radius, non-Faceplate fonts, and a vendored copy whose checksum no longer matches.

None of this is about mistrust. It is about the fact that when a system quietly disagrees with itself, nobody finds out for months.

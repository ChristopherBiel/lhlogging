# Guidelines

How everything that represents Christopher Biel looks — and everything he builds. Design only: shape, colour, type, marks, lines.

## The model

Every artifact is **the core** plus **three switches**, in whatever medium you are working in.

| | Settings | Decides |
|---|---|---|
| **[Core](./core.md)** | — | never changes: square edges · white ground · an open page on a grid · Manrope against IBM Plex Mono · mono labels · sage as the accent · the step-response mark |
| **[Level](./switches.md#level)** | personal · project · neutral | which mark, name and signature appear |
| **[Intensity](./switches.md#intensity)** | 01 Line · 02 Tint · 03 Block · 04 Field | how strongly the design asserts itself: colour, line weight, type size and room, moved together |
| **[Theme](./switches.md#theme)** | light · dark | which neutrals an artifact is built from |

Two artifacts at different intensities look different and still read as one family, because the levels are one design turned up: each is the one below plus something.

## Applying it

1. **Find the [application card](./applications/).** It sets the switches, names the template to start from and says where the signature goes. No card yet? Choose the switches yourself.
2. **Start from the package or template for your medium**, never from a blank file. Its defaults already carry most of these rules. → [web](./media/web.md) · [LaTeX](./media/latex.md) · [Python](./media/python.md) · [KiCad](./media/kicad.md)
3. **Use the topic pages for what the template can't decide.**
4. **Compare with the examples** in `examples/` (open `examples/index.html`). Each one shows a card's switches as a rendered page, at all four intensities; if your result differs in kind, one of the two is wrong.

## Topic pages

Every topic page has the same sections: **Always** (part of the core), **By intensity**, **By theme**, **By medium** and **Never**. Sections that don't apply are left out.

| | |
|---|---|
| [logo](./logo.md) | the mark, lockups, the plate, the signature |
| [colour](./colour.md) | the closed palette, roles, contrast |
| [typography](./typography.md) | two faces, one scale |
| [lines](./lines.md) | corners, keylines, hairlines, the accent rule |
| [layout](./layout.md) | the chassis: sheet, grid, label column, room |
| [details](./details.md) | labels, registration marks, index squares, fields, the graphic slot |
| [icons](./icons.md) | topical motifs, utility icons, project glyphs, figures |
| [charts](./charts.md) | data visualisation and diagrams |
| [motion](./motion.md) | what moves, and the two curves taken from the mark |

**[Extending](./extending.md)** covers what to do when none of this fits your case. Read it before you add anything.

No value on these pages is typed by hand. Every one is filled in from `tokens/` by `npm run build`.

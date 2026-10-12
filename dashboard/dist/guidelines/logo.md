# Logo

The step-response mark is the only symbol. It appears on personal material and, on projects, inside the signature. Never on neutral work.

## The mark

The curve is a real second-order step response, traced from a simulation rather than drawn:
- damping ratio ζ = 0.5
- a delayed unit step
- 16.3% overshoot, then a clean settle onto the setpoint

It is the overshoot and settle of a control system that is well tuned but still alive.

**Construction, on a 64 × 64 grid:**
- **Curve:** stroke 4, round caps and joins. The flat rest before the step takes the first ~16% of the width.
- **Setpoint:** a dashed line at the settled value: stroke 2, dash 3 4, 35–40% opacity.
- **Settle dot:** a filled circle at the end of the curve, r ≈ 3.2.
- **Favicon:** stroke 5, a slightly larger dot, and no setpoint line, so it stays legible at 16–32px.

## Always

- **Use the supplied files.** The curve is a true simulation. Don't redraw, smooth or "fix" it, and don't change ζ.
- **Clear space:** at least ⅛ of the mark's height on every side (one module of the 64 grid).
- **Minimum size:**
  - the mark: 16px on screen
  - a lockup: drops its role line below ~120px wide, and becomes the mark alone below ~90px
- **No initials.** "CB" in letters is not a symbol: not in a plate, not as a monogram.

## Files

All in `assets/logo/`.

| File | What | Use |
|---|---|---|
| `mark.svg` | the mark in `currentColor` | inherits the surrounding text colour; the flexible default on screen |
| `mark-sage.svg` | sage mark | on white and light surfaces |
| `mark-on-sage.svg` | white mark on a sage square | on sage fills, avatars |
| `mark-plate.svg` | ink mark on the signature-grey plate | avatars, stamps |
| `lockup-horizontal.svg` | plate + wordmark + role line | headers of personal material |
| `favicon.svg` | simplified mark | browser tab |
| `png/` | hand-made rasters: favicon 16 / 32 / 512, apple-touch 180, avatar, mark and lockup exports | wherever SVG isn't accepted |

**Wordmark:** "Christopher Biel" in Manrope ExtraBold. **Role line:** "ROBOTICS & MACHINE LEARNING ENGINEER" in IBM Plex Mono, uppercase, tracked out. Both are for the personal level only.

**The plate** is a plain square in the signature grey, `#CBCBCB`, which reads as CB·CB·CB. It holds the mark and nothing else. Usual sizes on screen are about 44, 58, 74 and 150px.

## Signature

The small element that says "made by Christopher". It sits at the edge of a surface (a footer, a slide corner, the back of a board) and never competes with the content or with a project's own name. Its content adapts to the medium:

| Medium | Signature |
|---|---|
| Web footer | mark + "Christopher Biel" |
| Slides | the mark, in a corner of body slides |
| Letters, documents | mark + name, in the footer |
| PCB, hardware | mark + `CHRISTOPHER BIEL · <PROJECT> · REV <X> · <YEAR>`, in mono |

On the web it is the `.fp-signature` component; on a board, the signature footprints. → [web](./media/web.md#components) · [KiCad](./media/kicad.md)

## By intensity

The mark is sage on paper. At 04 the top bar sits on the lead's sage field, and the mark turns to the reversed colour with it. Inline, the mark is drawn with `currentColor` and follows by itself; as a file, use `mark-on-sage.svg`.

## By theme

Dark variants come with the dark theme. Until then, on an ink fill use `mark.svg` in the inverse text colour, or the plate.

## By medium

**Web:** SVG, with `currentColor` where possible. For the browser tab, use `favicon.svg` plus `png/favicon-32.png` and `png/apple-touch-icon-180.png`.

**Print:** vector only. In LaTeX use `\fpmark`, `\fpmarkplate` and `\fpmarkonsage`: the same curve, traced from `mark.svg` at build time, and sized like the SVG (the edge of its 64-unit square). → [LaTeX](./media/latex.md) Elsewhere, the lockup SVG uses live text, so install the fonts, or outline the text before handing the file to anyone else. An outlined version is planned.

**Physical** (silkscreen, engraving, 3D printing):
- One colour, no opacity. The setpoint line's transparency can't be printed.
- Most PCB fabs print lines down to about 0.15 mm. With the setpoint line, the mark therefore needs to be at least ~6 mm wide; without it, it can go smaller.
- The physical variant ships as KiCad footprints and as `dist/kicad/mark-physical.svg`. → [KiCad](./media/kicad.md)

## Never

- Stretch, rotate, recolour outside the variants, or add effects
- Redraw the curve or change its damping
- Reduce the mark to initials, or add anything to it (racetrack, skyline, frame)
- Place the bare mark on a busy background. Use the plate or the reversed variant.

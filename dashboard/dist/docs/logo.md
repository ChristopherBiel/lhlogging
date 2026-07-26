# Logo

> **Personal layer.** The mark, the wordmark and the lockups appear only on Chris's own materials. On unrelated projects use Faceplate unbranded: no wordmark, no mark, and `#CBCBCB` is simply a neutral.

## The mark

A real second-order step response with damping ratio ζ = 0.5 — the overshoot and settle of a control system that is well tuned but still alive. It is a control engineer's signature written in the shape of the thing itself, not a picture of one.

## Files

In `assets/logo/`:

| File | Use |
|---|---|
| `mark.svg` | `currentColor` — inherits the surrounding text colour |
| `mark-sage.svg` | fixed sage |
| `mark-on-sage.svg` | reversed, for placing on a sage fill |
| `mark-plate.svg` | mark inside the square monogram plate |
| `lockup-horizontal.svg` | mark plus wordmark |
| `favicon.svg` | browser tab |
| `png/` | rasterised sizes, hand-tuned — 16, 32, 512, apple-touch 180, plus avatar and lockup exports |

The PNGs are made by hand rather than generated. A 16px favicon needs a pixel of hinting that automated rasterisation loses, and these files change approximately never.

## Rules

**Minimum 16px.** Below that the overshoot stops reading and it becomes a squiggle.

**Never reduce to initials.** "CB" in a box is not the mark. The monogram plate is a separate component.

**Never stretch or recolour.** Use the provided variants — that is what they are for. If you need it in a colour that has no variant, you need a variant, not a filter.

**Clear space:** at least the height of the plate's corner radius equivalent — which, this being Faceplate, means the stroke weight. Keep it uncrowded.

## The monogram plate

Square, `#CBCBCB` fill, ink "CB" in Manrope 800, optionally with a keyline border. Roughly 44 / 58 / 74 / 150px. Available as `.fp-plate` in the reference components.

The grey is the [`CB·CB·CB` easter egg](./colour.md) — the monogram three times.

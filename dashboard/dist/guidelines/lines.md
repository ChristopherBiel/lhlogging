# Lines

Square corners and a small set of fixed line weights. This is the single most recognisable thing about the system.

## Always

**Corners are square.** `--fp-radius` is `0` everywhere, and nothing has a rounded variant.

**Three weights, each with one job:**

<!-- inject:table semantic.keyline -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.keyline` | `--fp-keyline` | `2px` | Structural frames and solid dividers. Whole pixels only: fractional borders render inconsistently across browsers. |

<!-- /inject -->

<!-- inject:table semantic.hairline -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.hairline` | `--fp-hairline` | `1px` | Subtle divider weight |

<!-- /inject -->

<!-- inject:table component.rule -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `component.rule.height` | `--fp-rule-height` | `4px` | Twice the keyline, so the rule reads as deliberately heavier. |
| `component.rule.width` | `--fp-rule-width` | `48px` |  |

<!-- /inject -->

- **Keyline:** controls (buttons, chips, segmented controls), cards, and from intensity 02 the rules and marks that structure a page, in ink.
- **Hairline:** the dividers between entries of a list and panel borders, in the signature grey; at intensity 01 also the rules and marks that structure a page.
- **Accent rule:** the short sage bar under a heading, the bar beside a selected entry and the line under a closing link. Deliberately twice as heavy as a keyline.

**Dividers** come in three kinds: solid ink, dashed ink, and grey hairline.

**Focus rings** reuse the keyline weight, in ink, offset from the control so both stay legible.

**Round caps are allowed on strokes, not corners.** Icons and the mark use round caps and joins where a stroke has a visible end. Shapes and frames stay square.

## By intensity

The lines that structure a page (the rule that opens a section, its ruler ticks, the registration marks) step once, between 01 and 02. Everything else keeps its weight.

<!-- inject:dial lines -->

| | 01 Line | 02 Tint | 03 Block | 04 Field | CSS variable |
|---|---|---|---|---|---|
| Structural line | `hairline · 1px` | `keyline · 2px` | `keyline · 2px` | `keyline · 2px` | `--fp-line` |
| Section rule | `grey` | `ink` | `ink` | `ink` | `--fp-line-color` |

<!-- /inject -->

At 04 the fields replace the rules between sections; only the lead keeps its rule, in the reversed colour.

## By medium

**Web:** whole pixels only. A fractional border like 1.5px renders inconsistently across browsers.

**Print and slides:** the same physical weights, converted at the CSS reference pixel (1px = 0.75pt): keyline <!-- inject:pt semantic.keyline -->1.5pt<!-- /inject -->, hairline <!-- inject:pt semantic.hairline -->0.75pt<!-- /inject -->, accent rule <!-- inject:pt component.rule.width -->36pt<!-- /inject --> × <!-- inject:pt component.rule.height -->3pt<!-- /inject -->. In LaTeX: `\fpkeyline`, `\fphairline`, `\fprule`, and `\fpline` for the level's structural line. On paper, hairlines are drawn in the signature grey, because the border grey disappears in print.

**Physical:** most PCB fabs print lines down to about 0.15 mm. Anything finer disappears.

## Never

- Round a corner, even slightly
- Use a fractional pixel border
- Separate surfaces with a shadow; use a keyline or a step in surface colour
- Invent a fourth line weight
- Close a frame around content. Four registration marks hold it; the frame is for hover.

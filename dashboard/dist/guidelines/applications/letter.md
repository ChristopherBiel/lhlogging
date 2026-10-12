# Letter

| | |
|---|---|
| **Level** | personal |
| **Intensity** | 01 Line; `\documentclass[intensity=02]{fpletter}` for 02 Tint |
| **Theme** | light |
| **Medium** | A4, delivered as PDF |
| **Start from** | `\documentclass{fpletter}`, a copy of `examples/latex/letter.tex` → [LaTeX](../media/latex.md) |
| **Signature** | not needed; the top bar carries the mark and the lead the wordmark |
| **Example** | `examples/latex/letter.pdf` |

## Decided

**A pair with the CV.** Same sheet, top bar, lead and footer (all from `fpdoc`), at the same intensity as the CV it travels with.

**Lead:** the contact lines stacked in the label column, in mono; the role as a label and the name as the display line beside them.

**Label column:** after a rule with its ruler ticks, mono labels (`To`, `Date`, `Re`, `Encl.`) stand beside their content, so the left edge reads as one scale.

**Body:**
- runs in the body column and flows across pages
- paragraphs separated by a line of space, not indented
- ragged right, hyphenation allowed but discouraged

**Closing:** the closing line, room to sign by hand, then the name in Bold and the role as a label.

**Window envelopes:** `\documentclass[window]{fpletter}` moves the address to the DIN 5008 form B field (25 mm from the left, 50 mm from the top), with an optional return line from `\returnaddress`. The rule that opens the letter waits below the field. It is the one place where function wins over the grid.

## To decide (design session)

Whether the date belongs in a stamp format (`06 / 10 / 2026`) or in words.

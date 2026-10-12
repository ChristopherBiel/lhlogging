# Slides

Talks, lab meetings, conference presentations.

| | |
|---|---|
| **Level** | personal |
| **Intensity** | 04 Field, for the whole deck |
| **Theme** | light (dark once it is defined) |
| **Medium** | 16:9 (160 × 90 mm), delivered as PDF |
| **Start from** | `\usetheme{faceplate}`, a copy of `examples/latex/slides.tex` → [LaTeX](../media/latex.md) |
| **Signature** | the mark, in the corner of every body slide, beside the page index |
| **Example** | `examples/latex/slides.pdf` |

## Decided

**A deck is one artifact at 04.** Its title and divider slides are its fields; its body slides are its paper. That is the same split a page has at 04, spread over slides.

**Title slide:** the deck's lead, on a solid sage field.
- **Top bar:** the mark reversed, the date where the body starts, the institute at the right.
- A reversed rule with its twelve ruler ticks, as under the top bar of a page at 04.
- **Body column:** the title, ragged and never hyphenated, the subtitle as its lede, and the author at the foot of the slide. All share one left edge.
- **Label column:** an institution's logo (`\titlegraphic`), on a white plate held by four registration marks.

A talk title is a sentence, not a name, so it is set two steps below the display line of a page at 04.

**Section dividers:** a solid field, with the section's index square reversed (white, the number in the deep colour) and the section name under it. The top bar carries the deck's title and the page index. A divider opens every `\section` by itself.

**Kinds:** `\fpkind{ink}` or `\fpkind{terra}` before a `\section` gives its divider and its index squares that kind's colour; `\fpkind{sage}` returns to the default. → [colour](../colour.md#kinds)

**Body slides:**
- A slide is a narrow sheet, so its label is a line: the section's index square, solid, the title beside it, and the section's name pushed to the right.
- Content starts at the same height on every slide.
- Square bullets, two-digit mono numbers.
- Nothing is boxed. A **block** is a mono label over its text, held by four registration marks. An **example block** and an **alerted block** take the treatment of a selected entry: an accent bar down the left edge, on a wash, in sage or in terracotta.

**Footer:** the author as a label, then the page index (`03 / 12`) and the mark. No rule: at 04 colour does the separating.

**Sizes:** the slide scale has the same ratios as print, on an 11 pt base. → [typography](../typography.md#by-medium)

**Plots:** `faceplate-plots` with the slide sizes, or matplotlib with `faceplate.use("slides")`. → [charts](../charts.md)

## To decide (design session)

The maths font (it stays Computer Modern for now); a closing slide; a dark variant for dark rooms.

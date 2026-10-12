# CV

| | |
|---|---|
| **Level** | personal |
| **Intensity** | 01 Line; `\documentclass[intensity=02]{fpcv}` for 02 Tint |
| **Theme** | light |
| **Medium** | A4, delivered as PDF |
| **Start from** | `\documentclass{fpcv}`, a copy of `examples/latex/cv.tex` → [LaTeX](../media/latex.md) |
| **Signature** | not needed; the top bar carries the mark and the lead the wordmark |
| **Example** | `examples/latex/cv.pdf`; at all four levels in `examples/index.html` |

## Decided

**The page chassis, on paper.** The CV is the same sheet as the website: a top bar, a lead, sections with a label column, a footer. → [layout](../layout.md)

**Calm, at intensity 01:** hairlines, registration marks and whitespace. The only colour is the mark, the keyline under a link and the level pips. One step up, at 02, the lead stands on a tinted field and the index squares take a tint.

**Top bar**, in the top margin of every page: the mark at the left, "Curriculum vitae" where the body starts, a date stamp at the right.

**Lead:**
- **Label column:** the photo, square, held by four registration marks. Crop it square; it keeps its colours.
- **Body:** the role as a label, the name as the display line, the profile, then the contact lines as a mono row. What links somewhere stands on an accent keyline.
- **Without a photo** the contact lines stack in the label column instead.

**Sections:**
- A rule across the page with its twelve ruler ticks opens every section.
- The **label column** (3 columns) holds the index square and the title; the **body** (9 columns) holds the entries.
- A section's kind sets the colour of its square: sage by default, `\cvsection[ink]{…}` for what is public and dated (talks, awards), `\cvsection[terra]{…}` for what is made on the side.

**Entries:**
- **The first line** is the title in Bold ink, a muted `·`, then the organisation in ink.
- **The date** stands at the right end of that line, in mono, so the dates of a section read down one edge.
- **Details** follow in the body colour, one line per fact, across all nine columns.
- Entries are parted by hairlines.

The date sits at the right, not in front as on the website's dated rows, because on paper the text needs the full width of the body. With the date in front, a two-page CV became three.

**Short sections** sit side by side, 6 columns each, to keep the CV at two pages. A half is a narrow sheet, so its label becomes a line above its body: square, then title.

**Levels** (languages, skills): five square pips, sage on signature grey.

**Footer:** a rule, then the name at the left, "Curriculum vitae" where the body starts and the page index (`01 / 02`) at the right. All mono labels.

**Text** is set ragged right, without hyphenation, because a CV is scanned, not read through.

## To decide (design session)

Whether a CV with many short sections should merge them; a list of publications with a graphic per entry, as on the website.

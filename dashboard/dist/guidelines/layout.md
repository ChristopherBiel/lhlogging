# Layout

One sheet, a twelve-column grid, a label column, and a lot of white. Structure comes from the grid and from rules; content sits on the paper and is never boxed in.

## Always

### Spacing

The base unit is 8px, with a 4px half-step for tight work. Spacing is a closed scale:

<!-- inject:table semantic.space -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.space1` | `--fp-space-1` | `.25rem` |  |
| `semantic.space2` | `--fp-space-2` | `.5rem` |  |
| `semantic.space3` | `--fp-space-3` | `1rem` |  |
| `semantic.space4` | `--fp-space-4` | `1.5rem` |  |
| `semantic.space5` | `--fp-space-5` | `2rem` |  |
| `semantic.space6` | `--fp-space-6` | `3rem` |  |
| `semantic.space7` | `--fp-space-7` | `4rem` |  |
| `semantic.space8` | `--fp-space-8` | `6rem` | Section room: what closes a section at intensity 02. |
| `semantic.space9` | `--fp-space-9` | `8rem` | Section room: what closes a section at intensity 01, the airiest level. |

<!-- /inject -->

Values off the scale add up to layouts that no longer align.

The two largest steps are section room: they are what closes a section at the quiet levels. Don't use them inside a component.

### The chassis

Every page is the same chassis. It never changes with [intensity](./switches.md#intensity): the order, the columns and the parts stay where they are.

- **The sheet** is centred, with generous side margins, and stops growing at a fixed width. The ground around it is the same white.
- **Twelve columns.** The first three are the **label column**; the other nine are the **body**.
- **The label column** holds what names a section: an index square, the title, a short note, a control that applies to the whole section. On the lead it holds a square photo or drawing. Its left edge reads as one scale down the page.
- **The body** holds the content. Lists inside it use the nine columns again: two for a date or a graphic, seven for the text.
- **A section** is one row of that grid. It opens with a rule across the full width, with a ruler tick at the start of every column, and closes with room.
- **The order of a page:** a top bar (identity, the sections, a stamp), the lead, the sections, a footer.

<!-- inject:table component.page -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `component.page.maxWidth` | `--fp-page-max-width` | `80rem` | The sheet stops growing here; the margins take the rest. |
| `component.page.margin` | `--fp-page-margin` | `4rem` | Side margins. |
| `component.page.marginNarrow` | `--fp-page-margin-narrow` | `1.5rem` | Side margins on a phone. |
| `component.page.gutter` | `--fp-page-gutter` | `1.5rem` | Between the twelve columns. |
| `component.page.measure` | `--fp-page-measure` | `64ch` | The longest line of running text. |

<!-- /inject -->

### Open, not boxed

A box around every item makes a page feel cramped and dated. So:

- **Containers are open.** Where a frame would stand, there are four [registration marks](./details.md#registration-marks). The marks close into a frame on hover, when the thing is a link.
- **Items in a list** are separated by a hairline, not wrapped in cards.
- **Colour comes as fields** that run to the edge of the window, never as tinted boxes around single items. The one exception is the wash behind a selected entry.
- **A card or a panel** is still right for a control surface: a form, a dialog, a tool with its own edge. It is the exception on a page, not its building block.

### The first screen

The lead has to answer who or what this is and how to go further, without scrolling: an identifying line, the display line, one line of what it is about, a few sentences, the links. If it doesn't fit, shorten the text; don't shrink the type.

## By intensity

Room shrinks as the level rises, because colour takes over the job of separating sections. Nothing else about the layout changes.

<!-- inject:dial room -->

| | 01 Line | 02 Tint | 03 Block | 04 Field | CSS variable |
|---|---|---|---|---|---|
| Section room | `step 9 · 128px` | `step 8 · 96px` | `step 7 · 64px` | `step 6 · 48px` | `--fp-room` |
| Above the lead | `step 7 · 64px` | `step 6 · 48px` | `step 6 · 48px` | `step 6 · 48px` | `--fp-lead-top` |
| Below the lead | `step 8 · 96px` | `step 7 · 64px` | `step 7 · 64px` | `step 6 · 48px` | `--fp-lead-bottom` |
| Entry padding | `step 5 · 32px` | `step 4 · 24px` | `step 4 · 24px` | `step 4 · 24px` | `--fp-row` |
| Index square | `step 6 · 48px` | `step 6 · 48px` | `56px` | `step 7 · 64px` | `--fp-index-size` |

<!-- /inject -->

- **01:** whitespace does the separating. Sections are far apart.
- **02–03:** ink rules and the lead field carry structure; sections move closer.
- **04:** the fields touch. Section room becomes the padding inside a field.

## By medium

**Web:** the chassis above, from `faceplate.components.css`. → [web](./media/web.md#the-page-chassis)

**Phone** (below 760px): one column.
- The label column becomes a line above its body: index, title, and the note pushed to the right.
- The side margins drop to step 4, and section room to step 7 at 01 and step 6 above it.
- A list keeps a narrow first column for its dates and graphics.
- On the lead, the links move above the text, so they stay on the first screen.
- Cells stack.

**A4:** the same chassis, from the `fpdoc` class. → [LaTeX](./media/latex.md)
- Spacing converts at the CSS reference pixel, so the scale runs from <!-- inject:pt semantic.space1 -->3pt<!-- /inject --> at step 1 to <!-- inject:pt semantic.space7 -->48pt<!-- /inject --> at step 7.
- **Margins:** step 7 all round. The top bar stands inside the top margin and the footer inside the bottom one, so the text area starts and ends on a rule.
- **Grid:** 12 columns with step-3 gutters. The label column takes 3 and the body 9; a row inside the body gives its first 2 columns to a key.
- **Fields** run to the edge of the paper, as they run to the edge of the window.
- **A half sheet** (6 columns) is narrow, like a phone: the label becomes a line above its body. Short sections may sit side by side this way. → [CV card](./applications/cv.md)
- **Room is tighter than on a screen.** A sheet holds far less than a window, so the dial's room and its index square sit lower on the spacing scale:

<!-- inject:dial room paper -->

| | 01 Line | 02 Tint | 03 Block | 04 Field | LaTeX |
|---|---|---|---|---|---|
| Section room | `24pt` | `18pt` | `18pt` | `12pt` | `\fpdial{room}` |
| Above the lead | `18pt` | `18pt` | `18pt` | `18pt` | `\fpdial{leadTop}` |
| Below the lead | `18pt` | `18pt` | `18pt` | `18pt` | `\fpdial{leadBottom}` |
| Entry padding | `3pt` | `3pt` | `3pt` | `3pt` | `\fpdial{row}` |
| Index square | `24pt` | `24pt` | `30pt` | `36pt` | `\fpdial{indexSize}` |

<!-- /inject -->

**Slides:**
- Side margins are step 5, and the same 12 columns run between them.
- A body slide is a narrow sheet: its label is a line (index square, title, the section's name at the right), and content starts under it at the same height on every slide.
- Title and divider slides fill the slide edge to edge. They are the deck's fields. → [slides](./applications/slides.md)

**Physical:** align the signature to a board edge. See the [PCB card](./applications/pcb.md).

## Never

- Use spacing off the scale
- Reorder or re-column a page to make a level look different
- Wrap list items in cards
- Put a page on a grey or tinted ground
- Crowd the margins to fit more in. Cut content instead.

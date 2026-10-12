# Switches

Three settings decide how one artifact differs from another. Choose them once per artifact; the [application cards](./applications/) give the usual choice.

## Level

Who the artifact represents. It decides which identity elements appear, never the design language.

| Level | For | Identity elements |
|---|---|---|
| **Personal** | material that represents Christopher: website, CV, letters, slides | the mark, the "Christopher Biel" wordmark, the role line |
| **Project** | things he builds: lhlogging, trivia, keyboards, PCBs | the project's own name and glyph; the mark only inside the small [signature](./logo.md#signature) |
| **Neutral** | anything that isn't his | none |

**Personal** uses the full identity. → [logo](./logo.md)

**Project** keeps the whole design language, including sage as the accent; shared sage is what ties the projects together. It adds two things of its own:
- a **name**, set like a wordmark
- a **glyph**, drawn in the [icon](./icons.md#project-glyphs) style

It credits Christopher with the signature at the edge of the surface, never in its header.

**Neutral** is the design language alone: no mark, no name, no signature. Here `#CBCBCB` is simply a neutral grey, not the CB·CB·CB easter egg.

## Intensity

How strongly the design asserts itself. It is one dial with four levels, and each level is the one below plus something.

<!-- inject:levels -->

| Level | Name | Adds |
|---|---|---|
| **01** | Line | hairlines, registration marks and one accent line; colour is a line and a point |
| **02** | Tint | + ink keylines, a tinted lead field, tinted index squares, type one step larger |
| **03** | Block | + a solid lead field and solid index squares |
| **04** | Field | + a field for every section |

<!-- /inject -->

`examples/index.html` shows one page at all four levels, side by side, and one CV on paper at the same four.

### What the dial moves

Four things together, so two levels never differ by colour alone.

| | From 01 to 04 | Values |
|---|---|---|
| **Colour** | a line and a point, then a tint, then a solid block, then a field for every section | [colour](./colour.md#by-intensity) |
| **Lines** | hairlines at 01, ink keylines from 02 | [lines](./lines.md#by-intensity) |
| **Type** | the display line grows a step with every level; section titles grow once, at 02 | [typography](./typography.md#by-intensity) |
| **Room** | most at 01 and least at 04, because colour takes over the job of separating | [layout](./layout.md#by-intensity) |

### What it never moves

- **The ground.** It is white at every level. A level adds fields on the paper; it never swaps the paper.
- **The chassis.** Same grid, same order of content, same parts in the same places. A page at 04 has the columns and sections of the page at 01.
- **Body text.** It keeps its size and leading.

If turning the dial rearranges something, it wasn't intensity that changed. This is the test for a new component: build it once, then check it at all four levels.

### How much colour that is

Measured on the reference page, `examples/web/site.html`, as the share of the whole page:

| Level | Sage and terracotta | Tints | Ink and grey fields |
|---|---|---|---|
| **01** | about 1% | none | none |
| **02** | about 1% | about 15% | none |
| **03** | about 10% | under 10% | none |
| **04** | about 20% | about 35% | about 20% |

Use the numbers to check a page, not to design one. A page that lands far from them has drifted.

### One intensity per artifact

An artifact is whatever a person meets as one thing: a website with all its routes, an app with all its screens, a document, a deck.

- **Set the level once and keep it.** A site whose home page is louder than its other pages reads as two sites.
- **A deck is one artifact too.** Its title and divider slides are its fields and its body slides are its paper, which is exactly level 04. → [slides](./applications/slides.md)
- **Never mix levels inside an artifact.** If one part needs more presence, turn the whole thing up.

### Choosing

- **01 Line:** formal and read closely. CVs, letters, long reading, a site that should stand back behind its content.
- **02 Tint ★:** the default for sites and apps. The structure shows and there is a splash of colour, but it is still paper and ink.
- **03 Block:** represents you and is seen briefly. A cover, a one-page site, a landing page.
- **04 Field:** seen across a room. Slides, posters.
- When in doubt, 02.

### Declaring

- **Web:** `data-fp-intensity="02"` on the page root. It is a real switch: the attribute selects the level's values, and the components follow. → [web](./media/web.md#intensity)
- **LaTeX:** the `intensity` option of a class or of the package. It is a real switch there too: it selects the same dial, with room at its paper values. → [LaTeX](./media/latex.md#intensity)

## Theme

Light or dark: which neutrals an artifact is built from. Every intensity exists in both themes.

- **Light** is defined and is the default.
- **Dark** is decided but not yet defined. When it arrives, sage stays the accent and the core stays the same.

Like intensity, theme is chosen once per artifact. It is wholly light or wholly dark, never a mix of sections.

Until dark is defined, build light.

# Personal website

Research, papers, projects, talks and contact, all under Christopher's name. A researcher's page: one text-first page that is easy to keep current.

| | |
|---|---|
| **Level** | personal |
| **Intensity** | 02 Tint. 01 Line is the quieter alternative. One level for the whole site. |
| **Theme** | light (dark once it is defined) |
| **Medium** | web |
| **Start from** | `examples/web/site.html`, on `faceplate.tokens.css` and `faceplate.components.css` → [web](../media/web.md) |
| **Signature** | not needed; the page carries the name |
| **Example** | `examples/web/site.html`, shown at 01–04 in `examples/index.html` |

## Decided

**One page on the [chassis](../layout.md#the-chassis).** Further pages (a paper, a project) are the same chassis at the same level.

**Top bar:** the mark alone at the left, the sections with their index numbers, a revision stamp at the right. The name is not repeated here; it is the display line.

**The lead** fits the first screen and answers who this is:
- a square photo in the label column, held by registration marks
- a label with the role and the institution
- the name as the display line
- one line of what the work is about, with a few words picked out
- a few sentences in the first person
- a row of links

**Sections, in this order,** each with an index square and a [kind](../colour.md#kinds):

| Section | Kind | Field at 04 | Holds |
|---|---|---|---|
| News | ink | solid | dated rows, newest first, kept short |
| Publications | sage | tint | entries |
| Projects | sage | none | cells, two across |
| Talks | ink | tint | dated rows with their links |
| Side projects | terracotta | tint | cells, three across |
| Contact | terracotta | solid | one large link and a status line |

**An entry** (a paper, a thesis) is the part the site lives on:
- a graphic in front of **every** entry: a looping video, an image, a plot or a drawing → [figures](../icons.md#figures)
- the title, the authors with your own name in bold, the venue as a label, at most one sentence of takeaway, then the links
- selected entries carry the accent bar and stand first

**Side projects** come last before the contact, coded terracotta so they read as separate from the research. The section exists only once there is something to show.

**Footer:** copyright, a last-updated stamp, the legal links.

**Motion:** hover only. → [motion](../motion.md)

**Favicon:** `favicon.svg`, plus the PNG fallbacks.

## To decide

- The page for a single paper or project.
- Whether the site ends up at 02 or 01. The markup is the same; it is one attribute.
- A dark variant, once the dark theme exists.

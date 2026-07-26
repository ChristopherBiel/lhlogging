# Intensity

The Intensity Scale is how Faceplate stays usable on a CV and a poster without becoming two systems. **Pick one level per surface and stay there.**

| Level | Name | Colour coverage | Use for |
|---|---|---|---|
| **04** | Full Bold | ~60–75% | Posters, covers, social graphics, slide title pages, packaging |
| **03** | Bold Band ★ | ~25–35% | **The signature.** Website headers and heroes, business cards, project covers, profile blocks |
| **02** | Structured | ~5–10% | Proposals, reports, slide bodies, web content sections |
| **01** | Minimal | ~2–4% | CVs, cover letters, official documents, long-form reading |

All four keep the same DNA — square plate, Manrope ExtraBold, mono labels, sage rule. **Only the colour volume changes.**

## Never mix levels in one artifact

A page with a 04 hero and 01 content doesn't read as range, it reads as two designs stapled together. If a section needs more presence, raise the whole surface.

## Declaring it

Declare it **once per artifact**, on the root — a page, a document, a slide deck. Not per section:

```html
<body data-fp-intensity="03">
  …
</body>
```

A web page at 03 has a bold band header *and* quieter content below it. That mix is what ~25–35% coverage means; the header is not separately "03" while the content is "02". Declaring per-section is the most common way people accidentally mix levels.

This is a declaration, not a switch — it does not restyle anything. It states what you intended so that `npx faceplate check` can tell you when the page drifted away from it, and so the next person knows which register they are working in.

**Why declared rather than automatic:** colour coverage is a property of a whole composition, not of any one element. No stylesheet can decide whether a band belongs on this page. What a tool *can* do is compare what you declared against what you built.

## What the checker looks at

- `.fp-band` on a surface declared 01 or 02 — a full-bleed colour block is a 03+ device
- measured colour coverage far from the declared band
- more than one dominant colour block at 03+
- mixed `data-fp-intensity` values inside one artifact

## Choosing

Ask what the piece is *for*. Formal and read closely → 01. Informative, structured, skimmed → 02. Representing you, seen briefly → 03. Seen across a room → 04.

When in doubt, 03. It is the signature register and the one the system was tuned around.

# Project app

A web app or site for something Christopher builds, such as lhlogging.biels.net or trivia.biels.net.

| | |
|---|---|
| **Level** | project |
| **Intensity** | 02 Tint, on every screen. One level per app, landing page included. |
| **Theme** | per project: light, or dark once it is defined |
| **Medium** | web |
| **Start from** | `examples/web/app.html`, on `faceplate.tokens.css` and `faceplate.components.css` → [web](../media/web.md) |
| **Signature** | footer: mark + "Christopher Biel", small, in mono (`.fp-signature`) |
| **Example** | `examples/web/app.html` |

## Decided

- **The same [chassis](../layout.md#the-chassis) as the personal site,** so every project looks related to it and to the others.
- **Top bar:** the project's **own name**, set like a wordmark, and its **own glyph**, drawn in the [icon](../icons.md#project-glyphs) style. Never Christopher's wordmark or mark.
- **The lead** says where you are and how things stand: the screen's title as the display line, and the few figures that matter.
- **The label column** holds a section's title and the controls that apply to all of it, such as a range switch.
- **Accent:** sage, as on every project. Projects are told apart by name and glyph, not by colour.
- **[Kinds](../colour.md#kinds)** apply as on the site: a log or a history is ink.
- **Charts:** the categorical series in order. → [charts](../charts.md)
- **Tables:** a keyline under the head, hairlines between rows, numbers in mono and right-aligned.
- **Cards** are for control surfaces (a form, a dialog), not for laying out a screen.
- **Checker** in CI: `npx faceplate check --strict`.

## To decide

The example is a first drawing of an app on the chassis and has not been reviewed against a real one: dense screens, forms, navigation deeper than one level, and empty and error states are still open.

## Current state

| Project | State |
|---|---|
| lhlogging | on Faceplate (a vendored v2.0.0 build), light. Needs: the move to the chassis, one intensity for all routes, the footer signature, the plane redrawn as a glyph. |
| trivia | not on Faceplate: dark, Inter, purple, rounded corners. It moves over once the dark theme exists. |

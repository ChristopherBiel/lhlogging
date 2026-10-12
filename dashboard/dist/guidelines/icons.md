# Icons

One drawing style, four jobs: utility icons do things, topical motifs label subjects, project glyphs name projects, and figures show what a piece of work is.

## Always

**The style:** geometric line drawings on a 24px grid, a 2px stroke, round caps and joins where strokes have visible ends, and `currentColor` so they take the surrounding text colour. Shapes and frames stay square.

### Utility icons

Universal meanings: menu, download, external link, close. The *style* is what's branded, not the subject.

Use **[Lucide](https://lucide.dev)**. It's drawn to the same spec (24 grid, 2px stroke, round caps and joins, `currentColor`), so it sits next to the topical set without adjustment. One exception: Lucide rounds the corners of some rectangles (`rx="2"`). Remove the `rx` attribute so frames stay square. Don't mix in a second utility set.

### Topical motifs

Eight domain icons in `assets/icons/`:
- robotics (robot arm)
- machine learning (node network)
- control (feedback loop with a state node)
- drones and eVTOL (quadrotor)
- hardware and PCB (chip)
- keyboards (keycap)
- 3D printing (isometric cube)
- smart home (house with signal)

**Use them for category tags and section markers only, never as UI controls.** A topical motif is a label about subject matter; using one as a button confuses the two jobs. **Always pair one with a text label.**

```html
<span class="fp-chip fp-chip--sage">
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">…</svg>
  CONTROL
</span>
```

### Project glyphs

A project's own symbol, such as lhlogging's plane or trivia's question mark. It is drawn to exactly the same spec, in one colour, so it reads as part of the family. It stands next to the project's name and plays the role the mark plays on personal material.

<a id="figures"></a>
### Figures

A figure is the graphic of an entry or a cell: the same drawing style at figure size. It shows the one idea of a paper or a project, as a sign for it and not as a figure taken from it.

- **Format:** a landscape field of 240 × 180 units, the shape of every [graphic slot](./details.md). The drawing keeps about a sixth of the field free on every side, so figures of different shapes share one optical centre.
- **Strokes:** the keyline weight, round caps and joins, and strokes that do not scale with the drawing (`vector-effect="non-scaling-stroke"`), so a figure has the same line weight at any size.
- **One accent part.** Its stroke is the accent colour and its fill steps with intensity: none at 01, the tint at 02, solid from 03. Everything else is ink.
- **Secondary parts** (a start state, a baseline, a reference) are hairlines; what is planned or expected is dashed.
- **It must read small.** A figure is shown at roughly 170px wide on a desktop and 90px on a phone. Axis labels and legends do not survive that; leave them out.

Other graphics are welcome in the same slot: a plot made with the Faceplate plot styles, a photo, a still, or a short, silent, looping video. Photos and video keep their own colours and sit inside the registration marks. → [charts](./charts.md)

### Adding an icon

Match the family exactly: `viewBox="0 0 24 24"`, `stroke-width="2"`, `stroke="currentColor"`, round caps and joins. `npx faceplate check` validates every SVG under an `icons/` folder against this. An icon drawn on a 32 grid, or with a baked-in hex colour, fails rather than quietly pulling the set apart.

## By medium

**Web:** inline SVG or `<img>`. Inline is better, because `currentColor` only works inline.

**Print:** the same SVGs, converted to PDF if your tool needs it.

## Never

- Use a topical motif as a button or control
- Show a topical motif without a label
- Use filled, duotone or multicolour icons
- Mix utility-icon sets
- Give a figure a second accent, or text that has to be read

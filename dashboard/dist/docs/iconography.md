# Iconography

Two tiers, one style, different jobs.

**Shared style:** geometric, 2px stroke, round caps and joins, drawn on a 24px grid, `currentColor` so they inherit the surrounding text colour.

## Utility icons

Universal meanings — menu, download, external link, close. They stay neutral and legible. **The style is what's branded, not the subject.** Use them anywhere a function needs an icon.

## Topical motifs

In `assets/icons/`. Eight domains, mapping to the work: robotics (robot arm) · machine learning (node network) · control (feedback loop with state node) · drones and eVTOL (quadrotor) · hardware and PCB (chip) · keyboards (keycap) · 3D printing (isometric cube) · smart home (house with signal).

**These are for category tags and section markers only — never as UI controls.** A topical motif is a label about subject matter; using one as a button confuses the two jobs.

**Always pair with a text label.** This is where personality lives, and personality without a label is a guessing game.

```html
<span class="fp-chip fp-chip--sage">
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">…</svg>
  CONTROL
</span>
```

## Contributing an icon

Match the family exactly: `viewBox="0 0 24 24"`, `stroke-width="2"`, `stroke="currentColor"`, round caps and joins where strokes have visible ends.

`npx faceplate check` parses every SVG in `assets/` and asserts this. An icon drawn on a 32 grid, or with a baked-in `#5E7A50` instead of `currentColor`, fails rather than shipping and quietly pulling the set apart. All eight current icons pass.

## The logo

See [logo](./logo.md). Short version: it is the personal layer, it appears only on Chris's own materials, minimum 16px, and it is never stretched, recoloured or reduced to initials.

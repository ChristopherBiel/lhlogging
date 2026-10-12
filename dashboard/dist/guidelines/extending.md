# Extending

What to do when the system doesn't cover your case. Read this **before** adding anything locally.

## The rule

**Never add a token, colour, size or line weight inside a project.** Not in a stylesheet, not in a component, and above all not in your copy of the generated files. That copy is checksummed, and editing it makes the project agree with itself while drifting away from the brand. Every later check then passes, because it measures against the edited file.

This has happened: a project added a seventh chart colour to its copy of the CSS, and four separate reviews called the result compliant. The need was real; the process was wrong. The details are in the [changelog](../CHANGELOG.md#v200--2026-07-22).

## What to do instead

**1 · Check that you really need it.** Most gaps are a component reaching for a colour that already has a role. Look at [colour](./colour.md) and [typography](./typography.md) first.

**2 · Consider changing the design.** Running out of chart colours usually means the chart carries too much; split the view instead of finding a seventh hue.

**3 · Propose it.** Open an issue on this repository with:
- what you're building and what it needs
- what you tried from the existing system and why it failed, measured rather than asserted (contrast ratio, ΔE against neighbours)
- a proposed value, if you have one

A new value is a brand decision. It is made here, added to `tokens/`, and shows up in the guidelines and in every generated output at once. Then every project can use it, and every project's checker knows about it.

**4 · If you're blocked and can't wait,** use the extension namespace:

```css
--fp-ext-my-project-steel: #4E6E8E;
```

`npx faceplate check` reports `--fp-ext-*` as "extension — requires sign-off" instead of failing. This exists so a gap stays *visible* rather than hidden. It's a flare, not a permit: an extension should become a proposal or be removed.

## What the tools enforce

The build refuses:
- a raw hex in a component token: colours must reference a role, and roles must reference the closed palette
- a template reaching past the roles into the private palette
- a token name that doesn't exist
- two tokens emitting the same CSS variable
- a token that depends on a higher tier
- a guideline that types a value into a table instead of injecting it

In a project, `npx faceplate check` reports:
- unknown `--fp-*` tokens
- off-palette hex values
- non-zero border radius
- other fonts
- spacing off the scale
- icons that break the spec
- a vendored copy whose checksum no longer matches

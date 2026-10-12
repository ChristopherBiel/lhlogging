# Motion

Things answer the pointer and settle. Nothing moves by itself.

## Always

**Motion is a response.** An element moves because someone touched it: a hover, a focus, a press. A page never animates in, nothing plays on scroll, and nothing loops except a video someone chose to show.

**The curves come from the mark.** The mark is the step response of a second-order system at ζ = 0.5. Motion uses the same system, sampled into two easings:

<!-- inject:table semantic.ease -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.easeResponse` | `--fp-ease-response` | `linear(0, .056, .196, .379, .573, .754, .907, 1.023, 1.103, 1.147, 1.163, 1.156, 1.135, 1.106, 1.075, 1.045, 1.019, .999, .985, .977, .974, .974, .977, .982, .987, .992, .996, 1, 1)` | The mark's own response, 16 % overshoot. For small moves: an arrow nudging, a drawing lifting. |
| `semantic.easeSettle` | `--fp-ease-settle` | `linear(0, .11, .31, .503, .657, .771, .85, .904, .939, .962, .976, .985, .991, .994, .997, .998, 1)` | The same system critically damped: no overshoot. For large moves: a frame closing. |

<!-- /inject -->

- **Response** overshoots by 16 % and comes back, like the mark. Use it for small moves, where the overshoot is a pixel or two: an arrow nudging along, a drawing lifting.
- **Settle** is the same system critically damped: it arrives without overshoot. Use it for large moves, where an overshoot would look like a mistake: registration marks closing into a frame.

**One duration.**

<!-- inject:table semantic.duration -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.duration` | `--fp-duration` | `.6s` | How long a response takes to settle. |

<!-- /inject -->

**What moves:**
- The registration marks around a link close into a frame.
- An arrow after a link steps in its direction.
- The drawing in a cell lifts by a step.
- Underlines change colour without a transition.

**Reduced motion:** when the reader asks for less motion, nothing moves. Hover states still change; they just don't travel.

## By medium

**Web:** `--fp-ease-response`, `--fp-ease-settle` and `--fp-duration`. The components already use them. → [web](./media/web.md)

**Slides:** no transitions between slides and no builds for effect. Reveal a step only when the talk needs it.

## Never

- Animate content in on load or on scroll
- Use a splash screen, a parallax, or a background that moves
- Add a third easing, or a bounce stronger than the mark's
- Move something that nobody touched

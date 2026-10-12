# Python

Plots for papers, slides and the website, from matplotlib. The same tokens drive `faceplate-plots.sty` for pgfplots, so a chart looks the same whichever tool drew it. For the design rules, see [charts](../charts.md).

Current version: <!-- inject:version -->3.0.0<!-- /inject -->

## Use

```python
import sys; sys.path.insert(0, "<faceplate>/dist/python")   # or copy faceplate.py next to your script
import faceplate

faceplate.use()            # print sizes, for papers and A4; faceplate.use("slides") for Beamer
plt.plot(...)              # the categorical colours, in order
plt.imshow(z, cmap=faceplate.cmap("seq"))      # or cmap("div") for negative ↔ positive
```

`use()` does three things:
- registers the bundled fonts from `fonts/ttf/`
- registers the two ramps as the colormaps `faceplate_seq` and `faceplate_div`
- sets matplotlib's rcParams

`faceplate.marks(ax)` is for an image or a map: it removes the axis and holds the area with four registration marks. Their weight is the structural line of the document's intensity, which `use(intensity="01")` sets; the default is 02.

Without the module, `plt.style.use("<faceplate>/dist/python/faceplate.mplstyle")` gives the same rcParams, but no fonts or colormaps.

## What it sets

- **IBM Plex Mono throughout.** A plot is an instrument; titles and captions belong to the document around it.
- **Open, not boxed.** The baseline is the only axis line, in ink at keyline weight, with ruler ticks hanging from it. The value axis has labels only, and a hairline grid in the signature grey runs across.
- **No frames:** none around the plot, none around the legend.
- **The categorical series in order**, sage first. Flat fills, no edges on bars.
- **Sizes from the medium's scale**: tick labels `xxs`, axis labels `xs`, titles `sm`. The print and slide scales differ only in base.
- **The values as data:** `faceplate.COLORS`, `faceplate.DV`, `faceplate.SEQ`, `faceplate.DIV`, `faceplate.SIZES`, `faceplate.LINES`, `faceplate.LENGTHS`.

## Example

`examples/python/plot.py` writes `plot.pdf` and `plot.png`. `examples/latex/plots.tex` sets the pgfplots version of the same charts beside it.

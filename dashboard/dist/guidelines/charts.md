# Charts

Data visualisation and diagrams follow the same rules as everything else: square, flat, open, mono labels.

## Always

**A plot is open, not boxed.**
- **The baseline** is the only axis line: an ink keyline, with short ruler ticks hanging from it.
- **The value axis** has labels and no line. A hairline grid in the signature grey runs across.
- **Data** is drawn at keyline weight; fills are flat; bars stand on the baseline.
- **Labels** are mono: axis labels and the title in ink, tick labels muted. The legend has no frame.
- **An image or a map** has no axis at all. Four registration marks hold it, as they hold a photo. A colour bar is a strip with labels.

### Categorical

<!-- inject:table semantic.dv -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.dv1` | `--fp-dv-1` | `#5E7A50` |  |
| `semantic.dv2` | `--fp-dv-2` | `#BB6240` |  |
| `semantic.dv3` | `--fp-dv-3` | `#D6A23C` |  |
| `semantic.dv4` | `--fp-dv-4` | `#2C7A78` |  |
| `semantic.dv5` | `--fp-dv-5` | `#8A6A53` |  |
| `semantic.dv6` | `--fp-dv-6` | `#9BA0A4` |  |

<!-- /inject -->

The categorical series *is* the brand palette: `dv-1` and `dv-2` are sage and terracotta, not separate values that happen to match. Use them in order.

**Reserve, don't extend.** If a chart needs colours for both categories and status, budget the six deliberately. One project reserved terracotta for "deviation" and ochre for "swapped", leaving four hues for four categories; that's the right instinct. Running out is a signal to simplify the chart (six series is usually too many) or to [propose a colour](./extending.md), never to invent one.

### Sequential, low to high

<!-- inject:table semantic.seq -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.seq1` | `--fp-seq-1` | `#EFF3EA` |  |
| `semantic.seq2` | `--fp-seq-2` | `#D8E2CC` |  |
| `semantic.seq3` | `--fp-seq-3` | `#BFD0AC` |  |
| `semantic.seq4` | `--fp-seq-4` | `#A2BB89` |  |
| `semantic.seq5` | `--fp-seq-5` | `#82A268` |  |
| `semantic.seq6` | `--fp-seq-6` | `#5E7A50` |  |
| `semantic.seq7` | `--fp-seq-7` | `#41562F` |  |

<!-- /inject -->

### Diverging, negative to positive

<!-- inject:table semantic.div -->

| Token | CSS variable | Value | Notes |
|---|---|---|---|
| `semantic.div1` | `--fp-div-1` | `#9E4F33` |  |
| `semantic.div2` | `--fp-div-2` | `#C9876B` |  |
| `semantic.div3` | `--fp-div-3` | `#EAD3C5` |  |
| `semantic.div4` | `--fp-div-4` | `#F1F2EC` |  |
| `semantic.div5` | `--fp-div-5` | `#C3D2B0` |  |
| `semantic.div6` | `--fp-div-6` | `#88A56F` |  |
| `semantic.div7` | `--fp-div-7` | `#41562F` |  |

<!-- /inject -->

### Diagrams

Block diagrams, control loops and flowcharts use tinted fills with matching borders: a sage-tint node with a sage border, a terracotta-tint node with a terracotta border. Nodes stay legible and on-brand. Arrows and connectors are ink keylines.

## By theme

Dark: not defined yet. The categorical order stays the same.

## By medium

**Web:** the `--fp-dv-*`, `--fp-seq-*` and `--fp-div-*` roles, or `dist/tokens.json` for charting libraries.

**LaTeX:** `\usepackage{faceplate-plots}` makes every pgfplots axis a Faceplate axis. It provides:
- the categorical cycle list
- `faceplate bars` for bar charts
- `faceplate map` for images and maps: no axis, four registration marks
- the ramps as the colormaps `fpseq` and `fpdiv` (`faceplate diverging`)

The colours alone are `fpDv1`–`fpDv6`, `fpSeq1`–`fpSeq7` and `fpDiv1`–`fpDiv7`.

**Python:** `import faceplate; faceplate.use()` does the same for matplotlib, and `faceplate.marks(ax)` holds an image with four marks. → [Python](./media/python.md)

Both are generated from the same tokens, so a plot looks the same in a paper, on a slide and on the website. `examples/latex/plots.tex` sets them side by side.

**Plots are set in IBM Plex Mono throughout:** tick labels, axis labels, legend and title. The caption belongs to the document around it.

## Never

- Add a seventh categorical colour
- Round the bars
- Box a plot: no frame around the plot area, the legend or a colour bar
- Use gradients, shadows or 3D effects
- Reorder the categorical series per chart. Sage is always first.

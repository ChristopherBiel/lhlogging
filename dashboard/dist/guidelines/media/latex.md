# LaTeX

How documents pick up the system: a package with the values, the fonts and the mark, plus a class per kind of document. For the switches, the application cards and the design rules, see the [guidelines](../README.md).

Current version: <!-- inject:version -->3.0.0<!-- /inject -->

## Install, once per machine

```bash
git clone https://github.com/ChristopherBiel/faceplate && cd faceplate
npm run install-latex          # or, from a project that installed the package: npx faceplate install-latex
```

This links `dist/latex/` into your personal TeX tree (`kpsewhich -var-value TEXMFHOME`):
- the package and classes go to `tex/latex/faceplate/`
- Manrope goes to `fonts/truetype/faceplate/`

They're links, not copies, so `git pull` updates every document. `npx faceplate uninstall-latex` removes them.

**Overleaf, or a machine where you can't install:** upload the contents of `dist/latex/` (`faceplate.sty`, the classes, and the `fonts/` folder's files) next to your `.tex` file.

**Engine:** LuaLaTeX, or XeLaTeX. Under pdfLaTeX the colours, lengths and macros still work, but the fonts fall back and you get a warning.

**Fonts:**
- **Manrope** ships in `fonts/` (SIL OFL), in the five weights the system uses.
- **IBM Plex Mono** comes with TeX Live.
- Manrope has no italic, so emphasis (`\emph`) is SemiBold, never a faked slant.

## Start from a class

| Class or theme | For | Switches | Example |
|---|---|---|---|
| `fpcv` | a CV | personal · 01 · light | `examples/latex/cv.tex` → [card](../applications/cv.md) |
| `fpletter` | a letter; option `window` for envelopes | personal · 01 · light | `examples/latex/letter.tex` → [card](../applications/letter.md) |
| `fpdoc` | anything else on A4: a report, a sheet, a specimen | personal · 02 · light | `examples/latex/specimen.tex` |
| `\usetheme{faceplate}` | Beamer slides, 16:9 | personal · 04 · light | `examples/latex/slides.tex` → [card](../applications/slides.md) |
| `faceplate-plots` | pgfplots charts | follows the document | `examples/latex/plots.tex` → [charts](../charts.md) |

`fpcv` and `fpletter` are built on `fpdoc`, which is the [page chassis](../layout.md) on paper: the top bar, the lead, sections with a label column, rows and the footer. A design change there reaches every document.

```latex
\documentclass{fpcv}              % or [intensity=02]
\name{Christopher Biel}
\role{Role · Institution}
\photo{photo.jpg}                 % optional; crop square
\updated{10 / 2026}               % date stamp in the top bar
\contact{City\\ phone\\ \href{mailto:…}{email}\\ \href{https://…}{site}}
\begin{document}
\makecvheader{Profile, two to four sentences.}
\cvsection{Experience}
\cventry{MM.YYYY – Present}{Title}{Organisation}{Details\\ one per line}
\cvitem{Tools}{Python, C++}
\cvsection[ink]{Talks}            % a kind: sage (default), ink or terra
\begin{cvcolumns}
  \cvsection{Languages}  \cvitem{native}{German}
\nextcvcolumn
  \cvsection{Programming} \cvitem{\cvlevel{5}}{Python}
\end{cvcolumns}
\end{document}
```

A plain document on `fpdoc`:

```latex
\documentclass[intensity=02]{fpdoc}
\kind{Report}  \name{Christopher Biel}  \updated{10 / 2026}
\begin{document}
\makelead[A paragraph of text.]{Label}{The title}{One line under it.}
\fpsection{First section}
\fprow{10 / 2026}{A dated row.}
\fpitem{Key}{A row without a line.}
Text and figures after \fpsection start at the body's left edge; \fpbodywidth is its width.
\fpsection[ink]{Second section}
\end{document}
```

<a id="intensity"></a>
## Intensity

`intensity` is a real switch, as `data-fp-intensity` is on the web. It selects one level of the dial when the class or the package is loaded, and everything drawn from the dial follows: the structural line and its colour, the index squares, the lead's field, the display and title sizes, and room.

```latex
\documentclass[intensity=02]{fpcv}
```

- **The classes have a default each:** the CV and the letter 01, `fpdoc` 02, the deck 04. A document keeps one level throughout.
- **Room has paper values.** A sheet holds far less than a window. → [layout](../layout.md#by-medium)
- **At 04** the lead's field covers the top of the first page, top bar included. Fields behind the other sections are drawn on slides only; on an A4 sheet the sections stay on white.
- **Reading the dial:** `\fpdial{room}` gives a value by its token name, `\fpline` is the level's structural line, and `\fpsize{display}`, `{lede}`, `{title}` and `{index}` are the level's type steps. The colours are `fpFill`, `fpWash`, `fpLeadFill` and so on.

## Or use the package on its own

```latex
\usepackage[intensity=02, level=personal]{faceplate}
```

| Option | Values | Default |
|---|---|---|
| `intensity` | `01` `02` `03` `04` | `02` |
| `level` | `personal` `project` `neutral` | `personal` |
| `fonts` | `true` `false`: `false` keeps your document's fonts | `true` |
| `medium` | `print` `slides`: which base the type scale sits on | `print` |

`level` records intent; read it back as `\fplevel`.

| Macro | Gives |
|---|---|
| `\fpsize{base}` | a step of the closed scale, with its leading: `xxs` `xs` `sm` `base` `h3` `h2` `h1`, and `xl` `xxl` `xxxl` for display; or one of the dial's four: `display` `lede` `title` `index` |
| `\fpspace{4}` | a step of the spacing scale, 1–9, as a length |
| `\fpkeyline` `\fphairline` `\fpline` | the two line weights and the level's structural line, as lengths |
| `\fpdial{room}` | a value of the dial, by its token name |
| `\fpkind{ink}` | the kind of what follows: `sage`, `ink` or `terra` |
| `\fpindex{01}` | an index square: marks at 01, the kind's tint at 02, its solid colour from 03 |
| `\fpreg[distance]{…}` | four registration marks around anything, where a frame would be |
| `\fptickrule[columns]{width}{gutter}` | a section rule with its ruler ticks |
| `\fprule` | the sage accent rule |
| `\fplabel[colour]{…}` | mono, uppercase, tracked; muted by default |
| `\fpkicker{…}` | a label in deep sage, above a heading |
| `\fpmono{…}` | mono, untracked, as typed: dates, figures |
| `\fpwordmark{…}` | ExtraBold, tracked tight |
| `\fphr` | a signature-grey hairline across the line |
| `\fpchipsage{…}` `\fpchipterra{…}` `\fpchipgrey{…}` | square chips |
| `\fpmark[colour]{size}` | the bare mark, sage by default |
| `\fpmarkplate{size}` `\fpmarkonsage{size}` | the mark on its plate, or reversed on sage |
| `\fplabelfont` `\fpwordmarkfont` `\fpheadingfont` `\fpdisplayfont` `\fpmedium` `\fpsemibold` | font switches; `\fpdisplayfont` takes the weight the level gives the display line |

**Colours** are defined under their role names: `fpSage`, `fpSageDeep`, `fpInk`, `fpBody`, `fpMuted`, `fpGrey`, `fpDv1`–`fpDv6`, and so on. Never `\definecolor` a brand colour in a document.

**The mark** is traced from `assets/logo/mark.svg` at build time, so it is the same simulated curve. Its size is the edge of the SVG's 64-unit square, so sizes match the files; the bare mark's box is trimmed to the ink, so it stands on a baseline like a glyph.

`examples/latex/specimen.tex` shows everything on one page.

## Values

Print keeps the web scale's ratios on a print base; lines and spacing convert at the CSS reference pixel (1px = 0.75pt). See [typography](../typography.md#by-medium), [lines](../lines.md#by-medium) and [layout](../layout.md#by-medium).

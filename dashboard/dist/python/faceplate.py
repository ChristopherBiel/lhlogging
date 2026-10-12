"""Faceplate for matplotlib — v3.0.0.

GENERATED FROM tokens/*.json and build/templates/faceplate.py — DO NOT EDIT.

    import faceplate
    faceplate.use()                 # print sizes (papers, A4); faceplate.use("slides") for Beamer
    plt.plot(...)                   # the categorical colours, in order
    plt.imshow(z, cmap=faceplate.cmap("seq"))

    faceplate.marks(ax)             # an image or a map: no axis, four registration marks

Charts follow the guidelines (guidelines/charts.md). A plot is open, not boxed:
the baseline is its only axis line, with ruler ticks hanging from it; the grid
is hairlines across; fills are flat; the closed palette comes in order. Plots
are set in IBM Plex Mono throughout: they are instruments, and their captions
live in the document.
"""
from pathlib import Path

import matplotlib as mpl
from matplotlib import font_manager
from matplotlib.colors import LinearSegmentedColormap

VERSION = "3.0.0"

#: every colour role, by name
COLORS = {
    "bg": "#FFFFFF",
    "surface": "#F7F8F8",
    "border": "#E4E7E9",
    "muted": "#8A9097",
    "body": "#52585E",
    "ink": "#1C1E21",
    "sage": "#5E7A50",
    "sage_deep": "#455C3A",
    "sage_tint": "#DCE5D2",
    "sage_xl": "#EFF3EA",
    "terra": "#BB6240",
    "terra_deep": "#9E4F33",
    "terra_tint": "#F0D9CC",
    "grey": "#CBCBCB",
    "text_inverse": "#FFFFFF",
    "rule_color": "#5E7A50",
    "focus_ring_color": "#1C1E21"
}
#: the categorical series, in order — sage first, always
DV = ["#5E7A50","#BB6240","#D6A23C","#2C7A78","#8A6A53","#9BA0A4"]
#: sequential (low to high) and diverging (negative to positive) ramps
SEQ = ["#EFF3EA","#D8E2CC","#BFD0AC","#A2BB89","#82A268","#5E7A50","#41562F"]
DIV = ["#9E4F33","#C9876B","#EAD3C5","#F1F2EC","#C3D2B0","#88A56F","#41562F"]
#: the closed type scale per medium, in points
SIZES = {"print":{"xxs":5.94,"xs":7.13,"sm":8.31,"base":9.5,"h3":11.88,"h2":14.84,"h1":19,"xl":23.75,"xxl":29.69,"xxxl":38},"slides":{"xxs":6.88,"xs":8.25,"sm":9.63,"base":11,"h3":13.75,"h2":17.19,"h1":22,"xl":27.5,"xxl":34.38,"xxxl":44}}
#: line weights in points (1px = 0.75pt)
LINES = {"keyline":1.5,"hairline":0.75}
#: small lengths in points: a ruler tick, one stroke of a registration mark, its distance from the area
LENGTHS = {"tick":3,"mark":6,"markOffset":6}
#: the structural line of each intensity, in points: the weight of registration marks
DIAL_LINE = {"01":0.75,"02":1.5,"03":1.5,"04":1.5}

_state = {"intensity": "02"}

# fonts/ttf beside the package: <package>/dist/python/faceplate.py -> <package>/fonts/ttf
_FONT_DIRS = [Path(__file__).resolve().parents[2] / "fonts" / "ttf"]


def _register_fonts():
    found = False
    for d in _FONT_DIRS:
        for f in sorted(d.glob("*.ttf")) if d.is_dir() else []:
            font_manager.fontManager.addfont(str(f))
            found = True
    return found


def rc(medium="print"):
    """The rcParams for a medium ("print" or "slides"), as a dict."""
    s = SIZES[medium]
    return {
        "font.family": ["IBM Plex Mono", "DejaVu Sans Mono", "monospace"],
        "font.size": s["base"],
        "text.color": COLORS["body"],
        "axes.prop_cycle": mpl.cycler(color=DV),
        "axes.facecolor": COLORS["bg"],
        # open, not boxed: the baseline is the only axis line
        "axes.edgecolor": COLORS["ink"],
        "axes.linewidth": LINES["keyline"],
        "axes.spines.left": False,
        "axes.spines.right": False,
        "axes.spines.top": False,
        "axes.labelcolor": COLORS["ink"],
        "axes.labelsize": s["xs"],
        "axes.titlesize": s["sm"],
        "axes.titlecolor": COLORS["ink"],
        "axes.titlelocation": "left",
        "axes.titleweight": "medium",
        "axes.grid": True,
        "axes.grid.axis": "y",
        "axes.axisbelow": True,
        "grid.color": COLORS["grey"],
        "grid.linewidth": LINES["hairline"],
        "xtick.color": COLORS["ink"],
        "ytick.color": COLORS["ink"],
        "xtick.labelcolor": COLORS["muted"],
        "ytick.labelcolor": COLORS["muted"],
        "xtick.labelsize": s["xxs"],
        "ytick.labelsize": s["xxs"],
        # ruler ticks hang from the baseline; the value axis has labels only
        "xtick.major.width": LINES["hairline"],
        "xtick.major.size": LENGTHS["tick"],
        "xtick.direction": "out",
        "ytick.major.size": 0,
        "lines.linewidth": LINES["keyline"],
        "lines.solid_capstyle": "butt",
        "patch.linewidth": 0,
        "patch.edgecolor": COLORS["bg"],
        "legend.frameon": True,
        "legend.fancybox": False,
        "legend.edgecolor": "none",
        "legend.facecolor": COLORS["bg"],
        "legend.framealpha": 1,
        "legend.fontsize": s["xxs"],
        "legend.borderpad": 0.6,
        "figure.facecolor": COLORS["bg"],
        "savefig.facecolor": COLORS["bg"],
        "savefig.bbox": "tight",
        "figure.dpi": 150,
        "image.cmap": "faceplate_seq",
    }


def cmap(name="seq"):
    """The sequential ("seq") or diverging ("div") ramp as a matplotlib colormap."""
    return LinearSegmentedColormap.from_list(f"faceplate_{name}", {"seq": SEQ, "div": DIV}[name])


def marks(ax, intensity=None):
    """Hold an image or a map with four registration marks instead of an axis.

    Removes the axis lines and ticks of ``ax`` and draws the marks just outside
    its corners, at the structural line weight of the intensity (the one given
    to ``use()`` unless ``intensity`` says otherwise).
    """
    from matplotlib.lines import Line2D
    from matplotlib.transforms import ScaledTranslation

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.grid(False)
    fig = ax.figure
    weight = DIAL_LINE[intensity or _state["intensity"]]
    out, length = LENGTHS["markOffset"] / 72, LENGTHS["mark"] / 72
    for x in (0, 1):
        for y in (0, 1):
            sx, sy = (-1 if x == 0 else 1), (-1 if y == 0 else 1)
            # inches from the corner, so the marks follow the area when the layout moves it
            at_corner = fig.dpi_scale_trans + ScaledTranslation(x, y, ax.transAxes)
            ax.add_artist(Line2D([sx * out, sx * out, sx * (out - length)], [sy * (out - length), sy * out, sy * out],
                                 transform=at_corner, color=COLORS["ink"], linewidth=weight,
                                 solid_capstyle="butt", solid_joinstyle="miter", clip_on=False))
    # a title stands clear of the marks
    if ax.get_title(loc="left"):
        ax.set_title(ax.get_title(loc="left"), loc="left", pad=LENGTHS["markOffset"] + LENGTHS["mark"])
    return ax


def use(medium="print", intensity="02"):
    """Apply Faceplate to matplotlib. Registers the bundled fonts when they are there.

    ``intensity`` is the level of the document the plots go into ("01" to "04");
    it sets the weight of registration marks and nothing else.
    """
    if intensity not in DIAL_LINE:
        raise ValueError("intensity must be '01', '02', '03' or '04'")
    _state["intensity"] = intensity
    if not _register_fonts():
        import warnings
        warnings.warn("faceplate: fonts/ttf not found beside the package; falling back to system fonts")
    for name in ("seq", "div"):
        if f"faceplate_{name}" not in mpl.colormaps:
            mpl.colormaps.register(cmap(name))
    mpl.rcParams.update(rc(medium))

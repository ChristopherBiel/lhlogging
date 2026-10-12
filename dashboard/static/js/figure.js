// Figures, as the pages draw them: the SVG helpers, the tooltip and the figure
// scaffold that /insights and the monitor share (window.Fig). Builds on
// page.js, whose helpers it hands on.
//
// No libraries: the pages make no third-party requests, and a hand-drawn SVG
// can follow Faceplate to the pixel (square marks, mono axis labels, tokens
// only — every colour is a class or a var(--fp-*) from figure.css).
window.Fig = (() => {
'use strict';

const SVG_NS = 'http://www.w3.org/2000/svg';
const {$, el, esc, say, plural, section, state, stats, seg, table} = window.Page;

// The tails this visitor planned on /book (browser storage, no accounts).
function targets() {
  let v = [];
  try { v = JSON.parse(localStorage.getItem('lh.targets') || '[]'); } catch (e) { /* private mode */ }
  return new Set(Array.isArray(v) ? v : []);
}

// ── SVG helpers ────────────────────────────────────────────────────────
function svg(tag, attrs, parent) {
  const e = document.createElementNS(SVG_NS, tag);
  for (const k in attrs) if (attrs[k] != null) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}
function text(parent, x, y, str, cls, anchor) {
  const t = svg('text', {x, y, class: cls, 'text-anchor': anchor}, parent);
  t.textContent = str;
  return t;
}
const line = (parent, x1, y1, x2, y2, cls) => svg('line', {x1, y1, x2, y2, class: cls}, parent);
// Mono text is 0.6 em wide, so a label's width is known without measuring it.
const textW = str => String(str).length * 6;
// A label on a white box, so it can sit on a line or a strand (start-anchored).
function boxed(parent, x, y, str, cls) {
  svg('rect', {x: x - 3, y: y - 10, width: textW(str) + 6, height: 14, class: 'label-box'}, parent);
  return text(parent, x, y, str, cls);
}
const rect = (parent, x, y, w, h, cls) => svg('rect', {x, y, width: Math.max(0, w), height: Math.max(0, h), class: cls}, parent);
const crisp = v => Math.round(v) + 0.5;                    // a hairline on the pixel grid
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
const pct = (p, d = 0) => (p * 100).toFixed(d) + '%';

function plot(host, W, H, label) {
  return svg('svg', {class: 'plot', width: W, height: H, viewBox: '0 0 ' + W + ' ' + H,
                     role: 'img', 'aria-label': label}, host);
}
// A plot is open, not boxed. A matrix has no axis: four registration marks
// hold it where a frame would stand, a little outside the area so that they
// never cover a cell.
function marks(parent, x, y, w, h) {
  const out = 4, n = 8;
  const x0 = Math.round(x) - out, y0 = Math.round(y) - out, x1 = Math.round(x + w) + out, y1 = Math.round(y + h) + out;
  return svg('path', {class: 'reg', d: 'M' + x0 + ' ' + (y0 + n) + 'V' + y0 + 'H' + (x0 + n) +
    'M' + (x1 - n) + ' ' + y0 + 'H' + x1 + 'V' + (y0 + n) + 'M' + x1 + ' ' + (y1 - n) + 'V' + y1 + 'H' + (x1 - n) +
    'M' + (x0 + n) + ' ' + y1 + 'H' + x0 + 'V' + (y1 - n)}, parent);
}
// A plot with an axis has one axis line, the baseline: an ink keyline.
const baseline = (parent, x1, x2, y) => line(parent, x1, Math.round(y), x2, Math.round(y), 'base');
// Markers are angular (Faceplate has no round corner to borrow from).
function marker(parent, shape, x, y, r, cls) {
  if (shape === 'square') return rect(parent, x - r, y - r, 2 * r, 2 * r, cls);
  const pts = shape === 'diamond' ? [[0, -1.3], [1.3, 0], [0, 1.3], [-1.3, 0]]
            : shape === 'up' ? [[0, -1.25], [1.25, 1], [-1.25, 1]]
            : [[0, 1.25], [1.25, -1], [-1.25, -1]];
  return svg('polygon', {points: pts.map(p => (x + p[0] * r) + ',' + (y + p[1] * r)).join(' '), class: cls}, parent);
}
// Spread labels that would overlap: neighbours closer than `gap` merge into a
// block centred on their mean, so nobody drifts further than it has to.
function dodge(ys, gap, lo, hi) {
  const blocks = [];
  ys.forEach(y => {
    blocks.push({n: 1, sum: y});
    while (blocks.length > 1) {
      const b = blocks[blocks.length - 1], a = blocks[blocks.length - 2];
      const topB = b.sum / b.n - (b.n - 1) * gap / 2, botA = a.sum / a.n + (a.n - 1) * gap / 2;
      if (topB - botA >= gap) break;
      a.sum += b.sum; a.n += b.n; blocks.pop();
    }
  });
  const out = [];
  blocks.forEach(b => {
    const first = clamp(b.sum / b.n - (b.n - 1) * gap / 2, lo, Math.max(lo, hi - (b.n - 1) * gap));
    for (let i = 0; i < b.n; i++) out.push(first + i * gap);
  });
  return out;
}
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const dayLabel = d => d.getUTCDate() + ' ' + MONTHS[d.getUTCMonth()];
const isoDay = iso => dayLabel(new Date(iso + 'T00:00:00Z'));

// ── tooltip (one for the page; every value is also in the Data view) ───
const tip = document.body.appendChild(el('div', 'tip'));
tip.hidden = true;
function showTip(ev, title, rows, foot) {
  tip.replaceChildren();
  if (title) tip.append(el('div', 'tip-title', title));
  (rows || []).forEach(r => {
    const row = el('div', 'tip-row' + (r.on ? ' on' : ''));
    row.append(el('span', 'tip-key', r.key), el('span', 'tip-val', r.val));
    if (r.sub) row.append(el('span', 'tip-sub', r.sub));
    tip.append(row);
  });
  if (foot) tip.append(el('div', 'tip-foot', foot));
  tip.hidden = false;
  moveTip(ev);
}
function moveTip(ev) {
  const w = tip.offsetWidth, h = tip.offsetHeight, pad = 14;
  let x = ev.clientX + pad, y = ev.clientY + pad;
  if (x + w > innerWidth - 8) x = ev.clientX - w - pad;
  if (y + h > innerHeight - 8) y = ev.clientY - h - pad;
  tip.style.left = Math.max(8, x) + 'px';
  tip.style.top = Math.max(8, y) + 'px';
}
const hideTip = () => { tip.hidden = true; };

// ── a legend's key ─────────────────────────────────────────────────────
function key(draw, label) {
  const k = el('span', 'key');
  const s = svg('svg', {width: 26, height: 12, viewBox: '0 0 26 12', 'aria-hidden': 'true', class: 'plot'}, k);
  draw(s);
  k.append(document.createTextNode(label));
  return k;
}
// ── figure scaffold ────────────────────────────────────────────────────
// A figure is a wide section of the chassis: its title (a question, on
// /insights), the finding in a sentence, the legend, the plot and its twin
// table, and a note on how it was measured; a Plot/Data switch and the
// figure's own controls stand in the section's note (f.tools). A compact one
// ({compact: true}) shows the title and the plot and keeps the rest folded
// under the plot until it is asked for. `note`, `kind` and `field` go to the
// section.
const figures = [];
function figure(key_, question, opt) {
  opt = opt || {};
  const sec = section({id: 'fig-' + key_, title: question, note: opt.note, kind: opt.kind, field: opt.field, wide: true});
  const f = {root: sec.root, w: 0, dirty: true, draw: null, table: null,
             find: el('p', 'fig-find'), legend: el('div', 'legend'), tools: sec.note,
             plot: el('div', 'fig-plot'), data: el('div', 'fig-data table-scroll'), note: el('p', 'fig-note')};
  const fill = () => { f.data.replaceChildren(); f.table(f.data); };
  if (opt.compact) {
    f.more = el('details', 'fold');
    f.more.append(el('summary', 'fold__sum', 'How to read this'), f.find, f.legend, f.note, f.data);
    f.more.addEventListener('toggle', () => { if (f.more.open) fill(); });
    // after a control changed what the figure shows: redo the table if it is on view
    f.refresh = () => { if (f.more.open) fill(); };
    sec.body.append(f.plot, f.more);
  } else {
    f.data.hidden = true;
    f.view = seg([['plot', 'Plot'], ['data', 'Data']], 'plot', v => {
      f.plot.hidden = f.legend.hidden = v !== 'plot';
      f.data.hidden = v !== 'data';
      if (v === 'data') fill(); else redraw();
    }, 'Plot or data table');
    f.tools.append(f.view);
    f.refresh = () => { if (!f.data.hidden) fill(); };
    sec.body.append(f.find, f.legend, f.plot, f.data, f.note);
  }
  figures.push(f);
  return f;
}
function redraw() {
  figures.forEach(f => {
    if (f.plot.hidden) { f.dirty = true; return; }
    const w = Math.floor(f.plot.clientWidth);
    if (w > 0 && (w !== f.w || f.dirty)) {
      f.w = w; f.dirty = false;
      f.plot.replaceChildren();
      f.draw(f.plot, w);
    }
  });
}
// Redraw every figure at its new width when `host` changes size, and drop a
// tooltip that scrolling has left behind.
function watch(host) {
  let queued = false;
  new ResizeObserver(() => {
    if (queued) return;
    queued = true;
    requestAnimationFrame(() => { queued = false; redraw(); });
  }).observe(host);
  addEventListener('scroll', hideTip, {passive: true});
}

return {$, targets, el, esc, svg, text, line, textW, boxed, rect, crisp, clamp, pct, plural, plot, marks, baseline, marker, dodge,
        MONTHS, DOW, dayLabel, isoDay, say, showTip, moveTip, hideTip, seg, key, table, section, state, stats, figure, redraw, watch};
})();

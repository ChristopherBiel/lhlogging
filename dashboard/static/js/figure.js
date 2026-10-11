// Figures, as the pages draw them: the SVG helpers, the tooltip and the
// figure scaffold that /insights and the monitor share (window.Fig).
//
// No libraries: the pages make no third-party requests, and a hand-drawn SVG
// can follow Faceplate to the pixel (square marks, mono axis labels, tokens
// only — every colour is a class or a var(--fp-*) from figure.css).
window.Fig = (() => {
'use strict';

const SVG_NS = 'http://www.w3.org/2000/svg';
const $ = id => document.getElementById(id);

// The tails this visitor planned on /book (browser storage, no accounts).
function targets() {
  let v = [];
  try { v = JSON.parse(localStorage.getItem('lh.targets') || '[]'); } catch (e) { /* private mode */ }
  return new Set(Array.isArray(v) ? v : []);
}

// ── DOM and SVG helpers ────────────────────────────────────────────────
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
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
const plural = (n, word) => n + ' ' + word + (n === 1 ? '' : 's');

function plot(host, W, H, label) {
  return svg('svg', {class: 'plot', width: W, height: H, viewBox: '0 0 ' + W + ' ' + H,
                     role: 'img', 'aria-label': label}, host);
}
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

// A sentence with some parts set bold: say(p, 'plain ', ['bold'], ' plain').
function say(node, ...parts) {
  node.replaceChildren(...parts.map(p => Array.isArray(p) ? el('b', null, p[0]) : document.createTextNode(p)));
}

// ── tooltip (one for the page; every value is also in the Data view) ───
const tip = $('tip');
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

// ── controls ───────────────────────────────────────────────────────────
function seg(options, value, onChange, label) {
  const box = el('div', 'fp-seg');
  box.setAttribute('role', 'group');
  if (label) box.setAttribute('aria-label', label);
  options.forEach(([v, name]) => {
    const b = el('button', v === value ? 'is-on' : '', name);
    b.type = 'button';
    b.setAttribute('aria-pressed', String(v === value));
    b.addEventListener('click', () => {
      box.querySelectorAll('button').forEach(o => {
        o.classList.toggle('is-on', o === b);
        o.setAttribute('aria-pressed', String(o === b));
      });
      onChange(v);
    });
    box.append(b);
  });
  return box;
}
function key(draw, label) {
  const k = el('span', 'key');
  const s = svg('svg', {width: 26, height: 12, viewBox: '0 0 26 12', 'aria-hidden': 'true', class: 'plot'}, k);
  draw(s);
  k.append(document.createTextNode(label));
  return k;
}
function table(host, head, rows) {
  const t = el('table');
  const tr = el('tr');
  head.forEach(h => tr.append(el('th', typeof h === 'string' ? '' : 'r', typeof h === 'string' ? h : h[0])));
  t.append(tr);
  rows.forEach(r => {
    const row = el('tr');
    r.forEach((c, i) => {
      const td = el('td', typeof head[i] === 'string' ? '' : 'r');
      if (c && c.href) { const a = el('a', 'tail-link', c.text); a.href = c.href; td.append(a); }
      else td.textContent = c == null ? '—' : c;
      row.append(td);
    });
    t.append(row);
  });
  host.append(t);
}

// ── figure scaffold ────────────────────────────────────────────────────
// A figure is a question, the finding in a sentence, the plot and its twin
// table, and a note on how it was measured. A compact one ({compact: true})
// shows a plain title and the plot and keeps the rest — finding, legend, note
// and table — folded under the plot until it is asked for.
const figures = [];
function figure(key_, question, opt) {
  const compact = !!(opt && opt.compact);
  const no = figures.length + 1;
  const root = el('section', compact ? 'fig fig--compact' : 'fig');
  root.id = 'fig-' + key_;
  const f = {root, w: 0, dirty: true, draw: null, table: null,
             find: el('p', 'fig-find'), legend: el('div', 'legend'), tools: el('div', 'fig-tools'),
             plot: el('div', 'fig-plot'), data: el('div', 'fig-data'), note: el('p', 'fig-note')};
  const fill = () => { f.data.replaceChildren(); f.table(f.data); };
  const head = el('div', 'fig-head');
  if (compact) {
    head.append(el('h2', 'fig-q', question), f.tools);
    f.more = el('details', 'fig-more');
    f.more.append(el('summary', 'fig-more-sum', 'How to read this'), f.find, f.legend, f.note, f.data);
    f.more.addEventListener('toggle', () => { if (f.more.open) fill(); });
    // after a control changed what the figure shows: redo the table if it is on view
    f.refresh = () => { if (f.more.open) fill(); };
    root.append(head, f.plot, f.more);
  } else {
    f.data.hidden = true;
    head.append(el('div', 'fig-no', 'Fig. ' + String(no).padStart(2, '0')), el('h2', 'fig-q', question), f.find);
    const bar = el('div', 'fig-bar');
    f.view = seg([['plot', 'Plot'], ['data', 'Data']], 'plot', v => {
      f.plot.hidden = f.legend.hidden = v !== 'plot';
      f.data.hidden = v !== 'data';
      if (v === 'data') fill(); else redraw();
    }, 'Plot or data table');
    f.tools.append(f.view);
    f.refresh = () => { if (!f.data.hidden) fill(); };
    bar.append(f.legend, f.tools);
    root.append(head, bar, f.plot, f.data, f.note);
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

return {$, targets, el, svg, text, line, textW, boxed, rect, crisp, clamp, pct, plural, plot, marker, dodge,
        MONTHS, DOW, dayLabel, isoDay, say, showTip, moveTip, hideTip, seg, key, table, figure, redraw, watch};
})();

// What every page built by a script shares (window.Page): small DOM helpers
// and the parts of the page chassis a script makes — a section, a status
// marker, key figures, a segmented control, a table. The pages with figures
// build on it (figure.js).
window.Page = (() => {
'use strict';

const $ = id => document.getElementById(id);
const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'})[c]);
const plural = (n, word) => n + ' ' + word + (n === 1 ? '' : 's');
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

// A sentence with some parts set bold: say(p, 'plain ', ['bold'], ' plain').
function say(node, ...parts) {
  node.replaceChildren(...parts.map(p => Array.isArray(p) ? el('b', null, p[0]) : document.createTextNode(p)));
}

// ── the page chassis ───────────────────────────────────────────────────
// A section: its label (the index square, the title, a note) and its body.
// Sections number themselves in the order they are made. `kind: 'ink'` is a
// log or a history (Faceplate's code: ink for what is public and dated, sage
// for the work itself). `field: 'tint'` is the field the section sits on at
// intensity 04 — it does nothing below, and is set from the start so that
// neighbours differ when the dial is turned up; without it the section stays
// on the paper. `wide` gives the body all twelve columns, with the label as a
// line above it — what a dense plot needs.
let sections = 0;
function section(o) {
  const root = el('section', 'fp-sec' + (o.kind === 'ink' ? ' fp-sec--ink' : '') + (o.field ? ' fp-sec--' + o.field : '') +
                             (o.wide ? ' sec--wide' : ''));
  if (o.id) root.id = o.id;
  const label = el('header', 'fp-sec__label'), note = el('div', 'fp-sec__note'), body = el('div', 'fp-sec__body');
  if (o.note) note.append(el('span', null, o.note));
  label.append(el('span', 'fp-index', String(++sections).padStart(2, '0')), el('h2', 'fp-sec__title', o.title), note);
  root.append(label, body);
  return {root, label, note, body};
}
// A status marker: how something stands (ok, note, bad), or what stands in for
// content that is loading (wait) or could not be loaded (bad).
const state = (level, words) => el('p', 'fp-status state state--' + level, words);
// Key figures: [what it counts, the number, a note], `n` to a row.
function stats(items, n) {
  const row = el('dl', 'stats' + (n ? ' stats--' + n : ''));
  items.forEach(([what, value, note]) => {
    const cell = el('div', 'stat');
    cell.append(el('dt', 'stat__key', what), el('dd', 'stat__num', value), el('dd', 'stat__note', note || ''));
    row.append(cell);
  });
  return row;
}

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
// A column given as [name, 'r'] holds numbers: mono and right-aligned.
function table(host, head, rows) {
  const t = el('table', 'table');
  const tr = el('tr');
  head.forEach(h => tr.append(el('th', typeof h === 'string' ? '' : 'num', typeof h === 'string' ? h : h[0])));
  t.append(tr);
  rows.forEach(r => {
    const row = el('tr');
    r.forEach((c, i) => {
      const td = el('td', typeof head[i] === 'string' ? '' : 'num');
      if (c && c.href) { const a = el('a', 'tail-link', c.text); a.href = c.href; td.append(a); }
      else td.textContent = c == null ? '—' : c;
      row.append(td);
    });
    t.append(row);
  });
  host.append(t);
}

return {$, el, esc, say, plural, section, state, stats, seg, table};
})();

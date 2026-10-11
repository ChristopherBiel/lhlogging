// /insights — the fleet field guide.
//
// Draws every figure as inline SVG from /api/insights (leg_stats.insights),
// with the helpers and the figure scaffold of figure.js.
//
// Colour does three jobs on this page and nothing else: sage is this fleet's
// data, grey is context (the other fleets, weak pairings), terracotta is the
// exception (a tail that is not flying, a route below the fleet's rate). The
// Faceplate series hues are not used to tell fleets apart: sage and
// terracotta collapse under red-green colour blindness, so fleets are told
// apart by marker shape and a label instead.
(() => {
'use strict';

const {$, targets, el, svg, text, line, textW, boxed, rect, crisp, clamp, pct, plural, plot, marker, dodge,
       MONTHS, DOW, dayLabel, isoDay, say, showTip, moveTip, hideTip, seg, key, table, figure, redraw, watch} = window.Fig;
const FLEETS = [['B748', '747-8'], ['A388', 'A380'], ['B787', '787'], ['A350', 'A350']];
const NAME = Object.fromEntries(FLEETS);
const SHAPE = {B748: 'square', A388: 'diamond', B787: 'up', A350: 'down'};
const QUERY = new URLSearchParams(location.search);
const REG = (QUERY.get('reg') || '').toUpperCase();

// The visitor's own tails, plus the one an /airframe page sent along:
// starred in every figure.
const MINE = targets();
if (REG) MINE.add(REG);

const tailHref = reg => '/airframe/' + encodeURIComponent(reg);
const clock = min => String(Math.floor(min / 60)).padStart(2, '0') + ':' + String(min % 60).padStart(2, '0');
const ground = min => Math.floor(min / 60) + ' h ' + String(Math.round(min % 60)).padStart(2, '0');
const cabin = s => s ? s.replace(/([A-Z])(\d+)/g, '$1$2 ').trim() : 'layout unknown';
const routeName = r => [r.a].concat(r.via || [], [r.b]).join('–');

// ── What changed lately ────────────────────────────────────────────────
const CHIP = {grounded: ['No flight', 'terra'], returned: ['Back', 'sage'], new_tail: ['New tail', 'sage'],
              new_route: ['New route', 'sage'], gone_route: ['Paused', 'gray'],
              diverted: ['Diverted', 'terra'], cancelled: ['Cancelled', 'gray']};
function news(D) {
  const E = D.events;
  const box = el('section', 'news');
  const column = (title, items, empty) => {
    const col = el('div', 'news-col');
    col.append(el('h2', 'news-h', title));
    if (!items.length) { col.append(el('p', 'news-none', empty)); return col; }
    const ul = el('ul', 'news-list');
    items.forEach(e => {
      const li = el('li');
      const chip = CHIP[e.kind] || [e.kind, 'gray'];
      const what = el('span', 'news-what');
      if (e.reg) {
        const a = el('a', 'tail-link', (MINE.has(e.reg) ? '★ ' : '') + e.reg);
        a.href = tailHref(e.reg);
        what.append(a, document.createTextNode(' ' + e.text));
      } else what.textContent = e.text;
      li.append(el('span', 'news-when', isoDay(e.date)), el('span', 'fp-chip fp-chip--' + chip[1], chip[0]), what);
      ul.append(li);
    });
    col.append(ul);
    return col;
  };
  box.append(
    column('Not flying now', E.now, 'Every ' + NAME[D.family] + ' has flown in the last three days.'),
    column('Last ' + E.days + ' days', E.recent, 'Nothing out of the ordinary.'));
  return box;
}

// ── Fig. 1 — when can I trust a published tail? ────────────────────────
function trust(D) {
  const f = figure('trust', 'When can I trust a published tail?');
  const fleets = D.trust.fleets, minN = D.trust.min_n;
  const me = fleets.find(x => x.id === D.family);
  const name = NAME[me.id];
  const shown = fl => fl.curve.filter(c => c.n >= minN);     // farthest first
  const at = (fl, band) => fl.curve.find(c => c.band === band && c.n >= minN);
  const mine = shown(me);

  // the finding, from the numbers drawn
  const parts = [];
  const c48 = at(me, 48), c24 = at(me, 24);
  if (c48 && c24) parts.push('A ' + name + ' tail published two days out flies the flight ', [pct(c48.p)],
                             ' of the time; one day out, ', [pct(c24.p)], '. ');
  if (me.t80) parts.push('It becomes a four-in-five bet about ', [Math.round(me.t80) + ' hours'], ' before departure');
  if (me.t80 && me.t50) parts.push(' and beats a coin flip from about ', [Math.round(me.t50) + ' hours'], '. ');
  else if (me.t80 && mine.length) parts.push(' and never drops to a coin flip in the ' + mine[0].band + ' hours we can see. ');
  const late = (me.settling || []).filter(s => s.far <= 48 && s.near >= 12);
  if (late.length) {
    const rate = late.reduce((a, s) => a + s.gain, 0) / late.reduce((a, s) => a + (s.far - s.near), 0) * 100;
    parts.push('Inside two days there is no decision moment: each hour you wait adds about ',
               [rate.toFixed(1) + ' points'], '.');
  }
  const rank = fleets.map(fl => [fl, at(fl, 72)]).filter(x => x[1]).sort((a, b) => b[1].p - a[1].p);
  if (rank.length > 1) parts.push(' Three days out the ' + NAME[rank[0][0].id] + ' holds best (' + pct(rank[0][1].p) +
                                  ') and the ' + NAME[rank[rank.length - 1][0].id] + ' worst (' +
                                  pct(rank[rank.length - 1][1].p) + ').');
  say(f.find, ...parts);

  fleets.forEach(fl => f.legend.append(key(s => {
    const cls = fl === me ? 'this' : 'ctx';
    line(s, 1, 6, 25, 6, 'ser-line ' + cls);
    marker(s, SHAPE[fl.id], 13, 6, fl === me ? 4 : 3.5, 'ser-mark ' + cls);
  }, NAME[fl.id] + (fl === me ? ' (this page)' : ''))));
  f.legend.append(key(s => rect(s, 1, 2, 24, 8, 'band'), '95% interval'),
                  key(s => line(s, 1, 6, 25, 6, 'ref'), 'even share of the fleet'));
  f.note.textContent = 'Panel A: of the ' + name + ' flights of the last ' + D.meta.weeks + ' weeks that had a tail ' +
    'published at each lead time, the share on which that tail flew. Panel B: the same flights followed from one ' +
    'lead time to the next — how many more had the right tail, per hour waited. Lead times with fewer than ' + minN +
    ' flights are left out of the plot (they are in Data). The even share is one over the ' + me.tails +
    ' tails that flew: what a guess with no information scores.';

  f.draw = (host, W) => {
    if (!fleets.some(fl => shown(fl).length)) { host.append(el('p', 'empty', 'Not enough flights yet.')); return; }
    const narrow = W < 620;
    const m = {l: 44, r: narrow ? 8 : 16, t: 36, b: 42};
    const H1 = narrow ? 250 : 340, gap = 36, H2 = narrow ? 72 : 92;
    const H = m.t + H1 + gap + H2 + m.b;
    const root = plot(host, W, H, 'Share of flights flown by the published tail, by hours before departure, per fleet');
    const x0 = m.l, x1 = W - m.r, yT = m.t, yB = m.t + H1, y2T = yB + gap, y2B = y2T + H2;
    const far = Math.max(...fleets.flatMap(fl => shown(fl).map(c => c.band)));
    const lo = Math.log2(3 / 1.18), hi = Math.log2(far * 1.12);
    const X = h => x0 + (hi - Math.log2(h)) / (hi - lo) * (x1 - x0);
    const Y = p => yB - p * H1;
    const gains = me.settling || [];
    const rTop = Math.max(1, Math.ceil(Math.max(0, ...gains.map(s => s.per_hour * 100)) * 2) / 2);
    const Y2 = r => y2B - clamp(r, 0, rTop) / rTop * H2;
    const bands = [...new Set(fleets.flatMap(fl => shown(fl).map(c => c.band)))].sort((a, b) => a - b);

    // grid and axes
    const g = svg('g', null, root);
    for (let i = 0; i <= 10; i++) {
      const y = crisp(Y(i / 10));
      if (i % 2 === 0) {
        line(g, x0, y, x1, y, 'grid');
        text(g, x0 - 8, y + 3.5, i * 10 + (i === 10 ? '%' : ''), 't-tick', 'end');
      }
      line(g, x0 - (i % 2 ? 3 : 5), y, x0, y, 'tick');
    }
    [0.5, 1, 2, 3, 4, 5, 7, 9].filter(d => d * 24 <= far * 1.05).forEach(d => {
      const x = crisp(X(d * 24));
      line(g, x, yT, x, yB, 'grid'); line(g, x, y2T, x, y2B, 'grid');
      line(g, x, yT - 5, x, yT, 'tick');
      text(g, x, yT - 9, d === 0.5 ? '12 h' : d + (narrow ? 'd' : ' d'), 't-tick', 'middle');
    });
    text(g, x0, 11, 'A · Published tail flew the flight', 't-title');
    if (!narrow) text(g, x1, 11, 'days before departure', 't-title', 'end');
    text(g, x0, y2T - 9, 'B · Points gained per hour ' + (narrow ? 'waited' : 'of waiting, ' + name), 't-title');
    for (let r = 0; r <= rTop; r += 0.5) {
      const y = crisp(Y2(r));
      line(g, x0, y, x1, y, 'grid'); line(g, x0 - 5, y, x0, y, 'tick');
      text(g, x0 - 8, y + 3.5, r.toFixed(1), 't-tick', 'end');
    }
    let lastX = Infinity;
    bands.forEach(b => {
      const x = crisp(X(b));
      line(g, x, yB, x, yB + 4, 'tick'); line(g, x, y2B, x, y2B + 5, 'tick');
      if (lastX - x >= 24) { text(g, x, y2B + 17, b, 't-tick', 'middle'); lastX = x; }
    });
    text(g, (x0 + x1) / 2, H - 6, narrow ? 'hours before departure (log)' : 'hours before departure · log scale', 't-title', 'middle');

    // panel B: what an hour of waiting buys
    gains.forEach(s => {
      const xa = X(s.far) + 1, xb = X(s.near) - 1, y = Y2(s.per_hour * 100);
      rect(g, xa, y, xb - xa, y2B - y, 'gain');
      line(g, xa, y, xb, y, 'gain-top');
    });

    // even share: what a guess with no information scores
    const yC = crisp(Y(1 / me.tails));
    line(root, x0, yC, x1, yC, 'ref');
    text(root, x1 - 5, yC - 5, (narrow ? '' : 'even share · ') + '1 in ' + me.tails, 't-tick', 'end');

    // the fleets: context first, this fleet on top
    const labels = [];
    fleets.filter(fl => fl !== me).concat([me]).forEach(fl => {
      const pts = shown(fl);
      if (!pts.length) return;
      const cls = fl === me ? 'this' : 'ctx';
      const sg = svg('g', null, root);
      if (fl === me) svg('polygon', {class: 'band', points:
        pts.map(c => X(c.band) + ',' + Y(c.hi)).concat(pts.slice().reverse().map(c => X(c.band) + ',' + Y(c.lo))).join(' ')}, sg);
      svg('polyline', {class: 'ser-line ' + cls, points: pts.map(c => X(c.band) + ',' + Y(c.p)).join(' ')}, sg);
      pts.forEach(c => marker(sg, SHAPE[fl.id], X(c.band), Y(c.p), fl === me ? 4 : 3.5, 'ser-mark ' + cls));
      labels.push({fl, x: X(pts[0].band), y: Y(pts[0].p)});
    });
    // direct labels at the far end, where the fleets are furthest apart
    labels.sort((a, b) => a.y - b.y);
    const placed = [];
    (narrow ? [] : labels).forEach(l => {
      const left = l.x - x0 > 58;
      let lx = left ? l.x - 9 : l.x + 2, ly = left ? l.y - 3 : l.y - 10;
      const w = NAME[l.fl.id].length * 7.5;
      const box = () => ({a: left ? lx - w : lx, b: left ? lx : lx + w, y: ly});
      while (placed.some(p => Math.abs(p.y - box().y) < 13 && p.a < box().b && box().a < p.b)) ly += 13;
      placed.push(box());
      text(root, lx, ly, NAME[l.fl.id], 't-lab' + (l.fl === me ? '' : ' ctx'), left ? 'end' : 'start');
    });

    // where this fleet crosses a coin flip and four in five
    const marks = [[0.5, me.t50], [0.8, me.t80]].filter(t => t[1]);
    marks.forEach(([p, t], i) => {
      const x = crisp(X(t)), y = crisp(Y(p));
      line(root, x0, y, x, y, 'ann'); line(root, x, y, x, yB, 'ann');
      rect(root, x - 3, y - 3, 6, 6, 'ann-dot');
      const close = marks.length > 1 && Math.abs(X(marks[0][1]) - X(marks[1][1])) < 118;
      boxed(root, x + 6, yB - 6 - (close && i === 0 ? 15 : 0), pct(p) + ' inside ' + Math.round(t) + ' h', 't-ann');
    });
    rect(root, crisp(x0), crisp(yT), x1 - x0, H1, 'frame');
    rect(root, crisp(x0), crisp(y2T), x1 - x0, H2, 'frame');

    // hover: the crosshair finds the lead time, the tooltip reads every fleet
    const cross = line(root, 0, yT, 0, y2B, 'cross');
    cross.style.display = 'none';
    const hit = rect(root, x0, yT, x1 - x0, y2B - yT, 'hit');
    const move = ev => {
      const px = ev.clientX - root.getBoundingClientRect().left;
      const b = bands.reduce((best, c) => Math.abs(X(c) - px) < Math.abs(X(best) - px) ? c : best, bands[0]);
      cross.setAttribute('x1', crisp(X(b))); cross.setAttribute('x2', crisp(X(b)));
      cross.style.display = '';
      const rows = [me].concat(fleets.filter(fl => fl !== me)).map(fl => {
        const c = at(fl, b);
        return {key: NAME[fl.id], on: fl === me, val: c ? pct(c.p) : '—',
                sub: c ? pct(c.lo) + '–' + pct(c.hi) + ' · n ' + c.n : 'too few flights'};
      });
      const s = gains.find(x => x.near === b);
      showTip(ev, b + ' h before departure', rows,
              s ? 'Waiting from ' + s.far + ' h to ' + s.near + ' h: ' + (s.gain >= 0 ? '+' : '') +
                  (s.gain * 100).toFixed(1) + ' points, ' + (s.per_hour * 100).toFixed(2) + ' per hour' : null);
    };
    hit.addEventListener('pointermove', move);
    hit.addEventListener('pointerdown', move);
    hit.addEventListener('pointerleave', () => { cross.style.display = 'none'; hideTip(); });
  };

  f.table = host => {
    const bands = [...new Set(fleets.flatMap(fl => fl.curve.map(c => c.band)))].sort((a, b) => b - a);
    table(host, ['Hours out'].concat(fleets.flatMap(fl => [[NAME[fl.id]], ['95% interval'], ['n']]), [['Gain / h']]),
      bands.map(b => [b + ' h'].concat(fleets.flatMap(fl => {
        const c = fl.curve.find(x => x.band === b);
        return c ? [pct(c.p), pct(c.lo) + '–' + pct(c.hi), c.n] : [null, null, null];
      }), [(() => { const s = (me.settling || []).find(x => x.near === b);
                    return s ? (s.per_hour * 100).toFixed(2) + ' pt' : null; })()])));
  };
  return f;
}

// ── Fig. 2 — which flights are the safer bet? ──────────────────────────
function routesFig(D) {
  const f = figure('routes', 'Which flights are the safer bet?');
  const R = D.routes, name = NAME[D.family], MIN = 15;
  let lead = String(R.leads.includes(48) ? 48 : R.leads[0]);
  const rowsAt = L => R.routes.map(r => ({r, o: r.out[L], b: r.back[L]}))
    .filter(x => (x.o && x.o.n >= MIN) || (x.b && x.b.n >= MIN))
    .sort((a, b) => ((b.o || b.b).p - (a.o || a.b).p) || routeName(a.r).localeCompare(routeName(b.r)));
  const sig = (c, all) => !c || !all || c.n < MIN ? '' : c.lo > all.p ? 'hi' : c.hi < all.p ? 'lo' : '';

  const describe = () => {
    const all = {o: R.all.out[lead], b: R.all.back[lead]};
    const rows = rowsAt(lead);
    if (!all.o || !all.b || !rows.length) { f.find.textContent = 'Not enough flights at this lead time yet.'; return; }
    const stand = (cls, sortDir) => rows.flatMap(x => [['outbound', x.o, all.o], ['return', x.b, all.b]]
      .filter(t => sig(t[1], t[2]) === cls).map(t => ({label: routeName(x.r) + ' ' + t[0], p: t[1].p})))
      .sort((a, b) => sortDir * (a.p - b.p)).slice(0, 3).map(t => t.label + ' (' + pct(t.p) + ')');
    const better = stand('hi', -1), worse = stand('lo', 1);
    const parts = all.b.p > all.o.p + 0.02
      ? [lead + ' hours out, a ' + name + ' tail holds on ', [pct(all.b.p)], ' of return legs but only ',
         [pct(all.o.p)], ' of outbound ones: the aircraft that flew out is the one that flies back. ']
      : [lead + ' hours out, a ' + name + ' tail holds on ', [pct(all.o.p)], ' of outbound legs and ',
         [pct(all.b.p)], ' of return ones. '];
    if (better.length) parts.push('Clearly above the fleet: ', [better.join(', ')], '. ');
    if (worse.length) parts.push('Clearly below: ', [worse.join(', ')], '.');
    if (!better.length && !worse.length) parts.push('No single route stands clear of the fleet at this lead time.');
    say(f.find, ...parts);
  };
  describe();
  f.tools.prepend(seg(R.leads.map(L => [String(L), L + ' h out']), lead, v => {
    lead = v; describe(); f.dirty = true;
    f.refresh();
    redraw();
  }, 'Lead time'));

  f.legend.append(
    key(s => rect(s, 9, 2, 8, 8, 'dot'), 'outbound, leaving the hub'),
    key(s => rect(s, 10, 3, 6, 6, 'dot open'), 'return'),
    key(s => line(s, 1, 6, 25, 6, 'whisker'), '95% interval'),
    key(s => rect(s, 9, 2, 8, 8, 'dot hi'), 'above the fleet’s rate'),
    key(s => rect(s, 9, 2, 8, 8, 'dot lo'), 'below it'));
  f.note.textContent = 'Each row is a route; the two marks are its outbound and its return flights of the last ' +
    D.meta.weeks + ' weeks, and how often the tail published at the chosen lead time flew them. The vertical ' +
    'lines are the fleet’s rate for all outbound (solid) and all return flights (dashed). A mark is coloured only ' +
    'when its whole interval lies on one side of that line; with this many marks, one or two will do so by chance. ' +
    'Directions with fewer than ' + MIN + ' flights are not drawn.';

  f.draw = (host, W) => {
    const rows = rowsAt(lead);
    if (!rows.length) { host.append(el('p', 'empty', 'Not enough flights at this lead time yet.')); return; }
    const all = {o: R.all.out[lead], b: R.all.back[lead]};
    const narrow = W < 600;
    const left = narrow ? 76 : 108, right = narrow ? 8 : 96, top = narrow ? 60 : 46, pitch = 24, bot = 36;
    const H = top + rows.length * pitch + bot;
    const root = plot(host, W, H, 'Hold rate per route, outbound and return, ' + lead + ' hours before departure');
    const x0 = left, x1 = W - right, yT = top, yB = top + rows.length * pitch;
    const cells = rows.flatMap(x => [x.o, x.b]).filter(c => c && c.n >= MIN);
    const lo = Math.max(0, Math.floor(Math.min(...cells.map(c => c.lo)) * 10) / 10);
    const hi = Math.min(1, Math.ceil(Math.max(...cells.map(c => c.hi)) * 10) / 10);
    const X = p => x0 + (p - lo) / (hi - lo) * (x1 - x0);
    const g = svg('g', null, root);
    const step = (hi - lo) * 10 > 6 && narrow ? 2 : 1;
    for (let i = Math.round(lo * 10); i <= Math.round(hi * 10); i++) {
      const x = crisp(X(i / 10)), major = (i - Math.round(lo * 10)) % step === 0;
      line(g, x, yT, x, yB, 'grid');
      line(g, x, yT - (major ? 5 : 3), x, yT, 'tick'); line(g, x, yB, x, yB + (major ? 5 : 3), 'tick');
      if (major) { text(g, x, yT - 9, i * 10 + '%', 't-tick', 'middle'); text(g, x, yB + 17, i * 10 + '%', 't-tick', 'middle'); }
    }
    text(g, (x0 + x1) / 2, H - 5, (narrow ? '' : 'published ') + 'tail flew the flight · ' + lead + ' h out', 't-title', 'middle');
    if (!narrow) text(g, x1 + 12, yT - 26, 'n out · back', 't-title');

    // the fleet's two rates, labelled on the outer side so they never collide
    const refs = [[all.o, 'ref-solid', 'all outbound'], [all.b, 'ref', 'all return']].filter(r => r[0]);
    refs.forEach(([c, cls, label], i) => {
      const x = crisp(X(c.p)), str = label + ' ' + pct(c.p);
      const leftSide = refs.length < 2 ? false : (X(refs[i][0].p) <= X(refs[1 - i][0].p));
      // a label that would leave the plot moves up a row and hangs from the edge
      const out = leftSide ? x - 5 - textW(str) < 0 : x + 5 + textW(str) > W;
      const y = yT - 26 - (out ? 14 : 0);
      line(root, x, y + 2, x, yB, cls);
      if (out) text(root, leftSide ? 2 : W - 2, y, str, 't-ann', leftSide ? 'start' : 'end');
      else text(root, x + (leftSide ? -5 : 5), y, str, 't-ann', leftSide ? 'end' : 'start');
    });

    rows.forEach((x, i) => {
      const y = top + i * pitch + pitch / 2;
      if (i) line(g, x0, crisp(y - pitch / 2), x1, crisp(y - pitch / 2), 'grid');
      text(root, x0 - 10, y + 3.5, routeName(x.r), 't-row', 'end');
      const o = x.o && x.o.n >= MIN ? x.o : null, b = x.b && x.b.n >= MIN ? x.b : null;
      if (o) line(root, X(o.lo), y - 4, X(o.hi), y - 4, 'whisker');
      if (b) line(root, X(b.lo), y + 4, X(b.hi), y + 4, 'whisker');
      if (o && b) line(root, X(o.p), y - 4, X(b.p), y + 4, 'pair');
      if (o) rect(root, X(o.p) - 4, y - 8, 8, 8, 'dot ' + sig(o, all.o));
      if (b) rect(root, X(b.p) - 3, y + 1, 6, 6, 'dot open ' + sig(b, all.b));
      if (!narrow) text(root, x1 + 12, y + 3.5, (o ? o.n : '–') + ' · ' + (b ? b.n : '–'), 't-tick');
      const hit = rect(root, 0, y - pitch / 2, W, pitch, 'hit');
      const show = ev => showTip(ev, routeName(x.r) + ' · ' + lead + ' h out', [
        {key: 'Outbound', val: o ? pct(o.p) : '—', sub: o ? pct(o.lo) + '–' + pct(o.hi) + ' · n ' + o.n : 'too few flights'},
        {key: 'Return', val: b ? pct(b.p) : '—', sub: b ? pct(b.lo) + '–' + pct(b.hi) + ' · n ' + b.n : 'too few flights'}],
        'Fleet: ' + (all.o ? pct(all.o.p) + ' outbound' : '') + (all.b ? ', ' + pct(all.b.p) + ' return' : ''));
      hit.addEventListener('pointermove', show);
      hit.addEventListener('pointerdown', show);
      hit.addEventListener('pointerleave', hideTip);
    });
    rect(root, crisp(x0), crisp(yT), x1 - x0, yB - yT, 'frame');
  };

  f.table = host => {
    const all = {o: R.all.out[lead], b: R.all.back[lead]};
    const cell = c => c ? [pct(c.p), pct(c.lo) + '–' + pct(c.hi), c.n] : [null, null, null];
    table(host, ['Route · ' + lead + ' h out', ['Outbound'], ['95% interval'], ['n'], ['Return'], ['95% interval'], ['n']],
      [['All ' + name + ' flights'].concat(cell(all.o), cell(all.b))]
        .concat(rowsAt(lead).map(x => [routeName(x.r)].concat(cell(x.o), cell(x.b)))));
  };
  return f;
}

// ── Fig. 3 — what does a plane do next? ────────────────────────────────
function turnsFig(D) {
  const f = figure('turns', 'What does a plane do next?');
  const T = D.turns, name = NAME[D.family];
  const hubs = T.hubs.filter(h => h.links.length), only = T.hubs.filter(h => h.only);
  const STRONG = 0.25;
  const share = (h, l) => l.n / h.inbound[l.i].n;
  const flight = n => 'LH' + n.flight;

  if (hubs.length) {
    const h = hubs[0];
    const best = h.links.filter(l => h.inbound[l.i].n >= 30 && l.n >= 8).sort((a, b) => share(h, b) - share(h, a))[0];
    const parts = [];
    if (h.choices) parts.push('Without knowing more, a ' + name + ' turning round at ' + h.hub + ' could take any of about ',
                              [String(Math.round(h.choices[0]))], ' flights. Knowing where it came in from narrows that to about ',
                              [String(Math.round(h.choices[1]))], '. ');
    if (best) parts.push('The firmest pairing: arrivals from ' + h.inbound[best.i].apt + ' leave for ' + h.outbound[best.o].apt + ' ',
                         [pct(share(h, best))], ' of the time (' + best.n + ' of ' + h.inbound[best.i].n + '), after about ' +
                         ground(best.turn) + ' on the ground.');
    say(f.find, ...parts);
  } else f.find.textContent = 'Not enough turnarounds recorded for this fleet yet.';

  f.legend.append(
    key(s => svg('path', {d: 'M1,9 C10,9 16,3 25,3', class: 'lk strong', 'stroke-width': 4}, s),
        'a quarter or more of that arrival’s turns'),
    key(s => svg('path', {d: 'M1,9 C10,9 16,3 25,3', class: 'lk', 'stroke-width': 2}, s), 'fewer'),
    key(s => svg('path', {d: 'M1,9 C10,9 16,3 25,3', class: 'lk next', 'stroke-width': 2}, s), 'leaves the next day'),
    el('span', 'key', 'width = number of turns'));
  const hidden = hubs.reduce((a, h) => a + h.rare, 0), long = T.hubs.reduce((a, h) => a + h.long, 0);
  f.note.textContent = 'Each strand is a turnaround of the last ' + D.meta.weeks + ' weeks: the same tail arriving on ' +
    'the flight on the left and leaving on the flight on the right at most ' + T.max_h + ' hours later. Flights sit ' +
    'at their usual local time, so a strand running down the page is a same-day turn and one running up leaves the ' +
    'next day. Hover a flight to isolate its strands. “About N flights” is the perplexity of the next flight, each ' +
    'turn scored on all the others.' +
    (hidden ? ' Not drawn: ' + plural(hidden, 'turn') + ' on flights seen fewer than five times.' : '') +
    (long ? ' ' + plural(long, 'stop') + ' longer than ' + T.max_h + ' hours ' + (long > 1 ? 'are' : 'is') + ' left to Fig. 05.' : '') +
    only.map(h => ' ' + h.hub + ' is left out: all ' + h.n + ' turns there go straight back to ' + h.only + '.').join('');

  function panel(host, h, W) {
    const narrow = W < 520;
    // the clock runs only over the hours this hub's flights use
    const times = h.inbound.concat(h.outbound).map(n => n.t);
    const h0 = Math.floor((Math.min(...times) - 30) / 180) * 3, h1 = Math.ceil((Math.max(...times) + 30) / 180) * 3;
    const gut = narrow ? 92 : 132, top = 44, hAx = clamp((h1 - h0) * 24, 330, 520), bot = 14;
    const H = top + hAx + bot;
    const root = plot(host, W, H, name + ' turnarounds at ' + h.hub + ': arriving flight to next departing flight');
    const xl = gut, xr = W - gut;
    const Y = t => top + (t - h0 * 60) / ((h1 - h0) * 60) * hAx;
    const g = svg('g', null, root);
    for (let hr = h0; hr <= h1; hr += 3) line(g, xl, crisp(Y(hr * 60)), xr, crisp(Y(hr * 60)), 'grid');
    text(root, xl - 12, 12, narrow ? 'in from' : 'arrives from', 't-title', 'end');
    text(root, xr + 12, 12, narrow ? 'out to' : 'leaves for', 't-title');
    text(root, (xl + xr) / 2, 13, h.hub + ' · ' + h.n + ' turns', 't-lab', 'middle');
    text(root, (xl + xr) / 2, 28, 'local time', 't-tick t-muted', 'middle');

    const maxN = Math.max(...h.links.map(l => l.n));
    const strands = svg('g', null, root);
    const links = h.links.slice().sort((a, b) => share(h, a) - share(h, b)).map(l => {
      const y1 = Y(h.inbound[l.i].t), y2 = Y(h.outbound[l.o].t), dx = (xr - xl) * 0.45;
      const d = 'M' + (xl + 4) + ',' + y1 + ' C' + (xl + 4 + dx) + ',' + y1 + ' ' + (xr - 4 - dx) + ',' + y2 + ' ' + (xr - 4) + ',' + y2;
      const strong = share(h, l) >= STRONG && l.n >= 5;
      const path = svg('path', {d, class: 'lk' + (strong ? ' strong' : '') + (l.d ? ' next' : ''),
                                'stroke-width': Math.max(1, l.n / maxN * 9).toFixed(1)}, strands);
      return {l, path, d};
    });
    // hour labels ride on top of the strands, boxed like contour labels
    for (let hr = h0; hr <= h1; hr += 3) {
      const y = Y(hr * 60), yy = clamp(y, top + 7, top + hAx - 7);
      boxed(root, (xl + xr) / 2 - 15, yy + 3.5, String(hr).padStart(2, '0') + ':00', 't-tick');
    }
    line(root, crisp(xl), top, crisp(xl), top + hAx, 'rail');
    line(root, crisp(xr), top, crisp(xr), top + hAx, 'rail');

    // isolate one flight's strands (hover, or tap to pin)
    let pinned = null;
    const focus = (side, idx, ev) => {
      links.forEach(k => k.path.classList.toggle('on', (side === 'i' ? k.l.i : k.l.o) === idx));
      root.classList.add('focus');
      if (!ev) return;
      const node = (side === 'i' ? h.inbound : h.outbound)[idx];
      const mine = h.links.filter(l => (side === 'i' ? l.i : l.o) === idx).sort((a, b) => b.n - a.n);
      showTip(ev, (side === 'i' ? 'In from ' : 'Out to ') + node.apt + ' · ' + flight(node) + ' · ' + clock(node.t),
        mine.slice(0, 7).map(l => {
          const other = side === 'i' ? h.outbound[l.o] : h.inbound[l.i];
          return {key: (side === 'i' ? '→ ' : '← ') + other.apt + ' ' + flight(other), val: pct(l.n / node.n),
                  sub: l.n + '× · ' + ground(l.turn) + (l.d ? ' · next day' : '')};
        }), node.n + ' turns' + (mine.length > 7 ? ', ' + (mine.length - 7) + ' rarer pairings not listed' : ''));
    };
    const clear = () => {
      hideTip();
      if (pinned) { focus(pinned[0], pinned[1]); return; }
      root.classList.remove('focus');
      links.forEach(k => k.path.classList.remove('on'));
    };
    const side = (nodes, s) => {
      const x = s === 'i' ? xl : xr, dir = s === 'i' ? -1 : 1;
      const ys = dodge(nodes.map(n => Y(n.t)), 13, top + 5, top + hAx - 1);
      nodes.forEach((n, k) => {
        const y = Y(n.t), ly = ys[k];
        svg('polyline', {points: (x + dir * 5) + ',' + y + ' ' + (x + dir * 12) + ',' + ly + ' ' + (x + dir * 15) + ',' + ly, class: 'leader'}, root);
        rect(root, x - 3.5, y - 3.5, 7, 7, 'node');
        const t = svg('text', {x: x + dir * 18, y: ly + 3.5, class: 't-row', 'text-anchor': s === 'i' ? 'end' : 'start'}, root);
        const bits = s === 'i' ? [[narrow ? '' : flight(n) + ' ', 't-muted'], [n.apt + ' ', 't-strong'], [clock(n.t), '']]
                               : [[clock(n.t) + ' ', ''], [n.apt, 't-strong'], [narrow ? '' : ' ' + flight(n), 't-muted']];
        bits.forEach(b => { const ts = svg('tspan', {class: b[1]}, t); ts.textContent = b[0]; });
        const hit = rect(root, s === 'i' ? 0 : x - 6, ly - 6.5, gut + 6, 13, 'hit');
        hit.addEventListener('pointerenter', ev => focus(s, k, ev));
        hit.addEventListener('pointermove', moveTip);
        hit.addEventListener('pointerleave', clear);
        hit.addEventListener('click', ev => {
          pinned = pinned && pinned[0] === s && pinned[1] === k ? null : [s, k];
          if (pinned) focus(s, k, ev); else clear();
        });
      });
    };
    side(h.inbound, 'i');
    side(h.outbound, 'o');
    // a strand on its own: a wide transparent twin is the hit target
    links.forEach(k => {
      const hit = svg('path', {d: k.d, class: 'lk-hit'}, strands);
      const a = h.inbound[k.l.i], b = h.outbound[k.l.o];
      const show = ev => {
        k.path.classList.add('hot');
        showTip(ev, a.apt + ' ' + flight(a) + ' → ' + b.apt + ' ' + flight(b), [
          {key: 'Turns', val: String(k.l.n), sub: k.l.d ? 'leaving the next day' : 'same day'},
          {key: 'Of these arrivals', val: pct(k.l.n / a.n), sub: 'n ' + a.n},
          {key: 'Of these departures', val: pct(k.l.n / b.n), sub: 'n ' + b.n},
          {key: 'On the ground', val: ground(k.l.turn), sub: 'median'}]);
      };
      hit.addEventListener('pointermove', show);
      hit.addEventListener('pointerleave', () => { k.path.classList.remove('hot'); hideTip(); });
    });
  }

  f.draw = (host, W) => {
    if (!hubs.length) { host.append(el('p', 'empty', 'Not enough turnarounds recorded for this fleet yet.')); return; }
    const two = hubs.length > 1 && W >= 940;
    const row = el('div', 'panels');
    host.append(row);
    hubs.forEach(h => {
      const cell = el('div', 'panel');
      row.append(cell);
      panel(cell, h, two ? Math.floor((W - 24) / 2) : W);
    });
  };

  f.table = host => {
    table(host, ['Hub', 'Arrives from', 'Flight', 'At', 'Leaves for', 'Flight', 'At', ['Turns'], ['Of arrivals'], ['Ground'], 'Day'],
      hubs.flatMap(h => h.links.slice().sort((a, b) => b.n - a.n).map(l => {
        const a = h.inbound[l.i], b = h.outbound[l.o];
        return [h.hub, a.apt, flight(a), clock(a.t), b.apt, flight(b), clock(b.t), l.n, pct(l.n / a.n), ground(l.turn),
                l.d ? 'next' : 'same'];
      })));
  };
  return f;
}

// ── Fig. 4 — who flies where? ──────────────────────────────────────────
function spreadFig(D) {
  const f = figure('spread', 'Who flies where?');
  const S = D.spread, name = NAME[D.family];
  const cellN = new Map(S.cells.map(c => [c[0] + ',' + c[1], c[2]]));
  const maxN = Math.max(1, ...S.cells.map(c => c[2]));
  const hubsSeen = [...new Set(S.routes.map(r => r.a))];
  const product = [['first', 'First'], ['allegris', 'Allegris']].filter(p => S.routes.some(r => r[p[0]] > 0));

  if (S.routes.length) {
    const often = S.routes.filter(r => r.n >= 20), busy = often.length ? often : S.routes;
    const counts = busy.map(r => r.tails).sort((a, b) => a - b);
    const med = counts[Math.floor(counts.length / 2)];
    const tight = busy.slice().sort((a, b) => a.tails - b.tails || b.n - a.n)[0];
    const parts = ['In ' + D.meta.weeks + ' weeks a typical ' + name + ' route was flown by ', [med + ' of the ' + S.fleet + ' tails'], '. '];
    const closed = busy.filter(r => r.tails <= 0.75 * med).sort((a, b) => a.tails - b.tails || b.n - a.n);
    if (med / S.fleet < 0.7 && tight) {
      const ri = S.routes.indexOf(tight);
      const top = S.cells.filter(c => c[1] === ri).sort((a, b) => b[2] - a[2])[0];
      parts.push('The fleet is split into sub-fleets: ' + routeName(tight) + ' saw only ', [tight.tails + ' tails'],
                 ', and ' + S.tails[top[0]].reg + ' alone flew ', [pct(top[2] / tight.n)], ' of it. ');
    } else if (closed.length) {
      parts.push('Most routes take whichever tail is next, but not all: ',
                 [closed.slice(0, 3).map(r => routeName(r) + ' (' + r.tails + ' tails)').join(', ')],
                 (closed.length > 1 ? ' were' : ' was') + ' flown by a subset. ');
    } else parts.push('Tails rotate through every route, so a route says little about which tail you get. ');
    const always = k => S.routes.filter(r => r.n >= 20 && r[k] >= 0.95).length;
    if (S.routes.some(r => r.allegris > 0)) parts.push('Allegris flew every flight on ', [plural(always('allegris'), 'route')], '.');
    else if (S.routes.some(r => r.first > 0) && S.routes.some(r => r.first !== null && r.first < 0.95))
      parts.push('First was sold on every flight of ', [plural(always('first'), 'route')], ', not on the rest.');
    say(f.find, ...parts);
  } else f.find.textContent = 'No flights recorded for this fleet yet.';

  const sq = (s, side, cls) => rect(s, 13 - side / 2, 6 - side / 2, side, side, cls);
  const sideOf = (v, full, top) => Math.max(2, Math.sqrt(v / top) * full);
  [1, 5, 10, 20, 40].filter(v => v <= maxN * 0.7).concat([maxN]).forEach(v => f.legend.append(
    key(s => sq(s, sideOf(v, 12, maxN), 'cell'), plural(v, 'flight'))));
  if (product.length) f.legend.append(key(s => sq(s, 12, 'cell prod'), 'every flight of the route'));
  f.note.textContent = 'Each square is one tail on one route (both directions together); its area is the number of ' +
    'flights in the last ' + D.meta.weeks + ' weeks. Tails are grouped by cabin and, like the ' +
    'routes, ordered so that tails sharing routes sit together. Top: how many different tails the route saw. Right: ' +
    'how many routes the tail flew. Routes flown fewer than three times are left out. ' +
    (product.length ? 'Bottom: the share of each route’s flights ' +
    (product.length > 1 ? 'sold with a First cabin and flown by an Allegris aircraft.' : product[0][0] === 'first' ?
     'sold with a First cabin.' : 'flown by an Allegris aircraft.') : '');

  f.draw = (host, W) => {
    if (!S.routes.length) { host.append(el('p', 'empty', 'No flights recorded for this fleet yet.')); return; }
    const narrow = W < 640;
    const left = 78, rightM = narrow ? 8 : 104, mgH = 38;
    const hubH = hubsSeen.length > 1 ? 16 : 0, top = 14 + mgH + 8 + hubH + 16;
    const ch = clamp(Math.floor(430 / S.tails.length), 18, 28);
    const cw = clamp(Math.floor((W - left - rightM) / S.routes.length), 20, 3 * ch), full = Math.min(cw, ch) - 3;
    const mw = cw * S.routes.length, x0 = left, x1 = left + mw;
    const totalW = Math.max(W, x1 + rightM);
    // rows, with a header line per cabin group
    const rows = []; let y = top, last = -1;
    S.tails.forEach((t, i) => {
      if (S.groups.length > 1 && t.group !== last) { rows.push({head: S.groups[t.group], y}); y += 22; last = t.group; }
      rows.push({t, i, y}); y += ch;
    });
    const yB = y, prodY = yB + 12, H = (product.length ? prodY + product.length * ch : yB) + 8;
    const root = plot(host, totalW, H, 'Share of each route’s flights flown by each ' + name + ' tail');
    const X = j => x0 + j * cw;
    const g = svg('g', null, root);

    // top margin: how many tails the route saw
    const mgB = 14 + mgH, MY = n => mgB - n / S.fleet * mgH;
    text(g, x0, 9, 'tails seen on the route, of ' + S.fleet, 't-title');
    [0, S.fleet].forEach(n => { line(g, x0 - 5, crisp(MY(n)), x0, crisp(MY(n)), 'tick'); text(g, x0 - 8, MY(n) + 3.5, n, 't-tick', 'end'); });
    line(g, x0, crisp(MY(S.fleet)), x1, crisp(MY(S.fleet)), 'grid');
    line(g, x0, crisp(mgB), x1, crisp(mgB), 'tick');
    S.routes.forEach((r, j) => {
      rect(g, X(j) + 2, MY(r.tails), cw - 4, mgB - MY(r.tails), 'mg');
      text(g, X(j) + cw / 2, top - 5, r.b, 't-tick t-strong', 'middle');
    });
    if (hubH) hubsSeen.forEach(hub => {
      const js = S.routes.map((r, j) => r.a === hub ? j : -1).filter(j => j >= 0);
      const a = X(js[0]) + 3, b = X(js[js.length - 1] + 1) - 3, yy = crisp(mgB + 10);
      line(g, a, yy, b, yy, 'tick');
      text(g, (a + b) / 2, yy + 11, b - a > 60 ? 'from ' + hub : hub, 't-title', 'middle');
    });

    // the matrix
    rows.forEach(row => {
      if (row.head) {
        rect(g, x0, row.y, mw, 22, 'group');
        text(g, x0 + 7, row.y + 15, cabin(row.head.seat_config) + (row.head.allegris ? ' · Allegris' : '') + ' · ' +
             plural(row.head.tails, 'tail'), 't-title t-strong');
        return;
      }
      const mine = MINE.has(row.t.reg);
      if (mine) rect(g, 0, row.y, x1, ch, 'mine');
      line(g, x0, crisp(row.y + ch), x1, crisp(row.y + ch), 'grid');
      const a = svg('a', {href: tailHref(row.t.reg)}, root);
      text(a, x0 - 8, row.y + ch / 2 + 3.5, (mine ? '★ ' : '') + row.t.reg, 't-row t-link', 'end');
      S.routes.forEach((r, j) => {
        const n = cellN.get(row.i + ',' + j);
        if (!n) return;
        const s = sideOf(n, full, maxN);
        rect(root, X(j) + cw / 2 - s / 2, row.y + ch / 2 - s / 2, s, s, 'cell');
      });
      if (!narrow) rect(g, x1 + 14, row.y + 3, row.t.routes / S.routes.length * (rightM - 30), ch - 6, 'mg');
    });
    hubsSeen.slice(1).forEach(hub => { const x = crisp(X(S.routes.findIndex(r => r.a === hub))); line(g, x, top, x, yB, 'tick'); });
    rect(root, crisp(x0), crisp(top), mw, yB - top, 'frame');
    if (!narrow) {
      const RX = n => x1 + 14 + n / S.routes.length * (rightM - 30);
      text(g, x1 + 14, top - 19, 'routes flown', 't-title');
      [0, S.routes.length].forEach(n => { line(g, crisp(RX(n)), top - 5, crisp(RX(n)), top, 'tick'); text(g, RX(n), top - 8, n, 't-tick', 'middle'); });
      line(g, crisp(RX(0)), top, crisp(RX(0)), yB, 'tick');
      line(g, crisp(RX(S.routes.length)), top, crisp(RX(S.routes.length)), yB, 'grid');
    }
    // product rows: what the route is sold with
    product.forEach(([k, label], p) => {
      const yy = prodY + p * ch;
      text(g, x0 - 8, yy + ch / 2 + 3.5, label, 't-title', 'end');
      S.routes.forEach((r, j) => {
        if (!(r[k] > 0)) return;
        const s = sideOf(r[k], full, 1);
        rect(root, X(j) + cw / 2 - s / 2, yy + ch / 2 - s / 2, s, s, 'cell prod');
      });
    });

    // hover: one overlay, the cell is found from the pointer
    const mark = rect(root, 0, 0, cw, ch, 'cell-hover');
    mark.style.display = 'none';
    const hit = rect(root, x0, top, mw, H - top, 'hit');
    const move = ev => {
      const b = root.getBoundingClientRect(), px = ev.clientX - b.left, py = ev.clientY - b.top;
      const j = clamp(Math.floor((px - x0) / cw), 0, S.routes.length - 1), r = S.routes[j];
      const row = rows.find(q => q.t && py >= q.y && py < q.y + ch);
      const prod = py >= prodY ? product[Math.floor((py - prodY) / ch)] : null;
      if (!row && !prod) { mark.style.display = 'none'; hideTip(); return; }
      mark.setAttribute('x', X(j)); mark.setAttribute('y', row ? row.y : prodY + product.indexOf(prod) * ch);
      mark.style.display = '';
      if (prod) {
        showTip(ev, routeName(r), [{key: prod[1], val: r[prod[0]] == null ? '—' : pct(r[prod[0]]), sub: 'of ' + r.n + ' flights'}]);
        return;
      }
      const n = cellN.get(row.i + ',' + j) || 0;
      showTip(ev, row.t.reg + ' on ' + routeName(r), [
        {key: 'Flights', val: n + ' of ' + r.n, sub: pct(n / r.n) + ' of the route'},
        {key: 'Even share', val: (r.n / S.fleet).toFixed(1), sub: 'flights, 1 in ' + S.fleet + ' tails'},
        {key: 'Route', val: r.tails + ' tails', sub: 'as good as ' + r.eff + ' equally used'}],
        row.t.reg + ' flew ' + plural(row.t.routes, 'route') + ' in all');
    };
    hit.addEventListener('pointermove', move);
    hit.addEventListener('pointerdown', move);
    hit.addEventListener('pointerleave', () => { mark.style.display = 'none'; hideTip(); });
  };

  f.table = host => {
    table(host, ['Route', ['Flights'], ['Tails'], ['Effective tails'], 'Most frequent tail', ['Its share'], ['First sold'], ['Allegris']],
      S.routes.map((r, j) => {
        const top = S.cells.filter(c => c[1] === j).sort((a, b) => b[2] - a[2])[0];
        const reg = S.tails[top[0]].reg;
        return [routeName(r), r.n, r.tails, r.eff, {text: reg, href: tailHref(reg)}, pct(top[2] / r.n),
                r.first == null ? null : pct(r.first), pct(r.allegris)];
      }));
  };
  return f;
}

// ── Fig. 5 — is my tail even flying? ───────────────────────────────────
function calendarFig(D) {
  const f = figure('calendar', 'Is my tail even flying?');
  const C = D.calendar, name = NAME[D.family], N = C.tails.length;
  const start = new Date(C.start + 'T00:00:00Z');
  const dateAt = i => new Date(start.getTime() + i * 864e5);
  const bin = h => h <= 6 ? 1 : h <= 12 ? 2 : h <= 18 ? 3 : 4;
  const stopAt = (t, i) => t.stops.find(s => i + 1 > s.from && i < (s.open ? C.days : s.to));

  if (N) {
    const done = C.flying.slice(0, C.complete).sort((a, b) => a - b);
    const typical = done[Math.floor(done.length / 2)];
    const out = C.tails.filter(t => t.stops.some(s => s.open));
    const longest = C.tails.flatMap(t => t.stops.filter(s => !s.open).map(s => ({t, s}))).sort((a, b) => b.s.days - a.s.days)[0];
    const busiest = C.tails.slice().sort((a, b) => b.per_day - a.per_day)[0];
    const parts = done.length ? ['On a typical day ', [typical + ' of the ' + N + ' ' + name + ' tails'], ' fly. '] : [];
    // dated, not counted: the count is only settled up to the truth horizon
    const since = t => { const s = t.stops.find(x => x.open); return s.from > 0 ? dayLabel(dateAt(Math.floor(s.from))) : 'before ' + dayLabel(start); };
    if (out.length) parts.push([out.map(t => t.reg).join(' and ')], (out.length > 1 ? ' have' : ' has') +
      ' no recorded flight since ' + out.map(since).join(' and ') + '. ');
    else parts.push('None is out of the rotation right now. ');
    if (longest) parts.push('The longest stop that has ended was ' + longest.t.reg + '’s, ', [Math.round(longest.s.days) + ' days'], '. ');
    if (busiest) parts.push('The hardest-worked tail, ' + busiest.reg + ', averages ', [busiest.per_day.toFixed(1) + ' block hours'], ' a day.');
    say(f.find, ...parts);
  } else f.find.textContent = 'No flights recorded for this fleet yet.';

  f.legend.append(el('span', 'key', 'block hours that day'));
  [[1, '≤ 6'], [2, '≤ 12'], [3, '≤ 18'], [4, '≤ 24']].forEach(([b, label]) =>
    f.legend.append(key(s => rect(s, 7, 0, 12, 12, 'q' + b), label)));
  f.legend.append(key(s => { hatch(s, 'hk'); rect(s, 1, 0, 24, 12, 'stop').setAttribute('fill', 'url(#hk)'); },
                      'no flight for ' + C.stop_days + ' days or more'),
                  key(s => rect(s, 1, 0, 24, 12, 'pending'), 'truth still pending'));
  f.note.textContent = 'One row per tail, one column per day (UTC); a cell is the block time that tail flew that ' +
    'day, from the tail Lufthansa reports as having operated each flight. Top: the tails that flew each day. ' +
    'Right: mean block hours per day over the whole period. A hatched stretch is ' + C.stop_days + ' days or more ' +
    'without a recorded flight — usually a hangar visit, though a flight missing from the feed looks the same.';

  function hatch(s, id) {
    const p = svg('pattern', {id, width: 5, height: 5, patternUnits: 'userSpaceOnUse', patternTransform: 'rotate(45)'}, svg('defs', null, s));
    line(p, 0, 0, 0, 5, 'hatch');
  }

  f.draw = (host, W) => {
    if (!N) { host.append(el('p', 'empty', 'No flights recorded for this fleet yet.')); return; }
    const narrow = W < 640;
    const left = 72, rightM = narrow ? 4 : 132, mgH = 40, top = 14 + mgH + 20, pitch = 16, ch = 13, bot = 30;
    const dw = clamp(Math.floor((W - left - rightM) / C.days), 4, 12);
    const nd = Math.min(C.days, Math.floor((W - left - rightM) / dw)), d0 = C.days - nd;   // the most recent days
    const x0 = left, mw = nd * dw, x1 = x0 + mw, yB = top + N * pitch, H = yB + bot;
    const root = plot(host, W, H, name + ' fleet calendar: block hours per tail and day');
    hatch(root, 'cal-hatch');
    const X = d => x0 + (d - d0) * dw;
    const g = svg('g', null, root);
    const pend = Math.max(C.complete, d0);
    if (pend < C.days) rect(g, X(pend), top, X(C.days) - X(pend), yB - top, 'pending');

    // top margin: tails flying per day
    const mgB = 14 + mgH, MY = n => mgB - n / N * mgH;
    text(g, x0, 9, 'tails flying, of ' + N, 't-title');
    [0, N].forEach(n => { line(g, x0 - 5, crisp(MY(n)), x0, crisp(MY(n)), 'tick'); text(g, x0 - 8, MY(n) + 3.5, n, 't-tick', 'end'); });
    line(g, x0, crisp(MY(N)), x1, crisp(MY(N)), 'grid');
    line(g, x0, crisp(mgB), x1, crisp(mgB), 'tick');
    for (let i = d0; i < C.days; i++) {
      const n = C.flying[i];
      if (n) rect(g, X(i), MY(n), dw - 1, mgB - MY(n), i < C.complete ? 'mg' : 'mg thin');
    }
    // months above, Mondays below
    const perLabel = Math.max(1, Math.ceil(46 / (7 * dw)));
    let week = 0;
    for (let i = d0; i < C.days; i++) {
      const d = dateAt(i);
      if (d.getUTCDate() === 1 || i === d0) {
        if (d.getUTCDate() === 1) line(g, crisp(X(i)), mgB, crisp(X(i)), yB, 'month');
        if (i === d0 ? dateAt(Math.min(C.days - 1, i + 6)).getUTCMonth() === d.getUTCMonth() : X(C.days) - X(i) > 26)
          text(g, X(i) + 4, top - 6, MONTHS[d.getUTCMonth()], 't-title');
      }
      if (d.getUTCDay() === 1) {
        line(g, crisp(X(i)), yB, crisp(X(i)), yB + 5, 'tick');
        if (week++ % perLabel === 0 && X(C.days) - X(i) > 30) text(g, X(i), yB + 17, dayLabel(d), 't-tick');
      }
    }
    text(g, x0 - 8, yB + 17, 'Mondays', 't-title', 'end');

    // right margin: how hard each tail works
    const uTop = Math.max(8, Math.ceil(Math.max(...C.tails.map(t => t.per_day || 0)) / 4) * 4);
    const RX = u => x1 + 16 + u / uTop * (rightM - 34);
    if (!narrow) {
      text(g, x1 + 16, 9, 'block h / day', 't-title');
      [0, uTop / 2, uTop].forEach(u => {
        line(g, crisp(RX(u)), top - 5, crisp(RX(u)), top, 'tick');
        line(g, crisp(RX(u)), top, crisp(RX(u)), yB, u ? 'grid' : 'tick');
        text(g, RX(u), top - 8, u, 't-tick', 'middle');
      });
    }

    C.tails.forEach((t, r) => {
      const y = top + r * pitch, mine = MINE.has(t.reg);
      if (mine) rect(g, 0, y, x1, pitch, 'mine');
      const a = svg('a', {href: tailHref(t.reg)}, root);
      text(a, x0 - 8, y + pitch / 2 + 3, (mine ? '★ ' : '') + t.reg, 't-row t-link', 'end');
      t.stops.forEach(s => {
        const a_ = Math.max(Math.ceil(s.from), d0), b = s.open ? C.days : Math.floor(s.to);
        if (b <= a_) return;
        const bar = rect(root, X(a_), y + 1, (b - a_) * dw - 1, ch, 'stop');
        bar.setAttribute('fill', 'url(#cal-hatch)');
        if ((b - a_) * dw >= 34) {
          const label = Math.round(s.days) + ' d' + (s.open ? ' +' : '');
          rect(root, X(a_) + 3, y + 2, label.length * 6.2 + 6, ch - 2, 'stop-tag');
          text(root, X(a_) + 6, y + 11, label, 't-tick t-strong');
        }
      });
      for (let i = d0; i < C.days; i++) if (t.hours[i] > 0) rect(root, X(i), y + 1, dw - 1, ch, 'q' + bin(t.hours[i]));
      if (t.first != null && t.first >= d0 + 1) {
        const x = X(Math.floor(t.first));
        line(root, crisp(x - 2), y + 1, crisp(x - 2), y + 1 + ch, 'first');
        if (x - x0 > 70) text(root, x - 7, y + 11, 'first seen', 't-tick t-muted', 'end');
      }
      if (!narrow && t.per_day) rect(g, RX(0), y + 3, RX(t.per_day) - RX(0), ch - 4, 'mg');
    });
    rect(root, crisp(x0), crisp(top), mw, N * pitch, 'frame');

    const mark = rect(root, 0, 0, dw, pitch, 'cell-hover');
    mark.style.display = 'none';
    const hit = rect(root, x0, top, mw, N * pitch, 'hit');
    const move = ev => {
      const b = root.getBoundingClientRect();
      const i = clamp(d0 + Math.floor((ev.clientX - b.left - x0) / dw), d0, C.days - 1);
      const r = clamp(Math.floor((ev.clientY - b.top - top) / pitch), 0, N - 1), t = C.tails[r];
      mark.setAttribute('x', X(i) - 0.5); mark.setAttribute('y', top + r * pitch);
      mark.style.display = '';
      const d = dateAt(i), h = t.hours[i], s = stopAt(t, i);
      const rows = [{key: 'Flown', val: h > 0 ? h.toFixed(1) + ' h' : 'no flight',
                     sub: i >= C.complete && !(h > 0) ? 'truth still pending' : 'block time'}];
      if (s && !(h > 0)) rows.push({key: 'Stop', val: Math.round(s.days) + ' days' + (s.open ? ' so far' : ''),
                                    sub: s.at ? 'last at ' + s.at : ''});
      showTip(ev, t.reg + ' · ' + DOW[d.getUTCDay()] + ' ' + dayLabel(d), rows,
              t.legs + ' flights, ' + (t.per_day || 0).toFixed(1) + ' block h a day in this period');
    };
    hit.addEventListener('pointermove', move);
    hit.addEventListener('pointerdown', move);
    hit.addEventListener('pointerleave', () => { mark.style.display = 'none'; hideTip(); });
  };

  f.table = host => {
    table(host, ['Tail', 'Type', ['Flights'], ['Block h / day'], ['Stops of ' + C.stop_days + ' d+'], ['Longest'], 'Now'],
      C.tails.map(t => {
        const open = t.stops.find(s => s.open);
        return [{text: t.reg, href: tailHref(t.reg)}, t.type, t.legs, t.per_day == null ? null : t.per_day.toFixed(1),
                t.stops.length, t.stops.length ? Math.round(Math.max(...t.stops.map(s => s.days))) + ' d' : null,
                open ? 'no flight for ' + Math.round(open.days) + ' d' + (open.at ? ', last at ' + open.at : '') : 'flying'];
      }));
  };
  return f;
}

// ── page ───────────────────────────────────────────────────────────────
async function init() {
  const asked = (QUERY.get('type') || 'B748').toUpperCase();
  let D;
  try {
    D = await (await fetch('/api/insights?type=' + encodeURIComponent(asked))).json();
  } catch (e) {
    $('meta').textContent = '';
    $('body').replaceChildren(el('p', 'empty', 'Failed to load.'));
    return;
  }
  FLEETS.forEach(([id, label]) => {
    const a = el('a', D.family === id ? 'is-on' : '', label);
    a.href = '/insights?type=' + id + (REG ? '&reg=' + encodeURIComponent(REG) : '');
    if (D.family === id) a.setAttribute('aria-current', 'page');
    $('fleets').append(a);
  });
  if (D.error) { $('meta').textContent = ''; $('body').replaceChildren(el('p', 'empty', D.error)); return; }
  const m = D.meta;
  say($('meta'), [NAME[D.family]], ' · ', [String(m.flights)], ' flights by ', [String(m.tails)], ' tails · ' +
      isoDay(m.from) + ' – ' + isoDay(m.to) + ' · from Lufthansa flight status');
  const body = $('body');
  body.replaceChildren(news(D));
  [trust, routesFig, turnsFig, spreadFig, calendarFig].forEach(make => body.append(make(D).root));
  redraw();
  watch(body);
}
init();
})();

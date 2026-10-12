// / — the monitor: how the site runs, and the big picture of what it logged.
//
// Draws the lead and the sections of the page chassis from /api/monitor
// (monitor_stats.py): how things stand, four figures and what has been logged,
// with the helpers and the figure scaffold of figure.js.
//
// Colour keeps the roles it has on /insights: sage is what worked or what was
// measured, grey is plan and context, terracotta is the exception (a read
// that failed, a day that should be known and is not). Nothing is told apart
// by hue alone — a failed read is solid, a missed one open, a planned one grey.
(() => {
'use strict';

const {$, targets, el, svg, text, line, textW, boxed, rect, crisp, clamp, pct, plural, plot, marks, baseline, marker, dodge,
       MONTHS, DOW, dayLabel, isoDay, say, showTip, moveTip, hideTip, seg, key, table, section, state, stats, figure, redraw, watch} = window.Fig;
const MINE = targets();
const FLEET = {B748: '747-8', A388: 'A380', B787: '787', A350: 'A350'};
const KIND = {pulse: 'pulse', probe: 'pulse and probe', watch: 'watch', far: 'far pass',
              lite: 'morning sweep', full: 'evening sweep', manual: 'run by hand'};
const CODE = {pulse: 'P', probe: 'P+', watch: 'W', far: 'F', lite: 'S', full: 'S'};
const pad2 = n => String(n).padStart(2, '0');
const num = n => Number(n).toLocaleString('en-GB');
const hhmm = min => pad2(Math.floor(min / 60) % 24) + ':' + pad2(Math.round(min) % 60);
const dow = iso => DOW[new Date(iso.slice(0, 10) + 'T00:00:00Z').getUTCDay()];
const longDay = iso => dow(iso) + ' ' + isoDay(iso.slice(0, 10));
const clockOf = iso => iso.slice(11, 16);                 // times arrive as Berlin wall time
const addDays = (iso, n) => new Date(Date.parse(iso + 'T00:00:00Z') + n * 864e5).toISOString().slice(0, 10);
const leadName = n => 'D' + (n > 0 ? '+' + n : n < 0 ? '−' + (-n) : '0');
const joinAnd = a => a.length < 2 ? a.join('') : a.slice(0, -1).join(', ') + ' and ' + a[a.length - 1];

// Berlin's wall clock for an instant (the collector's cron and the hubs run on it).
const BER = new Intl.DateTimeFormat('en-GB', {timeZone: 'Europe/Berlin', hourCycle: 'h23',
                                              year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit'});
function berlin(ms) {
  const p = {};
  BER.formatToParts(new Date(ms)).forEach(x => { p[x.type] = x.value; });
  return {day: p.year + '-' + p.month + '-' + p.day, hour: +p.hour};
}

// ── status ─────────────────────────────────────────────────────────────
const LEVEL = {ok: ['OK', 'sage'], warn: ['Note', 'gray'], bad: ['Fault', 'terra']};
const STATE = {ok: ['OK', 'sage'], stale: ['Stale', 'gray'], bad: ['Fault', 'terra']};
const chip = c => el('span', 'fp-chip fp-chip--' + c[1], c[0]);

// How things stand, for the lead: a state and a sentence each, then the collectors.
function status(D) {
  const lines = el('ul', 'status-lines');
  D.headline.forEach(h => {
    const li = el('li'), p = el('p');
    say(p, ...h.parts);
    li.append(chip(LEVEL[h.level] || LEVEL.warn), p);
    lines.append(li);
  });
  const t = el('table', 'table collectors');
  const head = el('tr');
  [['Collector', ''], ['What it does', 'wide'], ['Runs', 'wide'], ['Last run', 'num'], ['Result', '']]
    .forEach(([h, c]) => head.append(el('th', c, h)));
  t.append(head);
  D.collectors.forEach(c => {
    const tr = el('tr');
    const name = el('td', 'c-name');
    name.append(chip(STATE[c.state] || STATE.bad), document.createTextNode(c.name));
    tr.append(name, el('td', 'wide', c.does), el('td', 'wide c-runs', c.cadence),
              el('td', 'num', c.last ? c.ago + ' ago' : '—'), el('td', 'c-res', c.result));
    t.append(tr);
  });
  return [lines, t];
}
// The lead's own marker: the worst of the status lines.
function overall(D) {
  const count = level => D.headline.filter(h => h.level === level).length;
  const bad = count('bad'), warn = count('warn');
  return bad ? state('bad', plural(bad, 'fault')) : warn ? state('note', plural(warn, 'note')) : state('ok', 'All in order');
}
// When the page's rows were read, for the stamp of the top bar (Berlin time).
const STAMP = new Intl.DateTimeFormat('en-GB', {timeZone: 'Europe/Berlin', hourCycle: 'h23',
                                               day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit'});
function stamp(iso) {
  const p = {};
  STAMP.formatToParts(new Date(iso)).forEach(x => { p[x.type] = x.value; });
  return p.day + ' ' + p.month + ' · ' + p.hour + ':' + p.minute;
}

// What has been logged, as a row of key figures.
function ledger(D) {
  const L = D.ledger, since = iso => 'since ' + isoDay(iso.slice(0, 10));
  return stats([['flights logged', num(L.flights), since(L.flights_since)],
                ['flights', num(L.flights_week), 'last 7 days'],
                ['aircraft followed', num(L.aircraft), num(L.airborne) + ' in the air'],
                ['long-haul planned', num(L.legs), since(L.legs_since)],
                ['schedule lookups', num(L.lookups), since(L.lookups_since)],
                ['database', L.db_size, '']]);
}

// ── Schedule reads by flight day ───────────────────────────────────────
const READ_TEXT = {read: 'read', failed: 'asked, nothing came back', missed: 'planned, never asked', planned: 'planned'};
const COV_TEXT = {full: 'known', part: 'most flights known', thin: 'a few flights known',
                  missing: 'should have been read, is not', unread: 'not read at this distance'};
function readsFig(D) {
  const f = figure('reads', 'Schedule reads', {compact: true, kind: 'ink', note: 'By flight day'});
  const R = D.reads, days = R.days, today = R.today;
  const t0 = Date.parse(R.from), t1 = Date.parse(R.to), now = Date.parse(R.now);
  const covOf = tier => D.coverage.filter(c => c.tier === tier.id);
  const tierWords = t => t.name.split(' · ').join(' and ');
  const deep = R.tiers.find(t => t.id === 'deep'), wide = R.tiers.find(t => t.id === 'wide');

  const done = R.slots.filter(s => s.state !== 'planned' && s.state !== 'running');
  const cells = done.flatMap(s => s.cells), lost = cells.filter(c => c.state === 'failed' || c.state === 'missed');
  const parts = [];
  if (deep) parts.push('The ' + tierWords(deep) + ' are read ', [deep.per_day['1'] + ' times a day'],
                       ' one and two days ahead and once a day out to ', ['day ' + deep.horizon], '. ');
  if (wide) parts.push('The ' + tierWords(wide) + ' get ', [wide.per_day['1'] + ' reads a day'],
                       ' to day 3 and ', ['a single one on day ' + wide.horizon], ', their last. ');
  parts.push('In the last 48 hours ', lost.length ? [lost.length + ' of ' + cells.length] : ['all ' + cells.length],
             lost.length ? ' planned reads of a flight day did not happen.' : ' planned reads of a flight day happened.');
  say(f.find, ...parts);

  const sq = cls => s => rect(s, 6, 3, 14, 7, cls);
  f.legend.append(
    key(sq('rd read'), 'read'), key(sq('rd failed'), 'asked, nothing came back'),
    key(sq('rd missed'), 'planned, never asked'), key(sq('rd planned'), 'planned'),
    key(s => rect(s, 2, 1, 22, 10, 'd0'), 'the day of departure'),
    key(s => line(s, 2, 6, 24, 6, 'ref'), 'last day the pages show'),
    key(s => rect(s, 9, 2, 8, 8, 'cov full'), 'known now'),
    key(s => rect(s, 9, 2, 8, 8, 'cov part'), 'mostly'),
    key(s => rect(s, 9.5, 2.5, 7, 7, 'cov missing'), 'should be, is not'));

  // every full hour in the window, on Berlin's clock
  const hours = [];
  for (let t = Math.ceil((t0 - 30 * 36e5) / 36e5) * 36e5; t <= t1 + 30 * 36e5; t += 36e5) hours.push(Object.assign({t}, berlin(t)));
  const midnight = {};
  hours.forEach(h => { if (h.hour === 0 && !(h.day in midnight)) midnight[h.day] = h.t; });

  f.draw = (host, W) => {
    // a phone shows the last day and the next half, anything wider two days and one
    const tight = W < 720, roomy = W >= 900;
    const a0 = tight ? Math.max(t0, now - 24 * 36e5) : t0, a1 = tight ? Math.min(t1, now + 12 * 36e5) : t1;
    const L = roomy ? 190 : tight ? 100 : 150, famW = tight ? 40 : 54, RM = 2 * famW + (tight ? 6 : 12);
    const pw = W - L - RM, rh = 15, top = 40, gap = 50, ph = days.length * rh;
    const H = top + R.tiers.length * ph + (R.tiers.length - 1) * gap + 40;
    const s = plot(host, W, H, 'Which flight days each collector run read over the last two days, per fleet group, and what is known about each day now.');
    const x = t => L + (clamp(t, a0, a1) - a0) / (a1 - a0) * pw;
    const hits = [];

    R.tiers.forEach((tier, ti) => {
      const y0 = top + ti * (ph + gap), y1 = y0 + ph;
      const rowY = i => y0 + (days.length - 1 - i) * rh;              // later days sit higher
      const row = Object.fromEntries(days.map((d, i) => [d, i]));

      // the day of departure itself: a diagonal of shaded cells
      days.forEach((d, i) => {
        const a = midnight[d], b = midnight[addDays(d, 1)];
        if (a == null || b == null || b <= a0 || a >= a1) return;
        rect(s, x(a), rowY(i), x(b) - x(a), rh, 'd0');
      });
      days.forEach((d, i) => { if (i) line(s, L, crisp(rowY(i) + rh), L + pw, crisp(rowY(i) + rh), 'grid'); });
      hours.forEach(h => {
        if (h.t <= a0 || h.t >= a1 || h.hour % 6) return;
        line(s, crisp(x(h.t)), y0, crisp(x(h.t)), y1, h.hour ? 'grid' : 'month');
      });
      // the last day the pages show (day 4), climbing a row every midnight
      const stair = [];
      Object.keys(midnight).sort().forEach(d => {
        const a = midnight[d], b = midnight[addDays(d, 1)], i = row[addDays(d, 4)];
        if (b == null || i == null || b <= a0 || a >= a1) return;
        stair.push([x(a), rowY(i)], [x(b), rowY(i)]);
      });
      if (stair.length) svg('path', {d: 'M' + stair.map(p => p[0].toFixed(1) + ' ' + crisp(p[1])).join('L'), class: 'ref', fill: 'none'}, s);

      // what each run read
      R.slots.forEach(slot => slot.cells.forEach(c => {
        const i = row[c.day];
        if (c.tier !== tier.id || i == null) return;
        const a = Date.parse(c.from), b = Date.parse(c.to);
        if (b < a0 || a > a1) return;
        const xa = x(a), w = Math.max(3, x(b) - xa);
        rect(s, xa, rowY(i) + 3, w, rh - 6, 'rd ' + c.state);
        hits.push({x: xa - 2, y: rowY(i), w: Math.max(w + 4, 8), h: rh, slot, c, tier});
      }));
      marks(s, L, y0, pw, ph);

      // rows: the flight day, its distance from today, the planned reads a day
      text(s, 2, y0 - 22, tier.name, 't-lab');
      text(s, L - 8, y0 - 8, 'reads a day', 't-title', 'end');
      days.forEach((d, i) => {
        const lead = Math.round((Date.parse(d) - Date.parse(today)) / 864e5), y = rowY(i) + rh - 4;
        text(s, 2, y, roomy ? longDay(d) : dow(d) + ' ' + +d.slice(8), lead ? 't-row' : 't-strong');
        if (!tight) text(s, L - 62, y, leadName(lead), 't-tick' + (lead ? ' t-muted' : ''), 'end');
        const n = tier.per_day[String(lead)];
        text(s, L - 8, y, n ? n + '×' : '—', 't-tick' + (n ? '' : ' t-muted'), 'end');
      });

      // what is known now, per fleet
      covOf(tier).forEach((c, k) => {
        const cx = L + pw + (tight ? 8 : 14) + k * famW;
        text(s, cx + famW - 6, y0 - 8, FLEET[c.id] || c.id, 't-title', 'end');
        c.cells.forEach(cell => {
          const i = row[cell.day];
          if (i == null) return;
          const y = rowY(i);
          if (cell.state === 'unread') line(s, cx + 1, crisp(y + rh / 2), cx + 8, crisp(y + rh / 2), 'cov-none');
          else rect(s, cx + (cell.state === 'missing' || cell.state === 'thin' ? 0.5 : 0), y + 3 + (cell.state === 'missing' || cell.state === 'thin' ? 0.5 : 0),
                    cell.state === 'missing' || cell.state === 'thin' ? 8 : 9, cell.state === 'missing' || cell.state === 'thin' ? 8 : 9, 'cov ' + cell.state);
          if (cell.state !== 'unread') text(s, cx + famW - 6, y + rh - 4, cell.legs, 't-tick', 'end');
          hits.push({x: cx - 2, y, w: famW, h: rh, cov: c, cell});
        });
      });
      if (ti === 0 && !tight) text(s, L + pw + RM, y0 - 22, 'flights with a tail now', 't-title', 'end');
    });

    // the clock, under the last panel
    const yb = H - 40 + 4;
    hours.forEach(h => {
      if (h.t <= a0 || h.t >= a1 || h.hour % 6) return;
      if (h.hour === 0) text(s, x(h.t) + 4, yb + 24, longDay(h.day), 't-row');
      else if (roomy || h.hour === 12) text(s, x(h.t), yb + 10, pad2(h.hour), 't-tick t-muted', 'middle');
    });
    line(s, crisp(x(now)), top - 12, crisp(x(now)), H - 40, 'now');
    boxed(s, x(now) - 9, top - 16, 'now', 't-ann');

    // hover
    const hover = rect(s, 0, 0, 0, 0, 'cell-hover');
    hover.style.display = 'none';
    hits.forEach(h => {
      const hit = rect(s, h.x, h.y, h.w, h.h, 'hit');
      hit.addEventListener('pointerenter', ev => {
        hover.setAttribute('x', crisp(h.x)); hover.setAttribute('y', crisp(h.y));
        hover.setAttribute('width', h.w); hover.setAttribute('height', h.h);
        hover.style.display = '';
        if (h.cov) {
          const cell = h.cell, name = FLEET[h.cov.id] || h.cov.id;
          showTip(ev, name + ' · ' + longDay(cell.day) + ' (' + leadName(cell.lead) + ')', [
            {key: 'flights', val: String(cell.legs), sub: 'of about ' + Math.round(h.cov.usual) + ' on a usual day'},
            {key: 'newest', val: cell.newest ? clockOf(cell.newest) : '—', sub: cell.newest ? longDay(cell.newest) : ''}],
            COV_TEXT[cell.state]);
        } else {
          const c = h.c, slot = h.slot, run = slot.run;
          showTip(ev, clockOf(slot.at) + ' ' + KIND[slot.kind] + ' · ' + longDay(slot.at), [
            {key: h.tier.name, val: longDay(c.day), sub: leadName(c.lead) + ' then', on: true},
            {key: 'this day', val: c.asked ? c.found + ' of ' + c.asked : '—', sub: READ_TEXT[c.state]},
            {key: 'whole run', val: run ? (run.found || 0) + ' of ' + (run.total || 0) : '—',
             sub: run ? 'started ' + clockOf(run.start) : slot.state === 'planned' ? 'not yet due' : 'did not start'}],
            run && run.detail ? run.detail : null);
        }
      });
      hit.addEventListener('pointermove', moveTip);
      hit.addEventListener('pointerleave', () => { hover.style.display = 'none'; hideTip(); });
    });
  };

  f.table = host => table(host, ['Run', 'Pass', 'Outcome', ['Found', 'r'], ...R.tiers.map(t => t.name + ' days')],
    R.slots.map(s => [longDay(s.at) + ' ' + clockOf(s.at), KIND[s.kind], s.state,
                      s.run ? (s.run.found || 0) + ' of ' + (s.run.total || 0) : null,
                      ...R.tiers.map(t => s.cells.filter(c => c.tier === t.id)
                        .map(c => leadName(c.lead) + (c.state === 'failed' ? ' failed' : c.state === 'missed' ? ' missed' : '')).join(', ') || null)]));
  f.note.textContent = 'Each mark is one flight day read by one run of the schedule reader: across, when the run asked ' +
    '(Berlin time); up, the flight day it asked about. The shaded diagonal is the day of departure, so a mark’s height ' +
    'above it is how far ahead the read was. A run asks the nearest days first, which is why a sweep’s marks step to the right. ' +
    '“Reads a day” is the plan for a day at that distance from today. On the right, the flights of each fleet that have a tail ' +
    'now, against the fleet’s median day of the last four weeks. The pages show the plan up to day 4; what is read further ' +
    'out is kept until it has been scored. Short-haul legs that only connect two long-haul flights are read too and not drawn.';
  return f;
}

// ── Collector runs on the clock ────────────────────────────────────────
const RUN_TEXT = {ok: 'went through', cut: 'stopped early or partly blocked', failed: 'nothing came back',
                  lost: 'never finished', running: 'running', skipped: 'did not run'};
function recordFig(D) {
  const f = figure('record', 'Collector runs', {compact: true, kind: 'ink', field: 'tint', note: 'Last ' + D.record.days + ' days'});
  const C = D.record, N = C.days;
  const first = Date.parse(C.first + 'T00:00:00Z');
  const dayIso = i => new Date(first + i * 864e5).toISOString().slice(0, 10);
  const slotMin = st => +st.at.slice(0, 2) * 60 + +st.at.slice(3);
  const wrong = C.slots.reduce((a, st) => a + st.bad, 0), all = C.slots.reduce((a, st) => a + st.n, 0);
  const night = C.slots.filter(st => slotMin(st) >= 120 && slotMin(st) < 390);
  const nightBad = night.reduce((a, st) => a + st.bad, 0);
  const worst = C.slots.reduce((a, st) => st.bad / Math.max(st.n, 1) > a.bad / Math.max(a.n, 1) ? st : a, C.slots[0]);
  const parts = ['In the last ' + N + ' days ', [wrong + ' of ' + all], ' planned runs went wrong'];
  if (wrong) {
    parts.push(', ', [nightBad + ' of them'], ' in the ' + night.length + ' slots between 02:00 and 06:30. The worst slot is ',
               [worst.at], ': ' + worst.bad + ' of ' + worst.n);
    if (worst.streak > 1) parts.push(', the last ' + worst.streak + ' in a row');
  }
  parts.push('.');
  say(f.find, ...parts);

  const bar = cls => s => rect(s, 5, 3, 16, 7, cls);
  f.legend.append(key(bar('run ok'), 'went through'), key(bar('run cut'), 'stopped early or a tenth blocked'),
                  key(bar('run failed'), 'nothing came back'), key(s => rect(s, 9.5, 2.5, 7, 7, 'run skipped'), 'did not run'),
                  key(s => rect(s, 10, 1, 6, 10, 'slotbar'), 'share of the slot’s runs that went wrong'));

  f.draw = (host, W) => {
    const tight = W < 680;
    const L = tight ? 58 : 86, RM = tight ? 8 : 96, pw = W - L - RM, rh = 12, mTop = 58, top = 26 + mTop + 22, ph = N * rh;
    const H = top + ph + 30;
    const s = plot(host, W, H, 'Every run of the schedule reader over the last ' + N + ' days, by time of day, and how each ended.');
    const x = min => L + min / 1440 * pw;
    const rowY = i => top + i * rh;                               // newest day at the bottom

    for (let h = 0; h <= 24; h += 2) {
      if (h % 24) line(s, crisp(x(h * 60)), top, crisp(x(h * 60)), top + ph, 'grid');     // the marks hold the edges
      if (!tight || h % 4 === 0) text(s, x(h * 60), top + ph + 14, pad2(h % 24), 't-tick t-muted', 'middle');
    }
    text(s, L + pw, top + ph + 27, 'time of day, Berlin', 't-title', 'end');
    for (let i = 0; i < N; i++) {
      const d = dayIso(i), wd = new Date(d + 'T00:00:00Z').getUTCDay();
      if (wd === 1) {
        line(s, L, crisp(rowY(i)), L + pw, crisp(rowY(i)), 'grid');
        if (i < N - 1) text(s, L - 8, rowY(i) + rh - 2, (tight ? '' : 'Mon ') + isoDay(d), 't-row', 'end');   // the last row says "today"
      }
    }
    text(s, L - 8, rowY(N - 1) + rh - 2, 'today', 't-strong', 'end');
    marks(s, L, top, pw, ph);

    // above: how often each slot went wrong, and which pass it is
    const base = 26 + mTop;
    baseline(s, L, L + pw, base);
    text(s, L + pw, 12, tight ? 'share of runs that went wrong' : 'share of each slot’s runs that went wrong', 't-title', 'end');
    [0.25, 0.5].forEach(v => {
      line(s, L, crisp(base - v * mTop * 2), L + pw, crisp(base - v * mTop * 2), 'grid');
      text(s, L - 8, base - v * mTop * 2 + 3, pct(v), 't-tick t-muted', 'end');
    });
    C.slots.forEach((st, i) => {
      const cx = x(slotMin(st)), share = st.n ? st.bad / st.n : 0;
      line(s, crisp(cx), base, crisp(cx), base + 5, 'tick');
      if (!tight) text(s, cx, base + 16, CODE[st.kind], 't-tick', 'middle');
      if (share > 0) rect(s, cx - 3, base - Math.min(share, 0.5) * mTop * 2, 6, Math.min(share, 0.5) * mTop * 2, 'slotbar');
      if (st === worst && st.bad) text(s, cx + 7, base - Math.min(share, 0.5) * mTop * 2 + 9, st.bad + ' of ' + st.n, 't-ann');
    });

    // the runs
    const hits = [];
    C.runs.forEach(r => {
      const xa = x(r.s), w = Math.max(2.5, x(r.e) - xa), y = rowY(r.day) + 2;
      rect(s, xa, y, w, rh - 4, 'run ' + (r.state === 'lost' ? 'failed' : r.state));
      hits.push({x: xa - 2, y: rowY(r.day), w: Math.max(w + 4, 8), r});
    });
    C.skipped.forEach(k => {
      rect(s, x(k.s) + 0.5, rowY(k.day) + 2.5, rh - 5, rh - 5, 'run skipped');
      hits.push({x: x(k.s) - 2, y: rowY(k.day), w: rh, r: Object.assign({state: 'skipped', e: k.s}, k)});
    });
    line(s, crisp(x(C.now)), rowY(N - 1) - 3, crisp(x(C.now)), rowY(N - 1) + rh + 3, 'now');

    // on the right: the day's lookups, and the blocked ones
    if (!tight) {
      const top1 = Math.max(1, ...C.per_day.map(d => d.total));
      const bx = L + pw + 12, bw = RM - 16;
      text(s, bx + bw, top - 8, 'lookups a day', 't-title', 'end');
      C.per_day.forEach((d, i) => {
        rect(s, bx, rowY(i) + 3, d.found / top1 * bw, rh - 6, 'mg');
        rect(s, bx + d.found / top1 * bw, rowY(i) + 3, d.blocked / top1 * bw, rh - 6, 'mg bad');
      });
      line(s, Math.round(bx), top, Math.round(bx), top + ph, 'base');
      text(s, bx + bw, top + ph + 14, num(top1), 't-tick t-muted', 'end');
    }

    const hover = rect(s, 0, 0, 0, 0, 'cell-hover');
    hover.style.display = 'none';
    hits.forEach(h => {
      const hit = rect(s, h.x, h.y, h.w, rh, 'hit'), r = h.r;
      hit.addEventListener('pointerenter', ev => {
        hover.setAttribute('x', crisp(h.x)); hover.setAttribute('y', crisp(h.y));
        hover.setAttribute('width', h.w); hover.setAttribute('height', rh);
        hover.style.display = '';
        const d = C.per_day[r.day];                                 // r.at, r.kind: the slot as it was that day
        showTip(ev, longDay(dayIso(r.day)) + ' · ' + (r.kind ? r.at + ' ' + KIND[r.kind] : KIND.manual), [
          {key: 'ran', val: r.state === 'skipped' ? '—' : hhmm(r.s) + '–' + hhmm(r.e), sub: r.state === 'skipped' ? '' : (r.e - r.s) + ' min'},
          {key: 'found', val: r.state === 'skipped' ? '—' : r.found + ' of ' + r.total, sub: r.blocked ? r.blocked + ' blocked' : ''},
          {key: 'that day', val: num(d.found) + ' of ' + num(d.total), sub: d.blocked ? d.blocked + ' blocked' : ''}],
          r.detail || RUN_TEXT[r.state]);
      });
      hit.addEventListener('pointermove', moveTip);
      hit.addEventListener('pointerleave', () => { hover.style.display = 'none'; hideTip(); });
    });
  };

  const reads = k => ({pulse: '747-8 · A380, days 1–2', probe: '747-8 · A380, days 1–2 and 5–9', watch: 'today’s next departures',
                       far: 'days 3–4 (747-8 · A380 also 1–2)', lite: 'days −2 to 1 (long-haul fleets to 2)',
                       full: 'days −2 to 2 (long-haul fleets to 3)'})[k];
  f.table = host => table(host, ['Slot', 'Pass', 'Reads', ['Runs', 'r'], ['Went wrong', 'r'], ['In a row now', 'r'], 'Last good'],
    C.slots.map(st => [st.at, KIND[st.kind], reads(st.kind), st.n, st.bad, st.streak || null, st.last_ok ? isoDay(st.last_ok) : null]));
  f.note.textContent = 'One bar per run, as long as the run took; a day runs across, the days go down to today. The reader has ' +
    '15 slots a day and starts each after a random wait of up to half an hour. P is a pulse (747-8 and A380, one and two days ' +
    'ahead; the 22:00 one, P+, also probes days 5 to 9), W a watch (the next hours’ departures), F the far pass (days 3 and 4), ' +
    'S a sweep (two days back to two or three ahead, every fleet). A run “went wrong” when nothing came back, when it was ' +
    'stopped after ten blocked lookups in a row, when a tenth of it was blocked, or when it did not start. ' +
    'Lufthansa’s site decides what gets blocked; nothing here says why.' +
    (C.changes || []).map(ch => ' The plan changed on ' + isoDay(ch.day) + ': ' +
      ch.slots.map(c => 'the ' + c.was.at + ' ' + KIND[c.was.kind] + ' became the ' + c.now.at + ' ' + KIND[c.now.kind]).join(', ') +
      '. A slot is a place in the day, so its share counts the runs before and after.').join('');
  return f;
}

// ── Hub departures by time of day ──────────────────────────────────────
function hubsFig(D) {
  const f = figure('hubs', 'Hub departures', {compact: true, note: 'By time of day'});
  const hubs = D.hubs.hubs, FROM = 16, TO = 96, NB = TO - FROM;      // 04:00 to midnight, in quarter-hours
  let hub = hubs[0];
  const binAt = b => hhmm(b * 15);
  const level = n => n >= 10 ? 4 : n >= 6 ? 3 : n >= 3 ? 2 : n >= 1 ? 1 : 0;
  const describe = () => {
    const h = hub, p = h.profile;
    const parts = [h.name + ' sends off a median of ', [num(Math.round(h.median)) + ' flights a day'], ', ',
                   [pct(h.long_share)], ' of them long-haul, in waves that follow the waves of arrivals. The busiest quarter-hour starts at ',
                   [binAt(h.busiest)], ' (' + Math.round(p.dep.med[h.busiest]) + ' departures on a median day).'];
    if (h.open) parts.push(' Nothing left before ' + binAt(h.open[0]) + ' or after ' + binAt(h.open[1] + 1) + ' on nine days in ten.');
    parts.push(' Quietest day: ' + longDay(h.low.day) + ' (' + h.low.n + '); busiest: ' + longDay(h.high.day) + ' (' + h.high.n + ').');
    say(f.find, ...parts);
  };
  describe();
  f.tools.append(seg(hubs.map(h => [h.id, h.name]), hub.id, v => {
    hub = hubs.find(h => h.id === v); describe(); f.dirty = true; redraw();
    f.refresh();
  }, 'Hub'));
  const sq = cls => s => rect(s, 8, 1, 10, 10, cls);
  f.legend.append(key(sq('q1'), '1–2 departures in a quarter-hour'), key(sq('q2'), '3–5'), key(sq('q3'), '6–9'), key(sq('q4'), '10 or more'),
                  key(s => { rect(s, 6, 1, 6, 10, 'range'); rect(s, 6, 5, 6, 6, 'arr'); rect(s, 14, 5, 6, 6, 'arr long'); }, 'arrivals, long-haul'),
                  key(s => { rect(s, 6, 1, 6, 10, 'range'); rect(s, 6, 1, 6, 6, 'dep'); rect(s, 14, 1, 6, 6, 'dep long'); }, 'departures, long-haul'));

  f.draw = (host, W) => {
    const h = hub, p = h.profile, n = h.days;
    const tight = W < 700;
    const L = tight ? 58 : 86, RM = tight ? 8 : 104, pw = W - L - RM, bw = pw / NB, rh = 6, half = 52;
    const top = 22, mid = top + half, y0 = mid + half + 14, ph = n * rh, H = y0 + ph + 30;
    const s = plot(host, W, H, 'Departures at ' + h.name + ' by day and quarter-hour, with the median day’s arrivals and departures above.');
    const x = b => L + (b - FROM) * bw;
    const firstDay = Date.parse(h.first + 'T00:00:00Z');
    const dayIso = i => new Date(firstDay + i * 864e5).toISOString().slice(0, 10);

    // above: a median day — arrivals up, departures down, the busy tenth behind
    const peak = Math.max(1, ...p.arr.hi.slice(FROM, TO), ...p.dep.hi.slice(FROM, TO));
    const step = peak > 16 ? 10 : 5, sc = (half - 4) / peak;
    for (let v = step; v <= peak; v += step) {
      [mid - v * sc, mid + v * sc].forEach(y => line(s, L, crisp(y), L + pw, crisp(y), 'grid'));
      text(s, L - 8, mid - v * sc + 3, v, 't-tick t-muted', 'end');
      text(s, L - 8, mid + v * sc + 3, v, 't-tick t-muted', 'end');
    }
    for (let b = FROM; b < TO; b++) {
      const xa = x(b) + 0.5, w = Math.max(1, bw - 1);
      rect(s, xa, mid - p.arr.hi[b] * sc, w, p.arr.hi[b] * sc, 'range');
      rect(s, xa, mid - p.arr.med[b] * sc, w, p.arr.med[b] * sc, 'arr');
      rect(s, xa, mid - p.arr_long.med[b] * sc, w, p.arr_long.med[b] * sc, 'arr long');
      rect(s, xa, mid, w, p.dep.hi[b] * sc, 'range');
      rect(s, xa, mid, w, p.dep.med[b] * sc, 'dep');
      rect(s, xa, mid, w, p.dep_long.med[b] * sc, 'dep long');
    }
    baseline(s, L, L + pw, mid);
    if (tight) {
      text(s, L, top - 4, 'arrivals', 't-title');
      text(s, L + 12, mid + half + 6, 'departures', 't-title');      // clear of the registration mark below
    } else {
      text(s, 2, mid - 8, 'arrivals', 't-title');
      text(s, 2, mid + 16, 'departures', 't-title');
    }
    text(s, L + pw, top - 4, tight ? 'median day, per ¼ h' : 'a median day, per quarter-hour', 't-title', 'end');

    // below: every day's departures
    for (let hr = 4; hr <= 24; hr += 2) {
      if (hr > 4 && hr < 24) line(s, crisp(x(hr * 4)), y0, crisp(x(hr * 4)), y0 + ph, 'grid');
      if (hr < 24 && (!tight || hr % 4 === 0)) text(s, x(hr * 4), y0 + ph + 14, pad2(hr), 't-tick t-muted', 'middle');
    }
    text(s, L + pw, y0 + ph + 27, 'take-off time, Berlin', 't-title', 'end');
    for (let i = 0; i < n; i++) {
      const d = dayIso(i), wd = new Date(d + 'T00:00:00Z').getUTCDay(), y = y0 + i * rh;
      if (wd === 1) {
        line(s, L - 4, crisp(y), L, crisp(y), 'tick');
        if (i < n - 1 && (i % 2 === 0 || n < 40)) text(s, L - 8, y + 6, (tight ? '' : 'Mon ') + isoDay(d), 't-row', 'end');   // the last row says "today"
      }
      const row = h.dep[i];
      for (let b = FROM; b < TO; b++) {
        const lv = level(row[b]);
        if (lv) rect(s, x(b), y, bw + 0.2, rh, 'q' + lv);
      }
    }
    text(s, L - 8, y0 + ph - 1, 'today', 't-strong', 'end');
    marks(s, L, y0, pw, ph);

    // on the right: each day's total
    if (!tight) {
      const done = h.totals.dep.slice(0, n - 1), lo = Math.min(...done), hi = Math.max(...done);
      const a = Math.floor(lo / 20) * 20, b = Math.ceil(hi / 20) * 20 || 20;
      const tx = L + pw + 14, tw = RM - 22, tX = v => tx + (clamp(v, a, b) - a) / Math.max(b - a, 1) * tw;
      text(s, tx + tw, y0 - 6, 'departures a day', 't-title', 'end');
      [a, (a + b) / 2, b].forEach(v => {
        line(s, crisp(tX(v)), y0, crisp(tX(v)), y0 + ph, 'grid');
        text(s, tX(v), y0 + ph + 14, v, 't-tick t-muted', 'middle');
      });
      line(s, crisp(tX(h.median)), y0, crisp(tX(h.median)), y0 + ph, 'ref');
      done.forEach((v, i) => rect(s, tX(v) - 2, y0 + i * rh + 1, 4, 4, 'day-dot'));
      baseline(s, tx, tx + tw, y0 + ph);
    }

    // hover
    const mark = rect(s, 0, 0, bw, rh, 'cell-hover');
    mark.style.display = 'none';
    const hit = rect(s, L, y0, pw, ph, 'hit');
    hit.addEventListener('pointermove', ev => {
      const box = s.getBoundingClientRect();
      const b = clamp(Math.floor((ev.clientX - box.left - L) / bw) + FROM, FROM, TO - 1);
      const i = clamp(Math.floor((ev.clientY - box.top - y0) / rh), 0, n - 1);
      mark.setAttribute('x', crisp(x(b))); mark.setAttribute('y', crisp(y0 + i * rh));
      mark.style.display = '';
      showTip(ev, longDay(dayIso(i)) + ' · ' + binAt(b) + '–' + binAt(b + 1), [
        {key: 'departures', val: String(h.dep[i][b]), sub: 'median ' + p.dep.med[b] + ', busy day ' + p.dep.hi[b], on: true},
        {key: 'arrivals', val: '', sub: 'median ' + p.arr.med[b] + ' in this quarter-hour'},
        {key: 'the day', val: String(h.totals.dep[i]), sub: i === n - 1 ? 'so far' : 'departures, ' + h.totals.arr[i] + ' arrivals'}]);
    });
    hit.addEventListener('pointerleave', () => { mark.style.display = 'none'; hideTip(); });
  };

  f.table = host => table(host, ['Day', ['Departures', 'r'], ['Arrivals', 'r']],
    hub.totals.dep.map((v, i) => [longDay(new Date(Date.parse(hub.first + 'T00:00:00Z') + i * 864e5).toISOString().slice(0, 10)) +
                                  (i === hub.days - 1 ? ' (so far)' : ''), v, hub.totals.arr[i]]).reverse());
  f.note.textContent = 'From ADS-B: the aircraft of the Lufthansa group this site follows, by the quarter-hour they left the ground ' +
    '(not the gate) or touched down, Berlin time. Above, a median day of the last eight weeks: the solid bar is the median of ' +
    'each quarter-hour, the pale one behind it the busy day (90th percentile), the dark part the long-haul types. Below, every ' +
    'day’s departures; a delayed wave shows as a smear to the right. Flights the detector flagged for review are left out, ' +
    'so the counts run a little low, and today is still filling.';
  return f;
}

// ── Aircraft utilisation ───────────────────────────────────────────────
function useFig(D) {
  const f = figure('use', 'Aircraft utilisation', {compact: true, field: 'tint', note: 'Last ' + D.use.days + ' days'});
  const U = D.use, pts = U.points;
  const byType = Object.fromEntries(U.types.map(t => [t.type, t]));
  const longest = U.types[0], busiest = U.types.reduce((a, t) => t.sectors > a.sectors ? t : a, U.types[0]);
  const jumbo = byType.B748, bus = byType.A320;
  const between = pts.filter(p => p.sectors >= 2 && p.sectors < 3.5).length;
  const parts = [];
  if (jumbo && bus) parts.push('A 747-8 is in the air ', [jumbo.hours.toFixed(1) + ' hours a day'], ' in ' + jumbo.sectors.toFixed(1) +
                               ' flights; an A320 ', [bus.hours.toFixed(1) + ' hours'], ' in ' + bus.sectors.toFixed(1) + '. ');
  parts.push('Between the two worlds there is almost nothing: ', [between + ' of ' + pts.length + ' aircraft'], ' averaged 2 to 3.5 flights a day. ');
  if (longest) parts.push('The longest days belong to the ' + longest.type + ' (' + longest.hours.toFixed(1) + ' h), the most flights to the ' +
                          busiest.type + ' (' + busiest.sectors.toFixed(1) + ' a day). ');
  parts.push([U.idle.length + ' aircraft'], ' marked active did not fly at all.');
  say(f.find, ...parts);
  f.legend.append(key(s => rect(s, 10, 3, 6, 6, 'pt this'), 'the four fleets on the other pages'),
                  key(s => rect(s, 10.5, 3.5, 5, 5, 'pt ctx'), 'the rest of the fleet'),
                  key(s => marker(s, 'diamond', 13, 6, 4, 'med'), 'a type’s median aircraft'),
                  key(s => line(s, 2, 6, 24, 6, 'ref'), 'hours per flight'));

  f.draw = (host, W) => {
    const tight = W < 660;
    const L = tight ? 34 : 54, RM = tight ? 10 : 20, top = 26, ph = tight ? 360 : 430, pw = W - L - RM, H = top + ph + 44;
    const s = plot(host, W, H, 'Every active aircraft by flights a day and flight hours a day over the last ' + U.days + ' days.');
    const xMax = Math.max(7, Math.ceil(Math.max(...pts.map(p => p.sectors)))), yMax = Math.max(16, Math.ceil(Math.max(...pts.map(p => p.hours)) / 2) * 2);
    const x = v => L + v / xMax * pw, y = v => top + ph - v / yMax * ph;
    for (let v = 0; v <= xMax; v++) {
      line(s, crisp(x(v)), top + ph, crisp(x(v)), top + ph + 6, 'tick');
      text(s, x(v), top + ph + 18, v, 't-tick t-muted', 'middle');
    }
    for (let v = 2; v <= yMax; v += 2) {
      line(s, L, crisp(y(v)), L + pw, crisp(y(v)), 'grid');
      text(s, L - 8, y(v) + 3, v, 't-tick t-muted', 'end');
    }
    text(s, L - 8, y(0) + 3, 0, 't-tick t-muted', 'end');
    text(s, L + pw, top + ph + 34, 'flights a day', 't-title', 'end');
    text(s, 2, top - 12, 'flight hours a day', 't-title');
    // lines of equal flight length
    [1, 2, 4, 8, 12].forEach(k => {
      const xe = Math.min(xMax, yMax / k), ye = xe * k;
      line(s, x(0), y(0), x(xe), y(ye), 'ref');
      const lab = k + (tight ? ' h' : ' h a flight'), atTop = ye >= yMax - 1e-9;
      boxed(s, atTop ? x(xe) + 6 : x(xe) - textW(lab) - 8, atTop ? y(ye) + 14 : y(ye) - 6, lab, 't-tick t-muted');
    });
    baseline(s, L, L + pw, top + ph);

    pts.forEach(p => {
      const mine = MINE.has(p.reg);
      rect(s, x(p.sectors) - (mine ? 4 : 2.5), y(p.hours) - (mine ? 4 : 2.5), mine ? 8 : 5, mine ? 8 : 5,
           'pt ' + (mine ? 'mine' : p.family ? 'this' : 'ctx'));
      if (mine) boxed(s, x(p.sectors) + 8, y(p.hours) + 4, '★ ' + p.reg, 't-ann');
    });

    // each type's median aircraft, named where no aircraft is: the long-haul
    // types beside their cluster, the short-haul ones in a row above theirs
    const far = U.types.filter(t => t.sectors < 2.5).sort((a, b) => y(a.hours) - y(b.hours));
    const near = U.types.filter(t => t.sectors >= 2.5).sort((a, b) => a.sectors - b.sectors);
    const ys = dodge(far.map(t => y(t.hours)), 15, top + 10, top + ph - 8);
    far.forEach((t, i) => {
      const lx = x(2.05);
      line(s, lx - 3, ys[i], x(t.sectors), y(t.hours), 'leader');
      boxed(s, lx, ys[i] + 4, t.type + ' ' + t.n, t.family ? 't-ann' : 't-tick');
    });
    const roof = Math.max(...pts.filter(p => p.sectors >= 2.5).map(p => p.hours), 0);
    // (on a phone that row would run into the long-haul names, so it goes
    // below the cluster, in two staggered lines)
    const ly = tight ? y(2.7) : y(Math.min(roof + 1.6, yMax - 1));
    const xa = x(tight ? 3.6 : 3.1), xb = x(xMax) - (tight ? 24 : 40);
    near.forEach((t, i) => {
      const lab = tight ? t.type : t.type + ' ' + t.n, w = textW(lab);
      const cx = near.length > 1 ? xa + i * (xb - xa) / (near.length - 1) : (xa + xb) / 2;
      const down = tight && i % 2 ? 16 : 0;
      line(s, cx, tight ? ly + down - 10 : ly + 4, x(t.sectors), y(t.hours), 'leader');
      boxed(s, cx - w / 2, ly + down, lab, t.family ? 't-ann' : 't-tick');
    });
    U.types.forEach(t => marker(s, 'diamond', x(t.sectors), y(t.hours), 4, 'med'));
    if (U.idle.length) boxed(s, x(0) + 8, y(0) - 8, U.idle.length + ' did not fly', 't-tick t-muted');

    // hover: the nearest aircraft
    const mark = rect(s, 0, 0, 9, 9, 'cell-hover');
    mark.style.display = 'none';
    const hit = rect(s, L, top, pw, ph, 'hit');
    let hot = null;
    hit.addEventListener('pointermove', ev => {
      const box = s.getBoundingClientRect(), px = ev.clientX - box.left, py = ev.clientY - box.top;
      let best = null, bd = 18 * 18;
      pts.forEach(p => { const d = (x(p.sectors) - px) ** 2 + (y(p.hours) - py) ** 2; if (d < bd) { bd = d; best = p; } });
      hot = best;
      hit.style.cursor = best ? 'pointer' : '';
      if (!best) { mark.style.display = 'none'; hideTip(); return; }
      mark.setAttribute('x', crisp(x(best.sectors) - 5)); mark.setAttribute('y', crisp(y(best.hours) - 5));
      mark.style.display = '';
      const t = byType[best.type];
      showTip(ev, best.reg + ' · ' + (best.type || 'type unknown'), [
        {key: 'flights a day', val: best.sectors.toFixed(1), sub: t ? 'type median ' + t.sectors.toFixed(1) : '', on: true},
        {key: 'hours a day', val: best.hours.toFixed(1), sub: t ? 'type median ' + t.hours.toFixed(1) : ''},
        {key: 'a flight', val: (best.hours / best.sectors).toFixed(1) + ' h', sub: 'on average'}],
        best.family ? 'Click for the airframe page' : 'Click for the fleet entry');
    });
    hit.addEventListener('pointerleave', () => { hot = null; mark.style.display = 'none'; hideTip(); });
    hit.addEventListener('click', () => {
      if (hot) location.href = hot.family ? '/airframe/' + encodeURIComponent(hot.reg) : '/fleet/' + encodeURIComponent(hot.icao24);
    });
  };

  f.table = host => table(host, ['Type', ['Aircraft', 'r'], ['Flights a day', 'r'], ['Hours a day', 'r'], ['Hours a flight', 'r']],
    U.types.map(t => [t.type, t.n, t.sectors.toFixed(1), t.hours.toFixed(1), (t.hours / t.sectors).toFixed(1)])
      .concat([['did not fly', U.idle.length, null, null, null]]));
  f.note.textContent = 'One square per aircraft marked active, over the last ' + U.days + ' full days, from ADS-B: flights with a known ' +
    'landing, counted from take-off to touchdown (block time would run longer). A diamond is the median aircraft of a type, ' +
    'labelled with the type and the number of aircraft that flew; types of fewer than four are not named. The dashed lines join ' +
    'equal average flight lengths. An aircraft that did not fly may be in maintenance, stored, or simply out of reach of the ' +
    'receivers; the Data view counts them.';
  return f;
}

// ── page ───────────────────────────────────────────────────────────────
async function load() {
  return (await fetch('/api/monitor')).json();
}
// The lead is in the page from the start; the sections follow it into <main>.
// Whatever could not be loaded leaves a status marker where it would stand.
async function init() {
  const main = document.querySelector('main'), lead = $('status'), mark = $('state');
  const how = D => {
    lead.replaceChildren(...status(D));
    mark.replaceChildren(overall(D));
    $('stamp').textContent = stamp(D.generated);
  };
  let D;
  try { D = await load(); } catch (e) { lead.replaceChildren(state('bad', 'The monitor could not be loaded')); return; }
  const errors = D.errors || {};
  if (D.headline) {
    how(D);
    main.append(readsFig(D).root, recordFig(D).root);
  } else lead.replaceChildren(state('bad', 'The collector’s status could not be loaded'));
  const logged = section({id: 'logged', title: 'What it has logged', field: 'tint', wide: true});
  logged.body.append(D.ledger ? ledger(D) : state('bad', 'The counts could not be loaded'));
  main.append(logged.root);
  if (D.hubs) main.append(hubsFig(D).root, useFig(D).root);
  else logged.body.append(state('bad', 'The flight figures could not be loaded'));
  if (errors.collector || errors.data) console.warn('monitor:', errors);
  redraw();
  watch(main);
  // how things stand moves with every run; the figures keep what the page loaded with
  setInterval(async () => {
    if (document.hidden || !D.headline) return;
    try {
      const N = await load();
      if (N.headline) how(N);
    } catch (e) { /* the next tick tries again */ }
  }, 60000);
}
init();
})();

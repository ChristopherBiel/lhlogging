// /fleet/<icao24> — one aircraft of the registry: what the registry holds on
// it, how much it flew, where to, and its last hundred flights, from
// /api/fleet/<icao24>. A page below Fleet DB: the lead carries the way back.
(() => {
'use strict';

const {$, el, esc, section, state, stats} = window.Page;
const icao24 = decodeURIComponent(location.pathname.split('/').pop() || '');
const num = n => Number(n || 0).toLocaleString('en-GB');
const main = document.querySelector('main');
$('reg').textContent = icao24.toUpperCase();              // until the registration is known

// What the registry holds, as spec rows.
function registry(info) {
  const sec = section({id: 'registry', title: 'Registry'});
  const dl = el('dl', 'spec');
  [['Registration', info.registration], ['ICAO24', info.icao24], ['Type', info.aircraft_type], ['Model', info.aircraft_subtype],
   ['Airline', info.airline_iata], ['First seen', info.first_seen], ['Last seen', info.last_seen]].forEach(([k, v]) => {
    const row = el('div');
    row.append(el('dt', null, k), el('dd', null, v || '—'));
    dl.append(row);
  });
  sec.body.append(dl);
  return sec.root;
}

// Flights a day: bars on a baseline, the first and the last day named.
function activity(daily) {
  const sec = section({id: 'activity', title: 'Activity', note: 'Flights a day, last 30 days', field: 'tint'});
  if (!daily.length) { sec.body.append(el('p', 'empty', 'No flights in the last 30 days.')); return sec.root; }
  const top = Math.max(...daily.map(d => d.count), 1), bars = el('div', 'bars');
  bars.setAttribute('role', 'img');
  bars.setAttribute('aria-label', 'Flights a day from ' + daily[0].date + ' to ' + daily[daily.length - 1].date + ', at most ' + top);
  daily.forEach(d => {
    const bar = el('i');
    bar.style.blockSize = (d.count / top * 100) + '%';
    bar.title = d.date + ': ' + d.count;
    bars.append(bar);
  });
  const ends = el('div', 'bars-ends');
  ends.append(el('span', null, daily[0].date), el('span', null, 'most in a day: ' + top), el('span', null, daily[daily.length - 1].date));
  sec.body.append(bars, ends);
  return sec.root;
}

// The routes it flew most: lying bars, standing on a baseline of their own.
function routes(list) {
  const sec = section({id: 'routes', title: 'Top routes', note: 'By flights recorded'});
  if (!list.length) { sec.body.append(el('p', 'empty', 'No routes recorded.')); return sec.root; }
  const top = list[0].count, box = el('div', 'hbars');
  list.slice(0, 12).forEach(r => {
    const row = el('div', 'hbar'), track = el('span', 'hbar__track'), fill = el('i');
    fill.style.inlineSize = (r.count / top * 100) + '%';
    track.append(fill);
    row.append(el('span', 'hbar__key', r.route.split('-').join(' → ')), track, el('span', 'hbar__n', num(r.count)));
    box.append(row);
  });
  sec.body.append(box);
  return sec.root;
}

// A log is ink in Faceplate's code.
function flights(list) {
  const sec = section({id: 'flights', title: 'Recent flights', note: 'The last ' + Math.max(list.length, 1), kind: 'ink', field: 'tint'});
  if (!list.length) { sec.body.append(el('p', 'empty', 'No flights recorded.')); return sec.root; }
  const t = el('table', 'table table--page');
  t.innerHTML = '<thead><tr><th>Date</th><th>Callsign</th><th>From</th><th>To</th><th class="num">Departure</th>' +
    '<th class="num">Arrival</th><th class="num">Duration</th></tr></thead><tbody>' + list.map(f =>
    '<tr><td class="mono">' + esc(f.date || '—') + '</td><td class="mono">' + esc(f.callsign || '—') + '</td>' +
    '<td class="mono">' + esc(f.dep || '—') + '</td><td class="mono">' + esc(f.arr || '—') + '</td>' +
    '<td class="num">' + (f.first_seen ? f.first_seen.slice(11, 16) : '') + '</td>' +
    '<td class="num">' + (f.last_seen ? f.last_seen.slice(11, 16) : '') + '</td>' +
    '<td class="num">' + (f.duration ? Math.floor(f.duration / 60) + ' h ' + String(f.duration % 60).padStart(2, '0') : '—') + '</td></tr>').join('') + '</tbody>';
  sec.body.append(t);
  return sec.root;
}

async function init() {
  let data;
  try { data = await (await fetch('/api/fleet/' + encodeURIComponent(icao24))).json(); }
  catch (e) { data = {error: 'The aircraft could not be loaded'}; }
  if (data.error) { $('figures').replaceChildren(state('bad', data.error)); return; }
  const info = data.info;
  document.title = info.registration + ' | LH Fleet';
  $('reg').textContent = info.registration || icao24.toUpperCase();
  $('lede').textContent = [info.aircraft_subtype, info.aircraft_type].filter(Boolean).join(' · ');
  $('state').replaceChildren(info.is_active ? state('ok', 'Active') : state('note', 'Retired'));
  $('figures').replaceChildren(stats([['flights', num(info.total_flights), 'since ' + (info.first_seen || '—')],
                                      ['last 30 days', num(info.flights_30d), 'flights'],
                                      ['last 7 days', num(info.flights_7d), 'flights']], 3));
  const li = el('li'), a = el('a', null, 'Airframe and cabin ');
  a.href = '/airframe/' + encodeURIComponent(info.registration);
  a.append(el('span', 'fp-arrow', '→'));
  li.append(a);
  $('links').replaceChildren(li);
  main.append(registry(info), activity(data.daily || []), routes(data.routes || []), flights(data.flights || []));
}
init();
})();

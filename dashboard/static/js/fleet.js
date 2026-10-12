// /fleet — the aircraft registry: every aircraft the site follows, as one
// table that runs its full length (the window scrolls, the head stays),
// filtered and sorted in the browser from /api/fleet.
(() => {
'use strict';

const {$, el, esc, state} = window.Page;
const num = n => Number(n).toLocaleString('en-GB');

let all = [], sortKey = 'registration', sortAsc = true, status = 'all', reviewOnly = false;

function filtered() {
  const q = $('search').value.toLowerCase().trim(), type = $('type-filter').value;
  return all.filter(a => {
    if (reviewOnly && !a.needs_review) return false;
    if (status === 'active' && !a.is_active) return false;
    if (status === 'retired' && a.is_active) return false;
    if (status === 'tracking' && !a.currently_tracking) return false;
    if (type && a.aircraft_type !== type) return false;
    return !q || (a.registration + ' ' + a.icao24 + ' ' + a.aircraft_type + ' ' + a.aircraft_subtype).toLowerCase().includes(q);
  });
}
function sorted(list) {
  return [...list].sort((a, b) => {
    let va = a[sortKey], vb = b[sortKey];
    if (va == null) va = '';
    if (vb == null) vb = '';
    if (typeof va === 'boolean') { va = va ? 1 : 0; vb = vb ? 1 : 0; }
    if (typeof va === 'number') return sortAsc ? va - vb : vb - va;
    return sortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
  });
}

// Four states, three chip tints: retired is grey, a review terracotta, and
// tracking the plain chip.
const chip = (cls, word) => '<span class="fp-chip' + (cls ? ' fp-chip--' + cls : '') + '">' + word + '</span>';
function render() {
  const rows = sorted(filtered());
  $('count').textContent = num(rows.length) + ' of ' + num(all.length);
  document.querySelectorAll('#table-head th').forEach(th => {
    const on = th.dataset.key === sortKey;
    if (on) th.setAttribute('aria-sort', sortAsc ? 'ascending' : 'descending'); else th.removeAttribute('aria-sort');
    th.querySelector('i').textContent = on && !sortAsc ? '▼' : '▲';
  });
  $('table-body').innerHTML = rows.map(a => {
    const href = '/fleet/' + encodeURIComponent(a.icao24);
    return '<tr data-href="' + href + '">' +
      '<td class="reg"><a href="' + href + '">' + esc(a.registration) + '</a></td>' +
      '<td class="mono dim">' + esc(a.icao24) + '</td>' +
      '<td class="mono">' + esc(a.aircraft_type || '—') + '</td>' +
      '<td>' + esc(a.aircraft_subtype || '—') + '</td>' +
      '<td>' + (a.is_active ? chip('sage', 'active') : chip('gray', 'retired')) +
               (a.currently_tracking ? chip('', 'tracking') : '') + (a.needs_review ? chip('terra', 'review') : '') + '</td>' +
      '<td class="num">' + num(a.total_flights) + '</td>' +
      '<td class="num">' + num(a.flights_7d) + '</td>' +
      '<td class="mono">' + (a.last_flight || '—') + '</td>' +
      '<td class="mono">' + (a.first_seen || '—') + '</td>' +
    '</tr>';
  }).join('');
  $('none').hidden = rows.length > 0;
}

async function init() {
  let data;
  try { data = await (await fetch('/api/fleet')).json(); } catch (e) { data = {error: 'The fleet could not be loaded'}; }
  if (data.error) { $('state').replaceChildren(state('bad', data.error)); return; }
  all = data.aircraft;
  $('state').remove();
  $('filters').hidden = false;
  data.types.forEach(t => $('type-filter').append(el('option', null, t)));
  $('aircraft').hidden = false;
  render();
}

document.querySelectorAll('#table-head th').forEach(th => th.addEventListener('click', () => {
  if (sortKey === th.dataset.key) sortAsc = !sortAsc; else { sortKey = th.dataset.key; sortAsc = true; }
  render();
}));
$('search').addEventListener('input', render);
$('type-filter').addEventListener('change', render);
$('review-filter').addEventListener('click', ev => {
  reviewOnly = !reviewOnly;
  ev.currentTarget.classList.toggle('is-on', reviewOnly);
  ev.currentTarget.setAttribute('aria-pressed', String(reviewOnly));
  render();
});
document.querySelectorAll('#status-filter button').forEach(b => b.addEventListener('click', () => {
  document.querySelectorAll('#status-filter button').forEach(o => {
    o.classList.toggle('is-on', o === b);
    o.setAttribute('aria-pressed', String(o === b));
  });
  status = b.dataset.status;
  render();
}));
// a row leads to its aircraft; the registration is the link a keyboard finds
$('table-body').addEventListener('click', ev => {
  const row = ev.target.closest('tr[data-href]');
  if (row && !ev.target.closest('a')) location.href = row.dataset.href;
});
init();
})();

// The admin: the registry and the flight log, edited by hand (add, correct in
// place, retire, delete), against the admin's own API under its secret path.
// A plain script, not a module: its rows carry inline handlers.
const PFX = document.getElementById('admin').dataset.prefix;    // the admin's secret path, set by the page
const API = '/' + PFX + '/api';
let flPage = 1;

function $(id) { return document.getElementById(id); }

function switchTab(tab) {
  document.querySelectorAll('#tabs button').forEach(b => {
    b.classList.toggle('is-on', b.dataset.tab === tab);
    b.setAttribute('aria-pressed', String(b.dataset.tab === tab));
  });
  ['aircraft', 'flights'].forEach(t => { $('tab-' + t).hidden = t !== tab; });
  if (tab === 'flights') loadFlights();
}

function toast(msg, ok) {
  const t = $('toast');
  t.textContent = msg;
  t.className = 'toast fp-status state ' + (ok ? 'state--ok' : 'state--bad');
  t.style.opacity = '1';
  setTimeout(() => t.style.opacity = '0', 2500);
}

function debounce(fn, ms) {
  let t;
  return function() { clearTimeout(t); t = setTimeout(fn, ms); };
}

async function api(path, opts) {
  const r = await fetch(API + path, opts);
  const d = await r.json();
  if (!r.ok) { toast(d.error || 'Error', false); return null; }
  return d;
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s || '';
  return d.innerHTML;
}

// ── Aircraft ─────────────────────────────────────────────────

async function loadAircraft() {
  const p = new URLSearchParams();
  const s = $('ac-search').value.trim();
  if (s) p.set('search', s);
  const t = $('ac-type').value;
  if (t) p.set('type', t);
  const st = $('ac-status').value;
  if (st) p.set('status', st);
  if ($('ac-review').classList.contains('on')) p.set('needs_review', 'true');

  const d = await api('/aircraft?' + p);
  if (!d) return;

  // Populate type filter (only once)
  const sel = $('ac-type');
  if (sel.options.length <= 1) {
    const types = [...new Set(d.aircraft.map(a => a.aircraft_type).filter(Boolean))].sort();
    types.forEach(t => { const o = document.createElement('option'); o.value = t; o.textContent = t; sel.appendChild(o); });
  }

  $('ac-count').textContent = d.aircraft.length + ' aircraft';
  $('ac-body').innerHTML = d.aircraft.map(a => {
    const cls = a.needs_review ? ' class="review"' : '';
    const statusBadge = a.is_active
      ? '<span class="fp-chip fp-chip--sage">Active</span>'
      : '<span class="fp-chip fp-chip--gray">Retired</span>';
    const reviewBadge = a.needs_review
      ? '<span class="fp-chip fp-chip--terra">Review</span>' : '';
    return '<tr' + cls + '>' +
      '<td>' + esc(a.icao24) + '</td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="registration" onblur="inlineEditAc(this)">' + esc(a.registration) + '</span></td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="aircraft_type" onblur="inlineEditAc(this)">' + esc(a.aircraft_type) + '</span></td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="aircraft_subtype" onblur="inlineEditAc(this)">' + esc(a.aircraft_subtype) + '</span></td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="airline_iata" onblur="inlineEditAc(this)">' + esc(a.airline_iata) + '</span></td>' +
      '<td>' + statusBadge + '</td>' +
      '<td>' + reviewBadge + '</td>' +
      '<td class="actions">' +
        (a.is_active
          ? '<button class="fp-btn btn-sm" onclick="retireAc(\'' + esc(a.icao24) + '\')">Retire</button>'
          : '<button class="fp-btn fp-btn--solid btn-sm" onclick="reactivateAc(\'' + esc(a.icao24) + '\')">Reactivate</button>') +
        '<button class="fp-btn fp-btn--terra btn-sm" onclick="deleteAc(\'' + esc(a.icao24) + '\')">Del</button>' +
        (a.needs_review
          ? '<button class="fp-btn btn-sm" onclick="clearReviewAc(\'' + esc(a.icao24) + '\')">OK</button>'
          : '') +
      '</td></tr>';
  }).join('') || '<tr><td colspan="8" class="dim">No aircraft match these filters.</td></tr>';
}

async function inlineEditAc(el) {
  const icao = el.dataset.icao;
  const field = el.dataset.field;
  const val = el.textContent.trim();
  const d = await api('/aircraft/' + icao, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({[field]: val})
  });
  if (d) toast('Updated ' + icao, true);
}

async function retireAc(icao) {
  if (!confirm('Retire ' + icao + '?')) return;
  const d = await api('/aircraft/' + icao + '/retire', { method: 'POST' });
  if (d) { toast('Retired ' + icao, true); loadAircraft(); }
}

async function reactivateAc(icao) {
  const d = await api('/aircraft/' + icao + '/reactivate', { method: 'POST' });
  if (d) { toast('Reactivated ' + icao, true); loadAircraft(); }
}

async function deleteAc(icao) {
  if (!confirm('Delete ' + icao + '? This cannot be undone.')) return;
  const d = await api('/aircraft/' + icao, { method: 'DELETE' });
  if (d) { toast('Deleted ' + icao, true); loadAircraft(); }
}

async function clearReviewAc(icao) {
  const d = await api('/aircraft/' + icao, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({needs_review: false})
  });
  if (d) { toast('Cleared review for ' + icao, true); loadAircraft(); }
}

function showAddAircraft() {
  $('m-ac-icao24').value = '';
  $('m-ac-reg').value = '';
  $('m-ac-type').value = '';
  $('m-ac-subtype').value = '';
  $('m-ac-airline').value = 'LH';
  $('modal-ac').classList.add('show');
}

async function saveAircraft() {
  const body = {
    icao24: $('m-ac-icao24').value,
    registration: $('m-ac-reg').value,
    aircraft_type: $('m-ac-type').value,
    aircraft_subtype: $('m-ac-subtype').value,
    airline_iata: $('m-ac-airline').value
  };
  const d = await api('/aircraft', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  if (d) { toast('Added aircraft', true); closeModal('modal-ac'); loadAircraft(); }
}

// ── Flights ──────────────────────────────────────────────────

async function loadFlights() {
  const p = new URLSearchParams();
  const v = (id) => $(id).value.trim();
  if (v('fl-icao24')) p.set('icao24', v('fl-icao24'));
  if (v('fl-callsign')) p.set('callsign', v('fl-callsign'));
  if (v('fl-dep')) p.set('dep', v('fl-dep'));
  if (v('fl-arr')) p.set('arr', v('fl-arr'));
  if (v('fl-from')) p.set('date_from', v('fl-from'));
  if (v('fl-to')) p.set('date_to', v('fl-to'));
  if ($('fl-review').classList.contains('on')) p.set('needs_review', 'true');
  p.set('page', flPage);

  const d = await api('/flights?' + p);
  if (!d) return;

  $('fl-body').innerHTML = d.flights.map(f => {
    const cls = f.needs_review ? ' class="review"' : '';
    const dur = f.duration_minutes != null ? Math.floor(f.duration_minutes/60) + 'h ' + (f.duration_minutes%60) + 'm' : '\u2014';
    const depTime = f.first_seen ? f.first_seen.slice(11,16) : '';
    const arrTime = f.last_seen ? f.last_seen.slice(11,16) : '';
    const reviewBadge = f.needs_review ? '<span class="fp-chip fp-chip--terra">Review</span>' : '';
    return '<tr' + cls + '>' +
      '<td>' + esc(f.flight_date) + '</td>' +
      '<td>' + esc(f.icao24) + '</td>' +
      '<td>' + esc(f.registration || '') + '</td>' +
      '<td><span class="editable" contenteditable data-id="' + f.id + '" data-field="callsign" onblur="inlineEditFl(this)">' + esc(f.callsign) + '</span></td>' +
      '<td><span class="editable" contenteditable data-id="' + f.id + '" data-field="departure_airport_icao" onblur="inlineEditFl(this)">' + esc(f.departure_airport_icao) + '</span></td>' +
      '<td><span class="editable" contenteditable data-id="' + f.id + '" data-field="arrival_airport_icao" onblur="inlineEditFl(this)">' + esc(f.arrival_airport_icao) + '</span></td>' +
      '<td>' + depTime + '</td>' +
      '<td>' + arrTime + '</td>' +
      '<td>' + dur + '</td>' +
      '<td>' + reviewBadge + '</td>' +
      '<td class="actions">' +
        (f.needs_review ? '<button class="fp-btn btn-sm" onclick="clearReviewFl(' + f.id + ')">OK</button>' : '') +
        '<button class="fp-btn fp-btn--terra btn-sm" onclick="deleteFl(' + f.id + ')">Del</button>' +
      '</td></tr>';
  }).join('') || '<tr><td colspan="11" class="dim">No flights match these filters.</td></tr>';

  // Pagination
  const pg = $('fl-pagination');
  if (d.pages > 1) {
    let html = '<button class="fp-btn btn-sm" onclick="flGo(' + (d.page-1) + ')" ' + (d.page<=1?'disabled':'') + '>&laquo;</button>';
    html += '<span>Page ' + d.page + ' / ' + d.pages + ' (' + d.total + ' flights)</span>';
    html += '<button class="fp-btn btn-sm" onclick="flGo(' + (d.page+1) + ')" ' + (d.page>=d.pages?'disabled':'') + '>&raquo;</button>';
    pg.innerHTML = html;
  } else {
    pg.innerHTML = d.total ? '<span>' + d.total + ' flights</span>' : '';
  }
}

function flGo(p) { flPage = Math.max(1, p); loadFlights(); }

async function inlineEditFl(el) {
  const id = el.dataset.id;
  const field = el.dataset.field;
  const val = el.textContent.trim();
  const d = await api('/flights/' + id, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({[field]: val || null})
  });
  if (d) toast('Updated flight #' + id, true);
}

async function clearReviewFl(id) {
  const d = await api('/flights/' + id, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({needs_review: false})
  });
  if (d) { toast('Cleared review', true); loadFlights(); }
}

async function deleteFl(id) {
  if (!confirm('Delete flight #' + id + '?')) return;
  const d = await api('/flights/' + id, { method: 'DELETE' });
  if (d) { toast('Deleted', true); loadFlights(); }
}

function showAddFlight() {
  $('m-fl-icao24').value = '';
  $('m-fl-callsign').value = '';
  $('m-fl-dep').value = '';
  $('m-fl-arr').value = '';
  $('m-fl-first').value = '';
  $('m-fl-last').value = '';
  $('modal-fl').classList.add('show');
}

async function saveFlight() {
  const first = $('m-fl-first').value;
  const last = $('m-fl-last').value;
  const body = {
    icao24: $('m-fl-icao24').value,
    callsign: $('m-fl-callsign').value,
    departure_airport_icao: $('m-fl-dep').value,
    arrival_airport_icao: $('m-fl-arr').value,
    first_seen: first ? first + ':00Z' : '',
    last_seen: last ? last + ':00Z' : ''
  };
  const d = await api('/flights', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  if (d) { toast('Added flight', true); closeModal('modal-fl'); loadFlights(); }
}

function closeModal(id) { $(id).classList.remove('show'); }

// Close modals on backdrop click
document.querySelectorAll('.dialog-bg').forEach(bg => {
  bg.addEventListener('click', e => { if (e.target === bg) bg.classList.remove('show'); });
});

// wire the square "review" toggles (replace the old native-checkbox onchange)
function wireReview(id, cb){
  const el = $(id); if(!el) return;
  const t = ()=>{ const on=el.classList.toggle('on'); el.setAttribute('aria-checked', on?'true':'false'); cb(); };
  el.addEventListener('click', t);
  el.addEventListener('keydown', e=>{ if(e.key===' '||e.key==='Enter'){ e.preventDefault(); t(); } });
}
wireReview('ac-review', loadAircraft);
wireReview('fl-review', loadFlights);

// Init
loadAircraft();

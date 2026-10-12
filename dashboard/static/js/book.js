// /book — Catch a Tail: rank the flights of your dates by the chance your
// airframe operates them (Plan), or look up what is published by tail, by
// route or from an airport on the map, from /api/book and /api/book/plan.
// The query stands in the lead; the map and the flights are sections; a
// flight opens the flight dialog (flight.js).
(() => {
'use strict';

const {$, esc, state} = window.Page, Flight = window.Flight;
let mode = 'plan';
// The page's only prose: what stands where the flights will be until something
// has been asked, one sentence per mode.
const PROMPTS = {
  plan: 'Enter the tail you want to fly, e.g. D-ABYN, and optionally a route and dates: every flight is ranked by the chance that your tail operates it.',
  tail: 'Enter a registration to see every upcoming leg that airframe is published on.',
  route: 'Add one or more airports on either side, by code or city, to see the flights published between them.',
  map: 'Click an airport on the map to see what departs from it.',
};
// what the marks on a flight mean; it comes with the flights
const KEY = '<div class="key"><div><span><span class="star">&#9733;</span>your tails</span>'
  + '<span><span class="abadge">ALLEGRIS</span>the new cabin</span>'
  + '<span><span class="cbadge cf">F8</span><span class="cbadge">C80</span>First and Business seats</span></div></div>';
function setMode(m){
  mode = m;
  const prompt = $('results').querySelector(':scope > .empty[data-prompt]');
  if(prompt) prompt.textContent = PROMPTS[m];
  ['plan','tail','route','map'].forEach(k => { $('m-'+k).classList.toggle('is-on', m===k); $('m-'+k).setAttribute('aria-pressed', String(m===k)); });
  $('plan-in').style.display = m==='plan' ? '' : 'none';
  $('date-box').style.display = m==='plan' ? '' : 'none';
  $('tail-in').style.display = m==='tail' ? '' : 'none';
  $('dep-box').style.display = (m==='route' || m==='plan') ? '' : 'none';
  $('arr-box').style.display = (m==='route' || m==='plan') ? '' : 'none';
  // in map mode the marker *is* the query, so the free-text search button goes
  $('search-btn').style.display = m==='map' ? 'none' : '';
  // the filters act on the published tail; a plan is about one fixed tail
  $('filterbar').hidden = m==='plan';
  // the map is a section of its own, ahead of the flights
  $('map-sec').hidden = m!=='map';
  $('results-index').textContent = m==='map' ? '02' : '01';
  if(m==='map'){ mapInit(); if(MAP.marks.length) mapDraw(); }   // redraw: it sizes to a visible box
  else { ({plan:$('plan-in'), tail:$('tail-in'), route:$('dep-in')})[m].focus(); }
}

/* ── Your targets: kept in this browser only (no accounts), star your tails
   everywhere and prefill the Plan input. Storage can be blocked or throw
   (private windows) — then there are simply no stars. */
const TKEY = 'lh.targets';
function loadTargets(){
  try { const v = JSON.parse(localStorage.getItem(TKEY) || '[]'); return Array.isArray(v) ? v.filter(x => typeof x === 'string') : []; }
  catch(e){ return []; }
}
let WATCH = new Set(loadTargets());
function fillTargets(){
  WATCH = new Set(loadTargets());
  $('targets').innerHTML = [...WATCH].map(r => '<option value="'+esc(r)+'">').join('');
}
function saveTarget(reg){
  try { const t = loadTargets().filter(x => x !== reg); t.unshift(reg); localStorage.setItem(TKEY, JSON.stringify(t.slice(0, 8))); }
  catch(e){}
  fillTargets();
}
fillTargets();
function isoDay(d){ return d.toISOString().slice(0, 10); }
$('plan-from').value = isoDay(new Date());
$('plan-to').value = isoDay(new Date(Date.now() + 14 * 864e5));

/* ── Multi-airport token inputs with suggestions ─────────────────
   The suggestion list is the collected FIS network (code + city name),
   so everything offered is actually searchable. Filtering is local. */
let AIRPORTS = [];
fetch('/api/book/airports').then(r=>r.json()).then(d=>{ AIRPORTS = d.airports||[]; }).catch(()=>{});

function makeTok(boxId){
  const box = $(boxId), input = box.querySelector('input'), sugg = box.querySelector('.sugg');
  const codes = [];
  let items = [], act = -1;
  function renderChips(){
    box.querySelectorAll('.tok').forEach(t=>t.remove());
    codes.forEach(c=>{
      const el = document.createElement('span');
      el.className = 'tok';
      el.innerHTML = c + '<button type="button" aria-label="remove '+c+'">&times;</button>';
      el.querySelector('button').addEventListener('click', ev=>{
        ev.stopPropagation(); codes.splice(codes.indexOf(c),1); renderChips(); });
      box.insertBefore(el, input);
    });
  }
  function add(c){
    c = (c||'').trim().toUpperCase();
    if(/^[A-Z0-9]{3}$/.test(c) && !codes.includes(c)){ codes.push(c); renderChips(); }
    input.value = ''; hide();
  }
  function matches(q){
    q = q.trim().toUpperCase(); if(!q) return [];
    const ql = q.toLowerCase();
    return AIRPORTS
      .filter(a => !codes.includes(a.code) && (a.code.startsWith(q) || (a.name||'').toLowerCase().includes(ql)))
      .sort((a,b) => (b.code.startsWith(q) - a.code.startsWith(q)) || (b.n - a.n))
      .slice(0,8);
  }
  function show(){
    items = matches(input.value); act = -1;
    if(!items.length){ hide(); return; }
    sugg.innerHTML = items.map((a,i)=>'<div class="sitem" data-i="'+i+'"><span class="code">'+a.code+'</span><span class="nm">'+(a.name||'')+'</span></div>').join('');
    sugg.classList.add('show');
    sugg.querySelectorAll('.sitem').forEach(el=>{
      el.addEventListener('mousedown', ev=>{ ev.preventDefault(); add(items[+el.dataset.i].code); });
    });
  }
  function hide(){ sugg.classList.remove('show'); items = []; act = -1; }
  function mark(){ sugg.querySelectorAll('.sitem').forEach((el,i)=>el.classList.toggle('active', i===act)); }
  function commit(){   // turn whatever is typed into a chip (best match wins)
    const v = input.value.trim().toUpperCase();
    if(!v) return;
    if(/^[A-Z0-9]{3}$/.test(v)) add(v);
    else { const m = matches(v); if(m.length) add(m[0].code); else input.value=''; }
  }
  input.addEventListener('input', show);
  input.addEventListener('keydown', e=>{
    if(e.key==='ArrowDown' && items.length){ act = (act+1)%items.length; mark(); e.preventDefault(); }
    else if(e.key==='ArrowUp' && items.length){ act = (act-1+items.length)%items.length; mark(); e.preventDefault(); }
    else if(e.key===','){ e.preventDefault(); commit(); }
    else if(e.key==='Enter'){
      e.preventDefault();
      if(act>=0) add(items[act].code);
      else if(input.value.trim()) commit();
      else search();
    }
    else if(e.key==='Escape' && items.length){ hide(); e.stopPropagation(); }
    else if(e.key==='Backspace' && !input.value && codes.length){ codes.pop(); renderChips(); }
  });
  input.addEventListener('blur', ()=>{ commit(); hide(); });
  box.addEventListener('click', e=>{ if(e.target===box) input.focus(); });
  return {
    codes: ()=>codes.slice(),
    set(list){ codes.length = 0; input.value=''; (list||[]).forEach(c=>{
      c = (c||'').trim().toUpperCase();
      if(/^[A-Z0-9]{3}$/.test(c) && !codes.includes(c)) codes.push(c); }); renderChips(); },
  };
}
const tokDep = makeTok('dep-box'), tokArr = makeTok('arr-box');
function fmtDay(iso){ if(!iso) return '?'; return new Date(iso).toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'short',timeZone:'UTC'}); }
function fmtClock(iso){ if(!iso) return ''; return new Date(iso).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
function tcls(t){ return ({'748':'t748','388':'t388','788':'t789','789':'t789','78X':'t789','359':'t359','35K':'t359'})[t] || 'tother'; }

function miniChip(hold){
  if(!hold) return '<div class="miniconf"><span class="p none">&mdash;</span><span class="cn">no data</span></div>';
  const p = Math.round(hold.p*100);
  const cls = p>=85 ? 'cg' : (p>=60 ? 'ca' : 'cr');
  return '<div class="miniconf '+cls+'" title="chance the published tail still holds by departure ('+hold.lead+'d out, '+hold.basis+', n='+hold.n+')">'
    +'<span class="p">'+p+'%</span><span class="cn">holds</span></div>';
}

/* cabin config {F,C,E,M} — F/C badges on the card, full breakdown as tooltip */
function cabinText(c){
  const t = [];
  if(c.F) t.push('First '+c.F); if(c.C) t.push('Business '+c.C);
  if(c.E) t.push('Prem Eco '+c.E); if(c.M) t.push('Economy '+c.M);
  return t.join(' \u00b7 ');
}
function cabinBadges(c){
  if(!c) return '';
  const full = 'Cabin: '+cabinText(c);
  return (c.F ? '<span class="cbadge cf" title="'+full+'">F'+c.F+'</span>' : '')
       + (c.C ? '<span class="cbadge" title="'+full+'">C'+c.C+'</span>' : '');
}

/* ── Result filters (type / Allegris / First) — client-side over the last fetch ── */
const FILT = { fams:null, alleg:'any', first:'any' };   // fams null = all types
let LAST = null;
function famOf(t){ return ({'748':'747','388':'A380','788':'787','789':'787','78X':'787','359':'A350','35K':'A350'})[t] || 'other'; }
function passes(f){
  if(FILT.fams && !FILT.fams.has(famOf(f.type))) return false;
  if(FILT.alleg!=='any' && !!f.allegris !== (FILT.alleg==='yes')) return false;
  if(FILT.first!=='any' && !!(f.cabin && f.cabin.F) !== (FILT.first==='yes')) return false;
  return true;
}
function syncFilters(){
  if(LAST) drawResults();
  if(MAP.gLayer) mapRender(false);
}
function applyFams(){
  const boxes = [...document.querySelectorAll('.fchk')], on = boxes.filter(x=>!x.classList.contains('off'));
  FILT.fams = on.length===boxes.length ? null : new Set(on.map(x=>x.dataset.fam));
  syncFilters();
}
document.querySelectorAll('.fchk').forEach(el => {
  const t = ()=>{ const off=el.classList.toggle('off'); el.setAttribute('aria-checked', off?'false':'true'); applyFams(); };
  el.addEventListener('click', t);
  el.addEventListener('keydown', e=>{ if(e.key===' '||e.key==='Enter'){ e.preventDefault(); t(); } });
});
function segWire(id, key){
  $(id).querySelectorAll('button').forEach(b => b.addEventListener('click', ()=>{
    FILT[key] = b.dataset.v;
    $(id).querySelectorAll('button').forEach(x=>x.classList.toggle('is-on', x===b));
    syncFilters();
  }));
}
segWire('f-alleg','alleg'); segWire('f-first','first');
$('f-reset').addEventListener('click', ()=>{
  document.querySelectorAll('.fchk').forEach(x=>{ x.classList.remove('off'); x.setAttribute('aria-checked','true'); });
  ['f-alleg','f-first'].forEach(id => $(id).querySelectorAll('button')
    .forEach(x=>x.classList.toggle('is-on', x.dataset.v==='any')));
  FILT.fams = null; FILT.alleg = 'any'; FILT.first = 'any';
  syncFilters();
});

function renderResults(d){ LAST = d; drawResults(); }
function drawResults(){
  const R = $('results'), d = LAST;
  if(d.error){ R.replaceChildren(state('bad', d.error)); return; }
  const all = d.flights || [];
  if(!all.length){ R.innerHTML = '<p class="empty">No upcoming flights found. The schedule is only published about four days ahead: try again closer to your date, or check the spelling.</p>'; return; }
  const fs = all.filter(passes);
  if(!fs.length){ R.innerHTML = '<p class="empty">All '+all.length+' flights are hidden by the filters: adjust or reset them.</p>'; return; }
  let h = KEY;
  fs.forEach(f => {
    const lead = f.lead===0 ? 'today' : (f.lead===1 ? 'tomorrow' : 'in '+f.lead+' days');
    const reassigned = f.reassigned ? ' &middot; <span class="warn">reassigned before</span>' : '';
    h += '<div class="fcard" data-num="'+f.number+'" data-fdate="'+f.flight_date+'">'
      + '<div class="when"><b>'+fmtDay(f.dep_sched)+'</b>'+lead+'</div>'
      + '<div class="route"><div class="pair">'+f.dep+' &rarr; '+f.arr+viaTxt(f.via)+'</div>'
      + '<div class="sub">'+f.flight+' &middot; dep '+fmtClock(f.dep_sched)+(f.arr_sched?' &middot; arr '+fmtClock(f.arr_sched):'')+reassigned+'</div></div>'
      + '<div class="tail">'+(WATCH.has(f.reg)?'<span class="star">&#9733;</span>':'')+tlink(f.reg)
      + '<span class="tbadge '+tcls(f.type)+'">'+(f.type||'?')+'</span>'
      + (f.allegris?'<span class="abadge" title="Allegris cabin">ALLEGRIS</span>':'')
      + cabinBadges(f.cabin)+'</div>'
      + miniChip(f.hold) + '</div>';
  });
  if(fs.length < all.length) h += '<div class="fnote">'+fs.length+' of '+all.length+' flights shown &mdash; '+(all.length-fs.length)+' hidden by filters</div>';
  R.innerHTML = h;
}

/* ── Plan mode: every candidate flight for one target tail, ranked by P ── */
let PLAN = null, PSORT = 'p';
// the aeroLOPA seat map for a tail's sub-fleet code (opens their site)
function seatMapLink(sm){
  if(!sm || !sm.url) return '';
  return '<a class="tail-link" href="'+esc(sm.url)+'" target="_blank" rel="noopener">'
    + (sm.exact ? esc(sm.code)+' seat map' : 'seat maps')+' &rarr;</a>';
}
// a tail anywhere on the page opens its airframe profile (cabin, where it flies)
function tlink(reg){ return reg ? '<a class="tail-link" href="/airframe/'+encodeURIComponent(reg)+'">'+esc(reg)+'</a>' : '?'; }
function pctText(p){ const v = p * 100; return v >= 10 ? Math.round(v)+'%' : (v >= 1 ? v.toFixed(1)+'%' : (v > 0 ? '&lt;1%' : '0%')); }
function srcLabel(s){ return ({published:'published', scheduled:'FIS schedule', projected:'projected'})[s] || s; }
function leadTxt(fdate){
  const n = Math.round((new Date(fdate+'T00:00:00Z') - new Date(isoDay(new Date())+'T00:00:00Z')) / 864e5);
  return n === 0 ? 'today' : (n === 1 ? 'tomorrow' : 'in '+n+' days');
}
function renderPlan(d){ PLAN = d; drawPlan(); }
function drawPlan(){
  const R = $('results'), d = PLAN;
  if(d.error){ R.replaceChildren(state('bad', d.error)); return; }
  const t = d.target;
  let h = '<div class="tsum"><div class="tsum-head"><span class="treg">'+tlink(t.reg)+'</span>'
    + '<span class="tbadge '+tcls(t.type)+'">'+esc(t.type)+'</span>'
    + (t.allegris ? '<span class="abadge">ALLEGRIS</span>' : '') + cabinBadges(t.cabin)
    + (t.cabin ? '<span class="tcab">'+cabinText(t.cabin)+'</span>' : '')
    + (t.seat_map ? '<span class="tcab">'+seatMapLink(t.seat_map)+'</span>' : '') + '</div>';
  const idle = t.idle_days;
  h += '<div class="tsum-line">' + (idle == null ? 'No operated flight in the last weeks.'
    : (idle > 3 ? '&#9888; Has not flown for '+Math.floor(idle)+' days &mdash; possibly in maintenance.'
    : 'In service &middot; last flew '+(idle < 1 ? 'within the last day' : Math.round(idle)+' day'+(Math.round(idle) === 1 ? '' : 's')+' ago')+'.')) + '</div>';
  if(t.published_legs && t.published_legs.length){
    h += '<div class="tsum-line">Published next: ' + t.published_legs.map(l =>
      '<span class="pleg">'+esc(l.flight)+' '+fmtDay(l.dep_sched)+' '+esc(l.dep)+'&rarr;'+esc(l.arr)+'</span>').join('') + '</div>';
  }
  h += '<div class="tsum-note">' + (t.fit && t.fit.m >= 1e5
    ? 'On the '+esc(t.type)+' fleet, which tail flies a given flight can&rsquo;t be told from its history: every flight after '+fmtD(d.horizon)+' shows the same even share. The published tail, up to ~4 days out, is where the odds move &mdash; check back then.'
    : 'Tails are published up to '+fmtD(d.horizon)+'; after that the list is the timetable of recent weeks, ranked by how often '+esc(t.reg)+' flew each flight.') + '</div></div>';
  const fs = (d.flights || []).slice();
  if(!fs.length){
    R.innerHTML = h + '<p class="empty">No candidate flights for '+esc(t.reg)+' in these dates'+((d.query.dep || d.query.arr) ? ' on this route' : '')+'.</p>';
    return;
  }
  if(PSORT === 'date') fs.sort((a, b) => (a.dep_sched || '').localeCompare(b.dep_sched || ''));
  h += '<div class="plan-head"><span class="t">'+fs.length+' candidate flight'+(fs.length === 1 ? '' : 's')+'</span>'
    + '<div class="fp-seg" id="psort"><button type="button" data-v="p" class="'+(PSORT === 'p' ? 'is-on' : '')+'">Best chance</button>'
    + '<button type="button" data-v="date" class="'+(PSORT === 'date' ? 'is-on' : '')+'">By date</button></div></div>' + KEY;
  fs.slice(0, 150).forEach(f => {
    h += '<div class="fcard" data-num="'+f.number+'" data-fdate="'+f.flight_date+'" data-src="'+f.source+'">'
      + '<div class="when"><b>'+fmtDay(f.dep_sched)+'</b>'+leadTxt(f.flight_date)+'</div>'
      + '<div class="route"><div class="pair">'+esc(f.dep)+' &rarr; '+esc(f.arr)+viaTxt(f.via)+'</div>'
      + '<div class="sub">'+esc(f.flight)+' &middot; dep '+fmtClock(f.dep_sched)+' &middot; <span class="src src-'+f.source+'">'+srcLabel(f.source)+'</span></div>'
      + '<div class="why">'+esc(f.why)+'</div>'
      + ((t.cabin && t.cabin.F && f.cabin && !f.cabin.F)
          ? '<div class="why nof">&#9888; No First on this flight &mdash; Lufthansa publishes it without a First cabin (a route without First).</div>' : '')
      + '</div>'
      + '<div class="tail">' + (f.published
          ? (WATCH.has(f.published) ? '<span class="star">&#9733;</span>' : '')+tlink(f.published)
            + '<span class="tbadge '+tcls(f.type)+'">'+esc(f.type)+'</span>'
            + (f.published_allegris ? '<span class="abadge" title="Allegris cabin">ALLEGRIS</span>' : '')
          : '<span class="nopub">not published yet</span>')
      + cabinBadges(f.cabin || f.published_cabin) + '</div>'
      + '<div class="miniconf pchip" title="chance '+esc(t.reg)+' operates this flight"><span class="p">'+pctText(f.p)+'</span><span class="cn">'+esc(t.reg)+'</span></div></div>';
  });
  if(fs.length > 150) h += '<div class="fnote">150 of '+fs.length+' flights shown</div>';
  R.innerHTML = h;
  R.querySelectorAll('#psort button').forEach(b => b.addEventListener('click', () => { PSORT = b.dataset.v; drawPlan(); }));
}
function planQuery(){
  const q = new URLSearchParams({target: $('plan-in').value.trim().toUpperCase()});
  const dp = tokDep.codes().join(','), ar = tokArr.codes().join(',');
  if(dp) q.set('dep', dp);
  if(ar) q.set('arr', ar);
  if($('plan-from').value) q.set('from', $('plan-from').value);
  if($('plan-to').value) q.set('to', $('plan-to').value);
  return q;
}

function search(){
  let url;
  if(mode==='plan'){
    const q = planQuery();
    if(!q.get('target')) return;
    saveTarget(q.get('target'));
    history.replaceState(null, '', '?' + q.toString());   // the plan is its own link
    $('results').replaceChildren(state('wait', 'Ranking flights'));
    fetch('/api/book/plan?' + q.toString()).then(r => r.json()).then(renderPlan)
      .catch(() => { $('results').replaceChildren(state('bad', 'The plan could not be loaded')); });
    return;
  }
  if(mode==='tail'){
    const r = $('tail-in').value.trim().toUpperCase();
    if(!r){ return; }
    url = '/api/book?reg=' + encodeURIComponent(r);
  } else if(mode==='map'){
    if(!MAP.sel){ return; }              // the marker is the query
    url = '/api/book?dep=' + encodeURIComponent(MAP.sel);
  } else {
    const dp = tokDep.codes().join(','), ar = tokArr.codes().join(',');
    if(!dp && !ar){ return; }
    url = '/api/book?dep=' + encodeURIComponent(dp) + '&arr=' + encodeURIComponent(ar);
  }
  $('results').replaceChildren(state('wait', 'Searching'));
  fetch(url).then(r=>r.json()).then(renderResults)
    .catch(()=>{ $('results').replaceChildren(state('bad', 'The flights could not be loaded')); });
}

document.querySelectorAll('#modes button').forEach(b => b.addEventListener('click', () => setMode(b.dataset.mode)));
$('search-btn').addEventListener('click', search);
// route-mode inputs handle Enter themselves (suggestion pick vs. search)
$('tail-in').addEventListener('keydown', e => { if(e.key==='Enter') search(); });
$('plan-in').addEventListener('keydown', e => { if(e.key==='Enter') search(); });

function fmtD(iso){ if(!iso) return ''; return new Date(iso+'T00:00:00Z').toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'}); }
// a result opens the flight dialog (flight.js)
$('results').addEventListener('click', e => {
  if(e.target.closest('a')) return;              // a tail link navigates instead
  const c = e.target.closest('.fcard');
  if(c && c.dataset.src !== 'projected') Flight.open(c.dataset.num, c.dataset.fdate);  // a projection has no FIS record yet
});

/* ── Map mode ─────────────────────────────────────────────────────────
   Basemap and markers share one coordinate system — x = longitude,
   y = -latitude (equirectangular) — so pan and zoom are nothing but the
   viewBox, and a marker needs no projection maths. Both payloads come from
   this app, so the page makes no third-party request. Clicking a marker runs
   the ordinary dep= search: results, filters and the modal are shared with
   route mode, and the marker can never disagree with the cards it opens. */
const SVGNS = 'http://www.w3.org/2000/svg';
// minW caps the zoom: the outline is Natural Earth 1:110m, so past roughly a
// 12-degree window it degrades into abstract polygons. 12 degrees still puts
// ~50px between neighbours as close as HND/NRT or JFK/EWR.
const MAP = { on:false, svg:null, marks:[], byCode:{}, sel:null, pending:null,
              moved:0, minW:12, V:{x:-180,y:-70,w:360},
              layer:'up', up:null, net:null, tail:null, gLayer:null };

// stops of a flight of several legs (LH568 FRA -> SSG via LOS)
function viaTxt(v){ return (v && v.length) ? ' via '+v.map(esc).join(', ') : ''; }
function svgEl(tag, attrs){
  const e = document.createElementNS(SVGNS, tag);
  for(const k in attrs) e.setAttribute(k, attrs[k]);
  return e;
}
/* when the viewport outgrows the content, centre instead of clamping */
function clampRange(v, lo, hi){ return lo > hi ? (lo+hi)/2 : Math.min(hi, Math.max(lo, v)); }

/* Sets the viewBox, then re-sizes everything that must stay constant on
   screen: inside a scaled viewBox a CSS px is a user unit, so radii and
   label sizes would otherwise grow with the zoom. */
function mapDraw(){
  const svg = MAP.svg, r = svg.getBoundingClientRect(), V = MAP.V;
  const wpx = Math.max(r.width, 1);
  V.w = Math.min(360, Math.max(MAP.minW, V.w));
  const h = V.w * (r.height / wpx);
  V.x = clampRange(V.x, -186, 186 - V.w);
  V.y = clampRange(V.y, -88, 62 - h);
  svg.setAttribute('viewBox', V.x+' '+V.y+' '+V.w+' '+h);
  const k = V.w / wpx;                       // user units per screen pixel
  MAP.marks.forEach(m => {
    m.c.setAttribute('r', (m.r * k).toFixed(4));
    // label the hubs and the pick always, everything else once zoomed in —
    // and never label what is scrolled off the canvas
    const inView = m.lon > V.x - 2 && m.lon < V.x + V.w + 2
                && -m.lat > V.y - 2 && -m.lat < V.y + h + 2;
    const show = inView && (V.w < 100 || m.big || m.code === MAP.sel);
    m.t.style.display = show ? '' : 'none';
    if(show){
      m.t.setAttribute('x', (m.lon + (m.r + 3.5) * k).toFixed(4));
      m.t.setAttribute('y', (-m.lat + 3.7 * k).toFixed(4));
      m.t.setAttribute('font-size', (10.5 * k).toFixed(4));
      m.t.setAttribute('stroke-width', (3 * k).toFixed(4));
    }
  });
}

function mapFit(){
  const r = MAP.svg.getBoundingClientRect(), asp = r.height / Math.max(r.width, 1);
  if(!MAP.marks.length){ MAP.V = {x:-180, y:-70, w:360}; mapDraw(); return; }
  let x0=180, x1=-180, y0=90, y1=-90;
  MAP.marks.forEach(m => {
    x0 = Math.min(x0, m.lon); x1 = Math.max(x1, m.lon);
    y0 = Math.min(y0, -m.lat); y1 = Math.max(y1, -m.lat);
  });
  const spanX = (x1-x0) * 1.10, spanY = (y1-y0) * 1.18;
  let w = Math.max(spanX, spanY / Math.max(asp, 0.01), 20);
  // on a tall canvas, fitting every longitude buys mostly empty ocean — cap the
  // latitude shown at ~1.9x what the network needs and let the user pan instead
  w = Math.max(Math.min(w, (spanY * 1.9) / Math.max(asp, 0.01)), 20, MAP.minW);
  MAP.V = { x:(x0+x1)/2 - w/2, y:(y0+y1)/2 - (w*asp)/2, w:w };
  mapDraw();
}

function mapZoomAt(cx, cy, f){
  const r = MAP.svg.getBoundingClientRect(), V = MAP.V;
  const wpx = Math.max(r.width, 1), k = V.w / wpx;
  const wx = V.x + (cx - r.left) * k, wy = V.y + (cy - r.top) * k;
  const nw = Math.min(360, Math.max(MAP.minW, V.w * f)), nk = nw / wpx;
  V.x = wx - (cx - r.left) * nk;
  V.y = wy - (cy - r.top) * nk;
  V.w = nw;
  mapDraw();
}
function mapZoomCentre(f){
  const r = MAP.svg.getBoundingClientRect();
  mapZoomAt(r.left + r.width/2, r.top + r.height/2, f);
}

function mapTip(m, ev){
  const tip = $('maptip'), a = m.a;
  if(a.net){
    tip.innerHTML = '<b>'+esc(a.code)+'</b> '+esc(a.name)+'<br>'+a.count+' leg'+(a.count===1?'':'s')
      + ' in '+MAP.net.weeks+' weeks'+(MAP.tail ? ' by '+esc(MAP.tail) : '')
      + '<br><span class="d">click for its upcoming departures</span>';
  } else {
    const nd = a.dests.length;
    tip.innerHTML = '<b>'+esc(a.code)+'</b> '+esc(a.name)
      + '<br>'+a.count+' departure'+(a.count===1?'':'s')+(a.count !== a.flights ? ' (of '+a.flights+', filtered)' : '')
      + ' &middot; '+nd+' destination'+(nd===1?'':'s')
      + (a.types.length ? '<br><span class="d">'+esc(a.types.join(', '))
          + (a.next ? ' &middot; from '+fmtD(a.next) : '')+'</span>' : '');
  }
  tip.classList.add('show');
  const wr = $('mapwrap').getBoundingClientRect();
  const x = Math.min(ev.clientX - wr.left + 14, wr.width - tip.offsetWidth - 8);
  const y = Math.min(ev.clientY - wr.top + 14, wr.height - tip.offsetHeight - 8);
  tip.style.left = Math.max(6, x)+'px';
  tip.style.top = Math.max(6, y)+'px';
}
function mapHideTip(){ $('maptip').classList.remove('show'); }

/* Two layers on one basemap. Upcoming: airports with a published departure
   in the next days (marker = departures). Network: routes flown in the last
   8 weeks (line width = legs), from the tail that actually flew each leg.
   Both honour the type/Allegris/First filters and the one-tail box, so "where
   does D-ABYN go" and "where do Allegris A350s fly" are one click each. */
function mapBuild(world, data){
  const svg = MAP.svg;
  svg.innerHTML = '';
  svg.appendChild(svgEl('path', {d:world.path, class:'land'}));
  MAP.gLayer = svgEl('g', {});
  svg.appendChild(MAP.gLayer);
  MAP.up = data;
  mapRender(true);
}
function mapKeep(type, reg, alleg, first){      // first === undefined: caller counts it
  if(FILT.fams && !FILT.fams.has(famOf(type))) return false;
  if(FILT.alleg!=='any' && !!alleg !== (FILT.alleg==='yes')) return false;
  if(first !== undefined && FILT.first!=='any' && !!first !== (FILT.first==='yes')) return false;
  return !MAP.tail || reg === MAP.tail;
}
/* a gentle northward arc, so opposite directions and parallel routes separate */
function arcPath(A, B){
  const x1 = A.lon, y1 = -A.lat, x2 = B.lon, y2 = -B.lat;
  const dx = x2 - x1, dy = y2 - y1, len = Math.hypot(dx, dy) || 1;
  let nx = -dy / len, ny = dx / len;
  if(ny > 0){ nx = -nx; ny = -ny; }
  const k = 0.16 * len, cx = (x1 + x2) / 2 + nx * k, cy = (y1 + y2) / 2 + ny * k;
  return 'M'+x1+','+y1+' Q'+cx.toFixed(3)+','+cy.toFixed(3)+' '+x2+','+y2;
}
function mapRender(fit){
  const g = MAP.gLayer;
  if(!g) return;
  g.innerHTML = '';
  const gLines = svgEl('g', {}), gMk = svgEl('g', {}), gLbl = svgEl('g', {});
  g.appendChild(gLines); g.appendChild(gMk); g.appendChild(gLbl);
  MAP.marks = []; MAP.byCode = {};
  if(MAP.layer === 'net' && !MAP.net) return;      // still loading; mapLayer renders it
  let pts = [], unplaced = [];
  if(MAP.layer === 'up'){
    (MAP.up.airports || []).forEach(a => {
      const n = (a.rows || []).filter(r => mapKeep(r[0], r[1], r[2], r[3])).length;
      if(n) pts.push(Object.assign({}, a, {count:n}));
    });
    unplaced = (MAP.up.unplaced || []).map(u => u.code);
    $('map-count').textContent = pts.length + ' airport'+(pts.length===1?'':'s')+' with published departures';
  } else {
    const net = MAP.net, agg = {}, keep = [];
    net.routes.forEach(rt => {
      let n = 0;
      // First is per flight (routes without First publish none): count the legs
      for(const reg in rt.tails){
        const c = net.tails[reg] || {};
        if(!mapKeep(c.type, reg, c.allegris)) continue;
        const f = (rt.first && rt.first[reg]) || 0;
        n += FILT.first === 'yes' ? f : (FILT.first === 'no' ? rt.tails[reg] - f : rt.tails[reg]);
      }
      if(n && net.airports[rt.a] && net.airports[rt.b]) keep.push([rt, n]);
    });
    const maxn = keep.reduce((m, x) => Math.max(m, x[1]), 1);
    keep.sort((x, y) => x[1] - y[1]).forEach(([rt, n]) => {   // busiest drawn last, on top
      const A = net.airports[rt.a], B = net.airports[rt.b], d = arcPath(A, B);
      const w = (1 + 5 * Math.sqrt(n / maxn)).toFixed(2);
      const line = svgEl('path', {d:d, class:'rt', 'stroke-width':w});
      const hit = svgEl('path', {d:d, class:'rthit', tabindex:'0', role:'button',
        'aria-label':rt.a+' to '+rt.b+', '+n+' legs'});
      const info = {rt:rt, n:n};
      hit.addEventListener('click', () => { if(MAP.moved < 5) mapRoute(rt.a, rt.b); });
      hit.addEventListener('keydown', e => { if(e.key === 'Enter' || e.key === ' '){ e.preventDefault(); mapRoute(rt.a, rt.b); } });
      hit.addEventListener('pointerenter', ev => { line.classList.add('on'); mapRouteTip(info, ev); });
      hit.addEventListener('pointermove', ev => mapRouteTip(info, ev));
      hit.addEventListener('pointerleave', () => { line.classList.remove('on'); mapHideTip(); });
      gLines.appendChild(line); gLines.appendChild(hit);
      [rt.a, rt.b].forEach(c => { agg[c] = (agg[c] || 0) + n; });
    });
    pts = Object.keys(agg).map(c => ({code:c, name:net.airports[c].name, lat:net.airports[c].lat,
                                      lon:net.airports[c].lon, count:agg[c], net:true}));
    unplaced = net.unplaced || [];
    $('map-count').textContent = keep.length + ' route'+(keep.length===1?'':'s')+' flown in '+net.weeks+' weeks';
  }
  // busiest first, so the small outstations paint on top and stay clickable
  pts.sort((a, b) => b.count - a.count);
  const max = pts.length ? pts[0].count : 1;
  pts.forEach((a, i) => {
    const c = svgEl('circle', {cx:a.lon, cy:-a.lat, r:0, class:'mk', tabindex:'0',
      role:'button', 'aria-label':a.code+' '+a.name+', '+a.count+(a.net ? ' legs' : ' departures')});
    const t = svgEl('text', {x:a.lon, y:-a.lat, class:'mklbl'});
    t.textContent = a.code;
    const m = {code:a.code, lon:a.lon, lat:a.lat, a:a, c:c, t:t,
               r:3.4 + 3.8 * Math.sqrt(a.count / max), big:i < 8};
    c.classList.toggle('sel', a.code === MAP.sel);
    c.addEventListener('click', () => { if(MAP.moved < 5) mapSelect(a.code); });
    c.addEventListener('keydown', e => {
      if(e.key === 'Enter' || e.key === ' '){ e.preventDefault(); mapSelect(a.code); } });
    c.addEventListener('pointerenter', ev => mapTip(m, ev));
    c.addEventListener('pointermove', ev => mapTip(m, ev));
    c.addEventListener('pointerleave', mapHideTip);
    gMk.appendChild(c); gLbl.appendChild(t);
    MAP.marks.push(m); MAP.byCode[a.code] = a;
  });
  $('mapnote').style.display = unplaced.length ? '' : 'none';
  if(unplaced.length){
    $('mapnote').innerHTML = unplaced.length+' airport'+(unplaced.length===1?'':'s')
      + ' not placed on the map ('+unplaced.map(esc).join(', ')+') &mdash; reachable from route mode.';
  }
  if(fit) mapFit(); else mapDraw();
}
function mapRouteTip(info, ev){
  const rt = info.rt, tip = $('maptip');
  const tails = Object.entries(rt.tails).sort((x, y) => y[1] - x[1]);
  const types = Object.entries(rt.types).map(([k, v]) => k+' '+v).join(', ');
  const mine = MAP.tail ? (rt.tails[MAP.tail] || 0) : null;
  tip.innerHTML = '<b>'+esc(rt.a)+' &harr; '+esc(rt.b)+'</b><br>'+info.n+' leg'+(info.n===1?'':'s')+' in '+MAP.net.weeks+' weeks'
    + (mine != null ? ' by '+esc(MAP.tail)+' ('+Math.round(100*mine/rt.n)+'% of all '+rt.n+')' : '')
    + '<br><span class="d">'+esc(types)+' &middot; '+tails.length+' tail'+(tails.length===1?'':'s')+'</span>'
    + '<br><span class="d">click to '+(MAP.tail ? 'plan '+esc(MAP.tail)+' on this route' : 'see upcoming flights')+'</span>';
  tip.classList.add('show');
  const wr = $('mapwrap').getBoundingClientRect();
  tip.style.left = Math.max(6, Math.min(ev.clientX - wr.left + 14, wr.width - tip.offsetWidth - 8))+'px';
  tip.style.top = Math.max(6, Math.min(ev.clientY - wr.top + 14, wr.height - tip.offsetHeight - 8))+'px';
}
/* a route line opens the route: planned for the one-tail filter if set, else
   the published flights on it (either direction via the hub side first) */
function mapRoute(a, b){
  mapHideTip();
  tokDep.set([a]); tokArr.set([b]);
  if(MAP.tail){ $('plan-in').value = MAP.tail; setMode('plan'); }
  else setMode('route');
  search();
}
function mapLayer(v){
  MAP.layer = v;
  $('map-layer').querySelectorAll('button').forEach(x => x.classList.toggle('is-on', x.dataset.v === v));
  $('map-hint').textContent = v === 'up' ? 'Marker size = departures published; click an airport to see them'
    : 'Line width = legs flown; click a line to open the route';
  if(v === 'net' && !MAP.net){
    $('map-count').textContent = 'Loading the network…';
    fetch('/api/book/network').then(r => r.json()).then(d => {
      if(d.error){ $('map-count').textContent = d.error; return; }
      MAP.net = d; if(MAP.layer === 'net') mapRender(true);
    }).catch(() => { $('map-count').textContent = 'Network failed to load.'; });
    return;
  }
  mapRender(true);
}
$('map-layer').querySelectorAll('button').forEach(b => b.addEventListener('click', () => mapLayer(b.dataset.v)));
function mapTailSet(){
  const v = $('map-tail').value.trim().toUpperCase();
  MAP.tail = v || null;
  if(MAP.gLayer) mapRender(false);
}
$('map-tail').addEventListener('change', mapTailSet);
$('map-tail').addEventListener('keydown', e => { if(e.key === 'Enter') mapTailSet(); });

function mapWire(){
  const svg = MAP.svg, ptrs = new Map();
  let last = null, pinch = 0;
  function pinchState(){
    const v = [...ptrs.values()];
    return {x:(v[0].x+v[1].x)/2, y:(v[0].y+v[1].y)/2,
            d:Math.hypot(v[0].x-v[1].x, v[0].y-v[1].y)};
  }
  // Deliberately no setPointerCapture: capturing retargets the compatibility
  // click to the <svg>, so a marker's own click would never fire. Tracking the
  // drag on window instead keeps clicks intact and still follows a pointer
  // that leaves the map mid-drag.
  svg.addEventListener('pointerdown', e => {
    ptrs.set(e.pointerId, {x:e.clientX, y:e.clientY});
    MAP.moved = 0;
    if(ptrs.size === 1){ last = {x:e.clientX, y:e.clientY}; svg.classList.add('grabbing'); }
    else if(ptrs.size === 2){ pinch = pinchState().d; last = null; }
  });
  window.addEventListener('pointermove', e => {
    if(!ptrs.has(e.pointerId)) return;
    ptrs.set(e.pointerId, {x:e.clientX, y:e.clientY});
    if(ptrs.size >= 2){                       // pinch zoom about the midpoint
      const p = pinchState();
      if(pinch > 0 && p.d > 0){ mapZoomAt(p.x, p.y, pinch / p.d); MAP.moved += 10; }
      pinch = p.d;
      return;
    }
    if(!last) return;
    const r = svg.getBoundingClientRect(), k = MAP.V.w / Math.max(r.width, 1);
    const dx = e.clientX - last.x, dy = e.clientY - last.y;
    MAP.moved += Math.abs(dx) + Math.abs(dy);   // tells a drag from a click
    if(MAP.moved > 4) mapHideTip();
    MAP.V.x -= dx * k; MAP.V.y -= dy * k;
    last = {x:e.clientX, y:e.clientY};
    mapDraw();
  });
  function up(e){
    if(!ptrs.delete(e.pointerId)) return;
    if(ptrs.size < 2) pinch = 0;
    if(ptrs.size === 0){ last = null; svg.classList.remove('grabbing'); }
  }
  window.addEventListener('pointerup', up);
  window.addEventListener('pointercancel', up);
  svg.addEventListener('wheel', e => {
    e.preventDefault();
    mapZoomAt(e.clientX, e.clientY, Math.exp(e.deltaY * 0.0016));
  }, {passive:false});
  $('map-in').addEventListener('click', () => mapZoomCentre(1/1.5));
  $('map-out').addEventListener('click', () => mapZoomCentre(1.5));
  $('map-fit').addEventListener('click', mapFit);
  window.addEventListener('resize', () => { if(MAP.marks.length) mapDraw(); });
}

function mapSelect(code){
  if(!MAP.byCode[code]){ MAP.pending = code; return; }   // still loading
  MAP.sel = code;
  MAP.marks.forEach(m => m.c.classList.toggle('sel', m.code === code));
  const a = MAP.byCode[code];
  $('map-pick').innerHTML = esc(a.code)+' &middot; '+esc(a.name);
  mapDraw();                   // the pick keeps its label at any zoom
  search();
}

function mapInit(){
  if(MAP.on) return;
  MAP.on = true;
  MAP.svg = $('mapsvg');
  mapWire();
  Promise.all([
    fetch('/book/world.json').then(r => r.json()),
    fetch('/api/book/map').then(r => r.json()),
  ]).then(([w, d]) => {
    if(d.error){ $('map-count').textContent = d.error; return; }
    mapBuild(w, d);
    if(MAP.layer === 'net') mapLayer('net');   // a ?map=net link asked before the basemap was in
    if(MAP.pending){ const c = MAP.pending; MAP.pending = null; mapSelect(c); }
  }).catch(() => { $('map-count').textContent = 'Map failed to load.'; });
}

/* Prefill + auto-search from URL (?target=[&dep=&arr=&from=&to=], ?reg=, ?dep=&arr=,
   ?loc=, or ?map=net|up[&tail=]) so links land on results */
(function(){
  const p = new URLSearchParams(location.search);
  if(p.get('map')){
    if(p.get('tail')){ $('map-tail').value = p.get('tail').trim().toUpperCase(); MAP.tail = $('map-tail').value; }
    MAP.layer = p.get('map') === 'net' ? 'net' : 'up';
    $('map-layer').querySelectorAll('button').forEach(x => x.classList.toggle('is-on', x.dataset.v === MAP.layer));
    setMode('map');
  }
  else if(p.get('target')){ setMode('plan'); $('plan-in').value = p.get('target');
    tokDep.set((p.get('dep')||'').split(/[,\s]+/));
    tokArr.set((p.get('arr')||'').split(/[,\s]+/));
    if(p.get('from')) $('plan-from').value = p.get('from');
    if(p.get('to')) $('plan-to').value = p.get('to');
    search(); }
  else if(p.get('reg')){ setMode('tail'); $('tail-in').value = p.get('reg'); search(); }
  else if(p.get('loc')){ setMode('map'); mapSelect(p.get('loc').trim().toUpperCase()); }
  else if(p.get('dep') || p.get('arr')){ setMode('route');
    tokDep.set((p.get('dep')||'').split(/[,\s]+/));
    tokArr.set((p.get('arr')||'').split(/[,\s]+/)); search(); }
})();
})();

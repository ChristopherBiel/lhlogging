// /airframe/<reg> — one long-haul tail: its cabin and hard product, what is
// published for it next, where it flies, what the feed has said about its
// cabin, and its last flights, from /api/airframe/<reg>. The page marks no
// entry of the top bar; its lead links on to Book, Insights and the Schedule.
(() => {
'use strict';

const {$, el, esc, section, state} = window.Page;
const REG = decodeURIComponent(location.pathname.split('/').pop() || '').toUpperCase();
const main = document.querySelector('main');
$('reg').textContent = REG;

function viaTxt(v){ return (v && v.length) ? ' via '+v.map(esc).join(', ') : ''; }
function fmtDay(iso){ if(!iso) return ''; return new Date(iso.length===10 ? iso+'T00:00:00Z' : iso).toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'short',timeZone:'UTC'}); }
function fmtClock(iso){ if(!iso) return ''; return new Date(iso).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
function ago(iso){
  const m = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
  if(m < 60) return m+' min ago';
  if(m < 48*60) return Math.round(m/60)+' h ago';
  return Math.round(m/1440)+' days ago';
}
function yn(v, extra){ return v==null ? '&mdash;' : (v ? 'yes'+(extra?' &middot; '+esc(extra):'') : 'no'); }
function tailLink(r){ return r===REG ? '<b>'+esc(r)+'</b>' : '<a class="tail-link" href="/airframe/'+encodeURIComponent(r)+'">'+esc(r)+'</a>'; }
const spec = (k, v) => '<div><dt>'+k+'</dt><dd>'+v+'</dd></div>';
const empty = words => '<p class="empty">'+words+'</p>';
const aside = html => '<p class="aside">'+html+'</p>';

function cabinBlock(d){
  const c = d.cabin || {};
  const rows = [['First','F'],['Business','C'],['Premium Economy','E'],['Economy','M']];
  const tot = rows.reduce((s,[,k]) => s + (c[k]||0), 0);
  let h = '<dl class="spec">';
  rows.forEach(([n,k]) => { h += spec(n, c[k] ? '<b>'+c[k]+'</b> seats' : '&mdash;'); });
  h += spec('Total', (tot ? '<b>'+tot+'</b>' : '&mdash;')+(d.seat_config ? ' &middot; '+esc(d.seat_config) : ''));
  h += spec('Allegris', yn(d.allegris));
  const a = d.amenities || {};
  h += spec('Wi-Fi', yn(a.wifi, a.wifi_type));
  h += spec('Seatback IFE', yn(a.ife));
  h += spec('USB power', yn(a.usb_power));
  if(a.livery) h += spec('Livery', esc(a.livery));
  if(d.seat_map) h += spec('Seat map', '<a class="tail-link" href="'+esc(d.seat_map.url)+'" target="_blank" rel="noopener">'
    + (d.seat_map.exact ? esc(d.seat_map.code)+' layout on aeroLOPA' : 'Lufthansa seat maps on aeroLOPA')+' &rarr;</a>');
  h += '</dl>';
  if(d.first_not_sold && d.first_not_sold.length){
    h += aside('<b>First isn&rsquo;t sold on every flight.</b> First is a route product: on these routes the '+esc(d.type)+' fleet was published without a First cabin (last '+d.window_weeks+' weeks, legs without First / legs): '
      + d.first_not_sold.map(r => esc(r.dep)+'&rarr;'+esc(r.arr)+' '+r.without+'/'+r.legs).join(', ')+'.');
  }
  if(d.variant){
    const others = d.variant.tails.filter(t => t !== REG);
    h += aside('Cabin variant <b>'+esc(d.variant.label)+'</b> of '+d.variants+' on the '+esc(d.type)+' fleet'
      + (others.length ? ' &mdash; the same cabin as '+others.map(tailLink).join(', ')+'.' : ' &mdash; the only tail with this cabin.'));
  }
  h += aside('Everything above is what Lufthansa&rsquo;s own flight-status feed publishes for this tail; the layout is the one it is most often sold with'
    + (d.amenities_seen ? ' (last seen '+ago(d.amenities_seen)+')' : '')+'.');
  h += '<div class="photo" id="photo"><button class="fp-btn" id="photo-btn" type="button">Show photo</button>'
    + '<p class="photo__credit">Loads from planespotters.net &mdash; nothing is requested from them until you click.</p></div>';
  return h;
}
// How the tail stands, for the lead's marker and the sentence under its name.
function standing(d){
  const s = d.status || {};
  if(s.idle_days == null) return ['note', 'No recent flight', 'No operated flight in the last weeks.'];
  if(s.idle_days > 3) return ['bad', 'Not flying', 'Has not flown for '+Math.floor(s.idle_days)+' days &mdash; possibly in maintenance.'];
  return ['ok', 'In service', 'Last flew '+(s.idle_days < 1 ? 'within the last day' : Math.round(s.idle_days)+' day'+(Math.round(s.idle_days)===1?'':'s')+' ago')+'.'];
}
function position(d){
  const a = (d.status || {}).adsb;
  return a ? ' Last ADS-B position '+ago(a.at)+': '+(a.on_ground ? 'on the ground'+(a.airport ? ' at <b>'+esc(a.airport)+'</b>' : '') : 'airborne'+(a.callsign ? ' as '+esc(a.callsign) : ''))+'.' : '';
}
function upcomingBlock(d){
  const u = d.upcoming || [];
  if(!u.length) return empty('Not published on any flight in the next days.');
  let h = '<table class="table"><tr><th>Date</th><th>Flight</th><th>Route</th><th class="num">Dep</th><th class="num">Holds</th></tr>';
  u.forEach(l => {
    h += '<tr><td>'+fmtDay(l.flight_date)+'</td><td class="mono">'+esc(l.flight)+'</td><td>'+esc(l.dep)+' &rarr; '+esc(l.arr)+viaTxt(l.via)+'</td>'
      + '<td class="num">'+fmtClock(l.dep_sched)+'</td>'
      + '<td class="num pct" title="'+(l.hold ? 'how often a tail published '+l.hold.band+'h out still flies it ('+l.hold.basis+', n='+l.hold.n+')' : '')+'">'+(l.hold ? Math.round(l.hold.p*100)+'%' : '&mdash;')+'</td></tr>';
  });
  return h + '</table>' + aside('Holds = the chance the tail published now is still the one that flies.');
}
function routesBlock(d){
  const r = d.routes || [];
  if(!r.length) return empty('No operated legs in the last '+d.window_weeks+' weeks.');
  let h = '<table class="table"><tr><th>Route</th><th class="num">Legs</th><th class="num">Of all legs on it</th><th class="num"></th></tr>';
  r.forEach(x => {
    const q = new URLSearchParams({target: REG, dep: x.dep, arr: x.arr});
    h += '<tr><td>'+esc(x.dep)+' &rarr; '+esc(x.arr)+'</td><td class="num">'+x.k+'</td>'
      + '<td class="num"><span class="pct">'+Math.round(x.share*100)+'%</span> of '+x.n+'</td>'
      + '<td class="num"><a class="tail-link" href="/book?'+q.toString()+'">plan</a></td></tr>';
  });
  return h + '</table>';
}
function historyBlock(d){
  const hs = d.cabin_history || [];
  if(!hs.length) return empty('No cabin data yet.');
  const refits = hs.filter(x => !x.glitch && !x.concurrent);
  let h = '<table class="table"><tr><th>Layout</th><th>Allegris</th><th>Sub-type</th><th class="num">From</th><th class="num">To</th></tr>';
  hs.forEach(x => {
    const tag = x.glitch ? ' &middot; one-off' : (x.concurrent ? ' &middot; alongside' : '');
    h += '<tr class="'+(x.glitch?'dim':'')+'"><td class="mono">'+esc(x.seat_config||'?')+'</td><td>'+(x.allegris?'yes':'no')+'</td><td class="mono">'+esc(x.sub_type||'')+'</td>'
      + '<td class="num">'+fmtDay(x.first_seen)+'</td><td class="num">'+fmtDay(x.last_seen)+tag+'</td></tr>';
  });
  h += '</table>';
  if(hs.some(x => x.concurrent)) h += aside('Layouts marked <b>alongside</b> were published at the same time as another: the same cabin published differently on some flights (no First on a route without it), not a refit.');
  if(refits.length > 1) h += aside('The cabin changed while we were watching &mdash; most recently from '+fmtDay(refits[refits.length-1].first_seen)+'.');
  return h;
}
function recentBlock(d){
  const r = d.recent || [];
  if(!r.length) return empty('No operated legs in the last weeks.');
  let h = '<table class="table"><tr><th>Date</th><th>Flight</th><th>Route</th></tr>';
  r.forEach(l => { h += '<tr><td>'+fmtDay(l.flight_date)+'</td><td class="mono">'+esc(l.flight)+'</td><td>'+esc(l.dep)+' &rarr; '+esc(l.arr)+viaTxt(l.via)+'</td></tr>'; });
  return h + '</table>';
}
// A photo is held by four registration marks, which close into a frame on its link.
function loadPhoto(){
  const box = $('photo'), credit = html => '<p class="photo__credit">'+html+'</p>';
  box.innerHTML = credit('Loading from planespotters.net&hellip;');
  fetch('/api/airframe/'+encodeURIComponent(REG)+'/photo').then(r => r.json()).then(p => {
    if(p.error || !p.photo){ box.innerHTML = credit(p.error ? esc(p.error) : 'No photo of '+esc(REG)+' on planespotters.net.'); return; }
    const ph = p.photo;
    box.innerHTML = '<a class="fp-reg photo__frame" href="'+esc(ph.link)+'" target="_blank" rel="noopener noreferrer">'
      + '<img src="'+esc(ph.src)+'" alt="'+esc(REG)+' on planespotters.net"'+(ph.size ? ' width="'+ph.size.width+'" height="'+ph.size.height+'"' : '')+'></a>'
      + credit('&copy; '+esc(ph.photographer || 'photographer')+' &middot; <a href="'+esc(ph.link)+'" target="_blank" rel="noopener noreferrer">planespotters.net</a>');
  }).catch(() => { box.innerHTML = credit('Photo lookup failed.'); });
}

// One section per part. A dated list is ink in Faceplate's code; the fields
// (at intensity 04) alternate so that neighbours differ.
function part(title, note, html, kind, field){
  const sec = section({title, note, kind, field});
  sec.body.innerHTML = html;
  return sec.root;
}
// the lead's paragraph holds the marker while there is nothing to say yet
const mark = (level, words) => el('span', 'fp-status state state--' + level, words);
const link = (href, words) => '<li><a href="'+href+'">'+words+' <span class="fp-arrow">&rarr;</span></a></li>';

fetch('/api/airframe/'+encodeURIComponent(REG)).then(r => r.json()).then(d => {
  if(d.error){ $('about').replaceChildren(mark('bad', d.error)); return; }
  document.title = d.reg + ' | LH Fleet';
  const [level, word, sentence] = standing(d);
  $('reg').textContent = d.reg;
  $('state').replaceChildren(state(level, word));
  $('lede').textContent = [d.fis_type, d.sub_type ? 'sub-fleet '+d.sub_type : null, d.serial ? 'MSN '+d.serial : null, d.model].filter(Boolean).join(' · ');
  $('about').innerHTML = (d.type ? '<span class="fp-chip">'+esc(d.type)+'</span> ' : '') + (d.allegris ? '<span class="fp-chip">Allegris</span> ' : '')
    + sentence + position(d);
  $('about').className = 'fp-body';
  $('links').innerHTML = link('/book?target='+encodeURIComponent(d.reg), 'Plan a booking on '+esc(d.reg))
    + link('/insights?type='+encodeURIComponent(d.type_code||'')+'&amp;reg='+encodeURIComponent(d.reg), 'Insights')
    + link('/schedule', 'Schedule');
  main.append(
    part('Cabin & hard product', '', cabinBlock(d)),
    part('Published next', 'From the flight-status feed', upcomingBlock(d), 'ink', 'tint'),
    part('Where it flies', 'Last '+d.window_weeks+' weeks', routesBlock(d), '', 'tint'),
    part('Cabin history', 'Every layout the feed has published for it', historyBlock(d), 'ink'),
    part('Recent flights', 'Operated, newest first', recentBlock(d), 'ink', 'tint'));
  $('photo-btn').addEventListener('click', loadPhoto);
}).catch(() => { $('about').replaceChildren(mark('bad', 'The airframe could not be loaded')); });
})();

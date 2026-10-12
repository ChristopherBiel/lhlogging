// The flight dialog that Schedule and Book share (window.Flight): what is
// published for one flight — the tail, its cabin, the times, how likely the
// tail is to hold — its assignment history and, for a leg in the past, what
// ADS-B recorded. A dialog is a control surface with its own edge, so it is
// the one card on these pages.
window.Flight = (() => {
'use strict';

const {el, esc} = window.Page;
const bg = document.body.appendChild(el('div', 'dialog-bg'));
const box = bg.appendChild(el('div', 'dialog'));
box.setAttribute('role', 'dialog');
box.setAttribute('aria-modal', 'true');
const show = html => {
  box.innerHTML = '<button class="dialog__close" type="button" aria-label="Close">×</button>' + html;
  bg.classList.add('show');
};
const close = () => bg.classList.remove('show');
bg.addEventListener('click', e => { if (e.target === bg || e.target.closest('.dialog__close')) close(); });
document.addEventListener('keydown', e => { if (e.key === 'Escape') close(); });

function fmt(iso){ if(!iso) return '?'; const d=new Date(iso);
  return d.toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
function fmtD(iso){ if(!iso) return ''; return new Date(iso+'T00:00:00Z').toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'}); }
function isoDur(s){ if(!s) return ''; const m=s.match(/PT(?:(\d+)H)?(?:(\d+)M)?/); if(!m) return ''; return (m[1]?m[1]+'h':'')+(m[2]?String(m[2]).padStart(2,'0')+'m':''); }
const row = (k, v) => '<div><dt>'+k+'</dt><dd>'+v+'</dd></div>';
const head = (title, sub) => '<h2 class="dialog__title">'+title+'</h2><p class="dialog__sub">'+sub+'</p>';
const mark = (level, html) => '<p class="fp-status state state--'+level+'">'+html+'</p>';

// How likely the published tail is to stay: the share, in words, and its basis.
function hold(h, reg){
  if(!h) return '';
  const p=Math.round(h.p*100);
  const label=p>=85?'likely to hold':(p>=60?'could still change':'often changes');
  const basis=h.basis==='route'?'this route':(h.basis==='type'?'this aircraft type':'all tracked flights');
  return '<div class="hold"><span class="hold__p">'+p+'%</span><p class="hold__t">chance it stays <b>'+esc(reg||'this tail')+'</b> &middot; '+label
    +'<small>'+h.lead+'d before departure &middot; based on '+basis+' (n='+h.n+')</small></p></div>';
}
// plan against actual, for a leg whose departure is in the past
function actual(leg){
  if(!leg || !['tracked','deviation','missing'].includes(leg.status)) return '';
  let h='<p class="fp-label dialog__h">Actual track (ADS-B)</p>';
  if(leg.act){
    const dm=leg.act.delta, ds=(dm>0?'+':'')+dm+'m', dev=leg.status==='deviation';
    h+='<dl class="spec">'
      +row('Flew', esc(leg.act.dep)+' → '+esc(leg.act.arr)+(leg.act.cs?' · '+esc(leg.act.cs):''))
      +row('Off / On', fmt(leg.act.start)+' → '+fmt(leg.act.end))
      +row('vs plan', ds+(dev?' · deviation from '+esc(leg.dep)+'→'+esc(leg.arr):' · on planned route'))
      +'</dl>';
  } else h+='<p class="empty">No ADS-B track found yet for this leg.</p>';
  return h;
}
function render(d, leg){
  if(d.error) return show(mark('bad', esc(d.error)));
  let h=head(esc(d.flight)+(d.dep_iata?' · '+[d.dep_iata].concat(d.via||[],[d.arr_iata]).map(esc).join('→'):''),
             [d.dep_name,d.arr_name].filter(Boolean).map(esc).join(' → ')+' · '+fmtD(d.flight_date));
  if(d.reassigned) h+=mark('bad', 'Reassigned — originally <b>'+esc(d.original_reg||'?')+'</b>, now <b>'+esc(d.current_reg||'?')+'</b>');
  h+=hold(d.hold, d.current_reg);
  if(d.found){
    const dur=isoDur(d.duration);
    h+='<dl class="spec">';
    h+=row('Aircraft', esc(d.current_reg||'?')+(d.current_type?' · '+esc(d.current_type):'')+(d.allegris?' <span class="fp-chip">Allegris</span>':''));
    if(d.seat_map){ const sc=String(d.seat_map.code||'').replace(/[^A-Za-z0-9]/g,'');
      h+=row('Seat map','<a class="tail-link" href="'+esc(d.seat_map.url)+'" target="_blank" rel="noopener">'+(d.seat_map.exact?sc+' on aeroLOPA':'aeroLOPA (Lufthansa)')+' →</a>'); }
    if(d.cabin){
      const cb=[]; if(d.cabin.F)cb.push('<b>First '+d.cabin.F+'</b>'); if(d.cabin.C)cb.push('Business '+d.cabin.C);
      if(d.cabin.E)cb.push('Prem Eco '+d.cabin.E); if(d.cabin.M)cb.push('Economy '+d.cabin.M);
      h+=row('Cabin',cb.join(' · '));
    }
    if(d.legs&&d.legs.length>1){   // a flight of several legs: each with its own (local) times
      d.legs.forEach((g,i)=>{ const gd=isoDur(g.duration);
        h+=row('Leg '+(i+1)+' '+esc(g.dep)+'→'+esc(g.arr), fmt(g.dep_sched)+' → '+fmt(g.arr_sched)
          +(gd?' · '+gd:'')+(g.dep_gate?' · Gate '+esc(g.dep_gate):'')); });
    } else {
      h+=row('Departure',fmt(d.dep_sched)+(d.dep_term?' · T'+esc(d.dep_term):'')+(d.dep_gate?' · Gate '+esc(d.dep_gate):''));
      h+=row('Arrival',fmt(d.arr_sched)+(d.arr_term?' · T'+esc(d.arr_term):'')+(d.arr_gate?' · Gate '+esc(d.arr_gate):''));
      if(dur) h+=row('Flight time',dur);
    }
    if(d.status) h+=row('Status',esc(d.status));
    if(d.codeshares&&d.codeshares.length) h+=row('Codeshare',d.codeshares.map(esc).join(', '));
    if(d.prev) h+=row('Previous leg',esc(d.prev)+(d.prev_date?' ('+fmtD(d.prev_date)+')':''));
    h+='</dl>';
  } else h+='<p class="empty">No current assignment for this date.</p>';
  h+=actual(leg);
  if(d.history&&d.history.length){
    h+='<p class="fp-label dialog__h">Assignment history</p>';
    let last=null;                                   // the tail before this one; a look without a tail changes nothing
    d.history.forEach((x,i)=>{
      const tags=[x.type]; if(i===0) tags.push('originally planned'); if(i===d.history.length-1) tags.push('current');
      h+='<div class="fp-row"><time class="fp-row__key">'+fmtD(x.observed)+'</time><p class="fp-row__text"><b>'+esc(x.reg||'no tail published')
        +(x.reg && last && x.reg!==last?' <span class="fp-chip fp-chip--terra">new tail</span>':'')
        +(x.allegris?' <span class="fp-chip">Allegris</span>':'')+'</b><small>'+tags.filter(Boolean).map(esc).join(' · ')+'</small></p></div>';
      if(x.reg) last=x.reg;
    });
  }
  show(h);
}

// A flight of the plan: `leg` (optional) is the leg as the schedule loaded it,
// which carries what ADS-B recorded.
function open(num, fdate, leg){
  show(mark('wait', 'Loading'));
  fetch('/api/schedule/flight/LH/'+encodeURIComponent(num)+'/'+encodeURIComponent(fdate)).then(r=>r.json()).then(d=>render(d, leg||null))
    .catch(()=>show(mark('bad', 'The flight could not be loaded')));
}
// A recorded (ADS-B) leg with no plan behind it, from the bar's data attributes.
function openActual(ds){
  let h=head(esc(ds.cs||'Recorded flight')+(ds.dep?' · '+esc(ds.dep)+'→'+esc(ds.arr):''),
             ds.planfl?'Deviation — the tail flew this instead of its plan':'Unplanned — recorded ADS-B track, no matching plan');
  h+='<dl class="spec">';
  if(ds.cs) h+=row('Callsign', esc(ds.cs));
  h+=row('Route', esc(ds.dep||'?')+' → '+esc(ds.arr||'?'));
  h+=row('Off blocks', fmt(ds.start));
  h+=row('On blocks', fmt(ds.end));
  const mins=Math.round((new Date(ds.end)-new Date(ds.start))/60000);
  if(mins>0) h+=row('Duration', Math.floor(mins/60)+'h'+String(mins%60).padStart(2,'0'));
  if(ds.planfl) h+=row('Planned', esc(ds.planfl)+' '+esc(ds.plandep)+'→'+esc(ds.planarr));
  show(h+'</dl>');
}
return {open, openActual, close};
})();

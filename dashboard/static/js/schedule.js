// /schedule — the airframe rotation: every long-haul tail as a row, the last
// day as ADS-B recorded it and the days ahead as published, from
// /api/schedule. The gantt is built as positioned boxes (a bar is a leg, its
// length the flight's duration); a leg opens the flight dialog (flight.js).
(() => {
'use strict';

const {$, el, state} = window.Page, Flight = window.Flight;
// The tails this visitor planned on /book (browser storage, no accounts).
function myTargets(){
  try { const v = JSON.parse(localStorage.getItem('lh.targets') || '[]'); return new Set(Array.isArray(v) ? v : []); }
  catch(e){ return new Set(); }
}
function fmt(iso){ if(!iso) return '?'; const d=new Date(iso);
  return d.toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
// hue per family (variants share it; the badge text names the exact variant)
const TFAM={'748':'t748','388':'t388','788':'t789','789':'t789','78X':'t789','359':'t359','35K':'t359'};
function tcls(t){ return TFAM[t] || 'tother'; }
const TYPE_ORDER=['748','388','788','789','78X','359','35K'];   // fixed checkbox/legend order
const HIDE_KEY='sched.hiddenTypes';
const ALLEG_KEY='sched.allegrisOnly';
const HUBS=['FRA','MUC'];                       // German bases these widebodies rotate from
function legDisplay(dep,arr){                   // destination-led label: arrow + the non-hub endpoint
  const dh=HUBS.includes(dep), ah=HUBS.includes(arr);
  if(dh&&!ah) return {arrow:'\u2192', stn:arr};
  if(!dh&&ah) return {arrow:'\u2190', stn:dep};
  return {arrow:'\u2192', stn:arr||dep||'?'};
}
const legByKey={};                              // num|fdate -> leg, so the modal can show the actual track

async function init(){
  let d;
  try { d = await (await fetch('/api/schedule')).json(); }
  catch(e){ d = {error: 'The schedule could not be loaded'}; }
  if(d.error){ $('state').replaceChildren(state('bad', d.error)); return; }
  if(!d.airframes || !d.airframes.length){
    $('state').replaceChildren(el('p', 'empty', 'No upcoming schedule yet: the collector has not read any future flights.'));
    return;
  }
  // starred + pinned = the visitor's own tails (planned on /book, kept in this browser)
  const MINE = myTargets(), TORD = {'748':0,'388':1,'788':2,'789':3,'78X':4,'359':5,'35K':6};
  d.airframes.forEach(a => { a.watch = MINE.has(a.reg); });
  d.airframes.sort((x,y) => (y.watch - x.watch) || ((x.type in TORD ? TORD[x.type] : 9) - (y.type in TORD ? TORD[y.type] : 9)) || x.reg.localeCompare(y.reg));

  const t0=new Date(d.window.start).getTime(), t1=new Date(d.window.end).getTime();
  const range=t1-t0, dayMs=86400000;
  const frac = ms => (ms - t0)/range;                        // 0..1 across the time area
  const oLeft = f => 'calc(var(--label) + (100% - var(--label)) * '+f+')'; // overlay coord (rows incl. label)
  const dayName = ms => new Date(ms).toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'});

  // midnight ticks (fake-UTC midnights are epoch multiples of a day)
  const ticks=[]; for(let m=Math.ceil(t0/dayMs)*dayMs; m<t1; m+=dayMs) ticks.push(m);

  // axis: day labels at the left edge + each midnight. Drop the partial first-day
  // label when the first midnight is so close to the left edge that the two would
  // overlap (e.g. when "now-24h" lands late in the day) — that day is still named by
  // the midnight tick just to its right.
  let html='<div class="gantt-axis-row"><div class="gantt-label"></div><div class="gantt-axis">';
  const firstTickPct = ticks.length ? frac(ticks[0])*100 : 100;
  if(firstTickPct >= 8)
    html+='<span class="gantt-day" style="left:0;border-left:none">'+dayName(t0)+'</span>';
  ticks.forEach(m=>{ html+='<span class="gantt-day" style="left:'+(frac(m)*100)+'%">'+dayName(m)+'</span>'; });
  html+='</div></div>';

  // one overlay across all rows: midnight gridlines + the single now line
  let overlay='<div class="gantt-grid">';
  ticks.forEach(m=>{ overlay+='<div class="grid-line" style="left:'+oLeft(frac(m))+'"></div>'; });
  if(d.now!=null){ const nf=frac(new Date(d.now).getTime());
    if(nf>=0&&nf<=1) overlay+='<div class="now-line" style="left:'+oLeft(nf)+'"></div>'; }
  overlay+='</div>';
  html+='<div class="gantt-rows">'+overlay;

  d.airframes.forEach(a=>{
    const tc=tcls(a.type);
    const lblInner=(a.watch?'<span class="star">\u2605</span>':'')+a.reg+'<span class="tbadge '+tc+'">'+a.type+'</span>'
      +(a.allegris?'<span class="abadge" title="Allegris cabin">A</span>':'');
    html+='<div class="gantt-row'+(a.watch?' watch':'')+'" data-reg="'+a.reg+'" data-type="'+a.type+'" data-allegris="'+(a.allegris?'1':'0')+'" data-dests="'+a.legs.map(l=>l.dep+' '+l.arr).join(' ')+'" data-fls="'+a.legs.map(l=>l.fl).join(' ')+'">';
    html+='<a class="gantt-label" href="/airframe/'+encodeURIComponent(a.reg)+'" title="'+a.reg
      +(a.cabin?' · '+a.cabin:'')+' — open airframe profile">'+lblInner+'</a>';
    html+='<div class="gantt-track">';

    // rotation ties ("stays"): consecutive legs where the tail sits at an outstation (out & back)
    a.legs.forEach((l,i)=>{
      const n=a.legs[i+1]; if(!n) return;
      if(l.arr && n.dep && l.arr===n.dep && !HUBS.includes(l.arr)){
        const ge=new Date(l.end).getTime(), gs=new Date(n.start).getTime();
        let gl=Math.max(0,Math.min(100,(ge-t0)/range*100)), gr=Math.max(0,Math.min(100,(gs-t0)/range*100));
        if(gr-gl>0.3){
          const hrs=Math.max(0,Math.round((gs-ge)/3600000));
          const tip=a.reg+' parked at '+l.arr+'\nturnaround '+hrs+'h'
            +'\n'+(l.fl||'?')+' \u2192 '+(n.fl||'?')
            +'\non ground '+fmt(l.end)+' \u2192 '+fmt(n.start);
          html+='<div class="tie '+tc+'" style="left:'+gl+'%;width:'+(gr-gl)+'%" title="'+tip.replace(/"/g,'&quot;')+'">'
            +(hrs>=10?'<span class="tielab">@'+l.arr+' '+hrs+'H</span>':'')+'</div>';   // short stays: line only, label in tooltip
        }
      }
    });

    a.legs.forEach(l=>{
      const s=new Date(l.start).getTime(), e=new Date(l.end).getTime();
      // clip to the visible window: keep the true end position, cut the start at the edge
      const left=Math.max(0,(s-t0)/range*100), right=Math.min(100,(e-t0)/range*100);
      const width=Math.max(0.5,right-left);
      const dur=l.dur?(Math.floor(l.dur/60)+'h'+String(l.dur%60).padStart(2,'0')):'';
      const st=l.status||'planned';
      // hue = aircraft type; status = a presentation class (cap / texture / tint), never a text-colour swap
      const stCls = st==='tracked'?'st-tracked' : st==='missing'?'st-missing'
        : st==='deviation'?'st-ghost' : st==='extra'?'st-extra' : 'st-planned';
      const dsp=legDisplay(l.dep,l.arr), flnum=(l.fl||'').replace('LH','');
      let title;
      if(st==='extra'){
        title=(l.fl||'actual')+'  '+l.dep+' \u2192 '+l.arr+'\n\u2713 recorded (ADS-B) \u00b7 no matching plan'
          +'\n'+fmt(l.start)+' \u2192 '+fmt(l.end);
      } else {
        title=l.fl+'  '+l.dep+' \u2192 '+l.arr+(l.nseg>1?'  (leg '+l.seg+' of '+l.route.join('\u2013')+')':'')
          +'\nDep '+fmt(l.dep_t)+' ('+l.dep+')'+(l.arr_t?'\nArr '+fmt(l.arr_t)+' ('+l.arr+')':'')
          +(dur?'\nFlight '+dur:'')+'  \u00b7  '+a.type
          +(l.prev?'\nprev: '+l.prev:'')+(l.lead!=null?'\nlead: +'+l.lead+'d':'')+(l.swap?'  \u00b7  \u26a0 reassigned':'');
        if(st==='deviation' && l.act){
          title+='\n\u26a0 planned slot \u2014 tail actually flew '+l.act.dep+'\u2192'+l.act.arr;
        } else if(st==='tracked' && l.act){
          const dm=l.act.delta, ds=(dm>0?'+':'')+dm+'m';
          title+='\n\u2713 tracked: '+l.act.dep+'\u2192'+l.act.arr+(l.act.cs?' ('+l.act.cs+')':'')+'  ['+ds+' vs plan]';
        } else if(st==='missing'){ title+='\n\u2014 no ADS-B track found yet'; }
      }
      if(l.num && l.fdate) legByKey[l.num+'|'+l.fdate+'|'+(l.seg||1)]=l;   // remember for the enriched modal
      // unplanned "extra" legs have no flight number → carry the actual track on data-* for openActual()
      const actAttrs = st==='extra'
        ? ' data-cs="'+(l.fl||'')+'" data-dep="'+l.dep+'" data-arr="'+l.arr+'" data-start="'+l.start+'" data-end="'+l.end+'"'
        : '';
      html+='<div class="gantt-flight '+tc+' '+stCls+(l.swap?' is-swap':'')+' clk" style="left:'+left+'%;width:'+width+'%"'
        +' data-dest="'+l.dep+' '+l.arr+'" data-fl="'+(l.fl||'')+'" data-num="'+(l.num||'')+'" data-fdate="'+(l.fdate||'')+'" data-seg="'+(l.seg||1)+'"'+actAttrs
        +' title="'+title.replace(/"/g,'&quot;')+'">'
        +(l.swap?'<span class="swapchip"></span>':'')
        +'<span class="lbl">'+dsp.arrow+' '+dsp.stn+(flnum?'<span class="fl">'+flnum+'</span>':'')+'</span></div>';
      // deviation: also draw the ACTUAL route the tail really flew (clickable → actual detail)
      if(st==='deviation' && l.act){
        const as=new Date(l.act.start).getTime(), ae=new Date(l.act.end).getTime();
        const al=Math.max(0,(as-t0)/range*100), ar=Math.min(100,(ae-t0)/range*100);
        const aw=Math.max(0.5,ar-al), ad=legDisplay(l.act.dep,l.act.arr);
        const at=a.reg+' actually flew\n'+l.act.dep+' \u2192 '+l.act.arr+(l.act.cs?' ('+l.act.cs+')':'')
          +'\n'+fmt(l.act.start)+' \u2192 '+fmt(l.act.end)+'\n(planned '+l.fl+' '+l.dep+'\u2192'+l.arr+')';
        html+='<div class="gantt-flight '+tc+' st-actual clk" style="left:'+al+'%;width:'+aw+'%"'
          +' data-cs="'+(l.act.cs||'')+'" data-dep="'+l.act.dep+'" data-arr="'+l.act.arr+'" data-start="'+l.act.start+'" data-end="'+l.act.end+'"'
          +' data-planfl="'+(l.fl||'')+'" data-plandep="'+l.dep+'" data-planarr="'+l.arr+'"'
          +' title="'+at.replace(/"/g,'&quot;')+'">'
          +'<span class="lbl">'+ad.arrow+' '+ad.stn+'</span></div>';
      }
    });
    html+='</div></div>';
  });
  html+='</div>';
  $('gantt').innerHTML=html;
  // the lead gives way to the filters; the top bar's stamp says when the plan was put together
  $('state').remove();
  $('filters').hidden=false;
  $('rotation').hidden=false;
  const made=new Date(d.generated);
  $('stamp').textContent=made.toLocaleDateString('en-GB',{day:'numeric',month:'short',timeZone:'UTC'})+' \u00b7 '
    +made.toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'})+' UTC';

  // type checkboxes — the type legend and the visibility control in one.
  // Hiding a type hides its rows EXCEPT watched (starred) tails; persisted.
  const present=[...new Set(d.airframes.map(a=>a.type))];
  const types=TYPE_ORDER.filter(t=>present.includes(t))
    .concat(present.filter(t=>!TYPE_ORDER.includes(t)).sort());
  let hidden;
  try { hidden=new Set(JSON.parse(localStorage.getItem(HIDE_KEY)||'[]')); } catch(e){ hidden=new Set(); }
  $('typechk').innerHTML=types.map(t=>
    '<span class="tchk'+(hidden.has(t)?' off':'')+'" data-t="'+t+'" role="checkbox" aria-checked="'+(hidden.has(t)?'false':'true')+'" tabindex="0" title="Show/hide '+t+' rows (watched tails stay visible)">'
    +'<span class="sw '+tcls(t)+'"></span>'+t+'</span>').join('');
  // "Allegris only" is strict on purpose (no watched-tail exemption): it's an
  // explicit cabin lens, not a row-decluttering toggle like the type boxes.
  let allegOnly=false;
  try { allegOnly = localStorage.getItem(ALLEG_KEY)==='1'; } catch(e){}
  $('allegchk').classList.toggle('off', !allegOnly);
  $('allegchk').setAttribute('aria-checked', allegOnly?'true':'false');
  const applyTypes=()=>{
    document.querySelectorAll('.gantt-row[data-type]').forEach(r=>{
      const typeHide = hidden.has(r.dataset.type) && !r.classList.contains('watch');
      r.classList.toggle('typehide', typeHide || (allegOnly && r.dataset.allegris!=='1'));
    });
  };
  const toggleType=el=>{
    const t=el.dataset.t; if(!t) return;
    if(hidden.has(t)) hidden.delete(t); else hidden.add(t);
    const off=hidden.has(t);
    el.classList.toggle('off', off);
    el.setAttribute('aria-checked', off?'false':'true');
    try { localStorage.setItem(HIDE_KEY, JSON.stringify([...hidden])); } catch(err){}
    applyTypes();
  };
  $('typechk').addEventListener('click', e=>{ const el=e.target.closest('.tchk'); if(el) toggleType(el); });
  $('typechk').addEventListener('keydown', e=>{ if(e.key===' '||e.key==='Enter'){ const el=e.target.closest('.tchk'); if(el){ e.preventDefault(); toggleType(el); } } });
  const toggleAlleg=()=>{
    allegOnly=!allegOnly;
    $('allegchk').classList.toggle('off', !allegOnly);
    $('allegchk').setAttribute('aria-checked', allegOnly?'true':'false');
    try { localStorage.setItem(ALLEG_KEY, allegOnly?'1':'0'); } catch(err){}
    applyTypes();
  };
  $('allegchk').addEventListener('click', toggleAlleg);
  $('allegchk').addEventListener('keydown', e=>{ if(e.key===' '||e.key==='Enter'){ e.preventDefault(); toggleAlleg(); } });
  applyTypes();
}

$('filter').addEventListener('input', e=>{
  const q=e.target.value.trim().toUpperCase();
  document.querySelectorAll('.gantt-row').forEach(row=>{
    if(!q){ row.classList.remove('dim'); row.querySelectorAll('.gantt-flight').forEach(f=>f.classList.remove('dim')); return; }
    const regMatch=(row.dataset.reg||'').toUpperCase().includes(q);
    let any=regMatch;
    row.querySelectorAll('.gantt-flight').forEach(f=>{
      const hit=regMatch || (f.dataset.dest||'').toUpperCase().includes(q) || (f.dataset.fl||'').toUpperCase().includes(q);
      f.classList.toggle('dim', !hit); if(hit) any=true;
    });
    row.classList.toggle('dim', !any);
  });
});

// a leg opens the flight dialog: a planned one with what ADS-B recorded for it,
// an unplanned one from the track alone
$('gantt').addEventListener('click', e=>{ const f=e.target.closest('.gantt-flight'); if(!f) return;
  if(f.dataset.num) Flight.open(f.dataset.num, f.dataset.fdate, legByKey[f.dataset.num+'|'+f.dataset.fdate+'|'+(f.dataset.seg||1)]);
  else if(f.dataset.dep) Flight.openActual(f.dataset); });

init();
})();

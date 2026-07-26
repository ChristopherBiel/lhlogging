#!/usr/bin/env node
/* faceplate check — verify a project stayed inside the system.
 *
 * This does not know what your components are and does not need to. It checks
 * the values, which is what scales to the components Faceplate never shipped.
 *
 * Messages state the rule, the reasoning, and where to go next — they are read
 * by whoever is holding the keyboard, so they should teach rather than scold.
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const cmd = argv.find(a => !a.startsWith('-')) || 'check';
const flag = f => argv.includes(f);
const STRICT = flag('--strict');

/* ------------------------------------------------------------- tokens */
function loadTokens() {
  const candidates = [
    path.join(HERE, '../dist/tokens.json'),
    path.join(process.cwd(), 'node_modules/faceplate/dist/tokens.json'),
    path.join(process.cwd(), 'faceplate/dist/tokens.json'),
    path.join(process.cwd(), 'dist/tokens.json'),
    path.join(process.cwd(), 'tokens.json'),
  ];
  for (const c of candidates) if (fs.existsSync(c)) return { data: JSON.parse(fs.readFileSync(c, 'utf8')), from: c };
  console.error('faceplate: could not find tokens.json. Install faceplate, or run from a project that vendors dist/.');
  process.exit(2);
}
const { data: TOKENS, from: TOKENS_FROM } = loadTokens();
const KNOWN_VARS = new Set(Object.keys(TOKENS.variables));
const PALETTE = new Set(Object.values(TOKENS.closedSet).map(h => h.toUpperCase()));
const SCALE = new Set(Object.entries(TOKENS.variables)
  .filter(([k]) => /^--fp-(text|space)-/.test(k)).map(([, v]) => v));

/* -------------------------------------------------------------- files */
/* fixtures hold intentionally non-conformant reference material — they exist to
   be compared against, not to conform */
const SKIP = new Set(['node_modules', '.git', 'dist', 'build', '.next', 'vendor', 'coverage', 'fixtures']);
const EXT = new Set(['.css', '.html', '.htm', '.scss', '.svg']);
function walk(dir, out = []) {
  let entries; try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return out; }
  for (const e of entries) {
    if (e.name.startsWith('.') && e.name !== '.') continue;
    const full = path.join(dir, e.name);
    if (e.isDirectory()) { if (!SKIP.has(e.name)) walk(full, out); }
    else if (EXT.has(path.extname(e.name))) out.push(full);
  }
  return out;
}

/* A project may legitimately alias tokens: --sans: var(--fp-font-sans).
   Resolve local custom properties before judging a value, or every alias
   reads as a violation — which is how a checker earns being switched off. */
function localDefs(text) {
  const defs = new Map();
  for (const m of text.matchAll(/(--[a-z0-9-]+)\s*:\s*([^;}\n]+)/gi))
    if (!m[1].startsWith('--fp-')) defs.set(m[1].toLowerCase(), m[2].trim());
  return defs;
}
function deref(value, defs, depth = 0) {
  if (depth > 4) return value;
  return value.replace(/var\(\s*(--[a-z0-9-]+)\s*(?:,[^)]*)?\)/gi, (whole, name) => {
    const v = defs.get(name.toLowerCase());
    return v === undefined ? whole : deref(v, defs, depth + 1);
  });
}

const findings = [];
const rel = f => path.relative(process.cwd(), f) || f;
function report(level, rule, file, line, msg, why, link) {
  findings.push({ level, rule, file: rel(file), line, msg, why, link });
}
const lineOf = (text, idx) => text.slice(0, idx).split('\n').length;

/* -------------------------------------------------------------- rules */

function checkTokens(text, file) {
  const re = /--fp-[a-z0-9-]+/gi; let m;
  const seen = new Set();
  while ((m = re.exec(text))) {
    const name = m[0].toLowerCase();
    if (seen.has(name + m.index)) continue;
    if (KNOWN_VARS.has(name)) continue;
    if (name.startsWith('--fp-ext-')) {
      report('warn', 'extension', file, lineOf(text, m.index), `${name}`,
        'Declared as a project extension. Extensions are a flare, not a permit — they are expected to become a proposal or be removed.',
        'docs/contributing.md');
      continue;
    }
    report('error', 'unknown-token', file, lineOf(text, m.index), `unknown token ${name}`,
      'Faceplate defines a closed set of tokens. A token invented inside a project is invisible to the brand sheet and disappears on the next update, taking whatever depended on it with it. This is exactly how --fp-dv-7 shipped unnoticed.',
      'docs/contributing.md#proposing-a-token');
  }
}

function checkHex(text, file) {
  const re = /#([0-9a-f]{3}|[0-9a-f]{6})\b/gi; let m;
  while ((m = re.exec(text))) {
    let hex = m[0].toUpperCase();
    if (hex.length === 4) hex = '#' + [...hex.slice(1)].map(c => c + c).join('');
    if (PALETTE.has(hex)) continue;
    const before = text.slice(Math.max(0, m.index - 60), m.index);
    if (/(&|#)$/.test(before)) continue;                      // html entity
    /* var(...) inside color-mix() defeats a naive lookbehind, so scan for an
       unclosed color-mix( instead of requiring no ) in between */
    const opens = (before.match(/color-mix\(/gi) || []).length;
    const closes = opens ? (before.split(/color-mix\(/i).pop().match(/\)/g) || []).length : 0;
    const insideMix = opens > 0 && closes < (before.split(/color-mix\(/i).pop().match(/\(/g) || []).length + 1;
    if (insideMix && /^#(FFFFFF|000000)$/.test(hex)) {
      report('warn', 'mix-operator', file, lineOf(text, m.index), `${m[0]} used as a mix operator`,
        'Mixing toward pure white or black is a common shading trick, but it produces colours that are not in the palette. Prefer an existing tint or deep variant.',
        'docs/colour.md');
      continue;
    }
    report('error', 'off-palette', file, lineOf(text, m.index), `off-palette colour ${m[0]}`,
      'The palette is closed. Every colour Faceplate renders is one of the primitives in docs/colour.md.',
      'docs/colour.md');
  }
}

function checkRadius(text, file, defs) {
  const re = /border-radius\s*:\s*([^;}\n"']+)/gi; let m;
  while ((m = re.exec(text))) {
    const v = deref(m[1].trim(), defs);
    if (/^(0|0px|0rem|0%)(\s+(0|0px|0rem|0%))*\s*(!important)?$/.test(v)) continue;
    if (/var\(--fp-radius\)/.test(v)) continue;
    report('error', 'rounded-corner', file, lineOf(text, m.index), `border-radius: ${v}`,
      'Hard square edges are the single most recognisable thing about Faceplate. --fp-radius is 0 and there is no rounded variant of anything; a rounded corner reads as a different brand.',
      'docs/principles.md');
  }
}

function checkFonts(text, file, defs) {
  const re = /font-family\s*:\s*([^;}\n"']+)/gi; let m;
  while ((m = re.exec(text))) {
    const v = deref(m[1].trim(), defs);
    if (/var\(--fp-font-(sans|mono)\)/.test(v)) continue;
    if (/^(inherit|initial|unset)\b/.test(v)) continue;
    if (/Manrope|IBM Plex Mono/i.test(v)) continue;
    report('error', 'off-brand-font', file, lineOf(text, m.index), `font-family: ${v}`,
      'Faceplate is Manrope and IBM Plex Mono. Use var(--fp-font-sans) or var(--fp-font-mono) — naming a family directly drifts as soon as the stack changes.',
      'docs/typography.md');
  }
}

function checkReset(text, file) {
  /* the * may follow a rule, a comma, or the opening of an inline <style> */
  const re = /(^|[},>]|\*\/)\s*\*(\s*,\s*\*::?(before|after))*\s*\{([^}]*)\}/gi; let m;
  while ((m = re.exec(text))) {
    const body = m[4];
    const zeroed = ['padding', 'margin'].filter(p =>
      new RegExp(`(^|;)\\s*${p}\\s*:\\s*0`, 'i').test(body));
    if (!zeroed.length) continue;
    report('warn', 'universal-reset', file, lineOf(text, m.index),
      `universal selector sets ${zeroed.join(' and ')} to 0`,
      'Faceplate ships inside @layer, and unlayered CSS beats layered CSS regardless of specificity — so this silently zeroes the padding of every .fp-* component. A real project shipped segmented controls reading "anyyesno" this way. Scope the reset, or exclude [class^="fp-"].',
      'docs/using-faceplate.md#a-note-on-resets');
  }
}

function checkSpacing(text, file) {
  const re = /\b(?:padding|margin|gap)(?:-(?:top|right|bottom|left|inline|block))?\s*:\s*([^;}\n]+)/gi; let m;
  while ((m = re.exec(text))) {
    const v = m[1].trim();
    if (/var\(|calc\(|%|auto|inherit|0\b/.test(v)) continue;
    const rems = v.match(/[\d.]+rem/g) || [];
    const off = rems.filter(r => !SCALE.has(r));
    if (off.length) {
      report('warn', 'off-scale-spacing', file, lineOf(text, m.index), `${off.join(', ')} is not on the spacing scale`,
        'Spacing runs 4 / 8 / 16 / 24 / 32 / 48 / 64. Values off the scale accumulate into layouts that no longer align.',
        'docs/principles.md#grid-and-spacing');
    }
  }
}

function checkIntensity(text, file) {
  if (!/data-fp-intensity/.test(text)) return;
  const declared = [...text.matchAll(/data-fp-intensity\s*=\s*["'](\d{2})["']/g)].map(m => m[1]);
  const uniq = [...new Set(declared)];
  if (uniq.length > 1) {
    report('warn', 'mixed-intensity', file, lineOf(text, text.indexOf('data-fp-intensity')),
      `this file declares intensity ${uniq.join(' and ')}`,
      'Never mix levels within one artifact. A page with a 04 hero and 01 content reads as two designs stapled together, not as range. If a section needs more presence, raise the whole surface.',
      'docs/intensity.md');
  }
  for (const m of text.matchAll(/data-fp-intensity\s*=\s*["'](0[12])["'][^>]*>([\s\S]{0,900})/g)) {
    if (/class\s*=\s*["'][^"']*\bfp-band\b/.test(m[2])) {
      report('warn', 'intensity-mismatch', file, lineOf(text, m.index),
        `.fp-band inside a surface declared intensity ${m[1]}`,
        'A full-bleed colour block is a 03+ device. At 01–02 colour coverage should be 2–10%, which a band alone exceeds.',
        'docs/intensity.md');
    }
  }
}

function checkIcon(text, file) {
  if (path.extname(file) !== '.svg') return;
  if (!/\/(icons)\//.test(file.replace(/\\/g, '/'))) return;
  const vb = text.match(/viewBox\s*=\s*"([^"]+)"/);
  if (!vb || vb[1].trim() !== '0 0 24 24')
    report('error', 'icon-grid', file, 1, `viewBox is ${vb ? vb[1] : 'missing'}`,
      'Topical motifs share one style: a 24px grid, 2px stroke, round caps. An icon off the grid pulls the family apart.',
      'docs/iconography.md');
  const sw = text.match(/stroke-width\s*=\s*"([^"]+)"/);
  if (sw && sw[1].trim() !== '2')
    report('error', 'icon-stroke', file, 1, `stroke-width is ${sw[1]}`, 'The family is drawn at 2px.', 'docs/iconography.md');
  if (/stroke\s*=\s*"#/.test(text))
    report('error', 'icon-color', file, 1, 'icon has a baked-in stroke colour',
      'Icons use currentColor so they inherit the surrounding text colour. A baked-in hex breaks reversed surfaces.',
      'docs/iconography.md');
}

/* ------------------------------------------------------ vendored copy */
function checkManifest() {
  const roots = [path.join(process.cwd(), 'dist'), path.join(process.cwd(), 'faceplate'),
                 path.join(process.cwd(), 'public/faceplate'), process.cwd()];
  for (const root of roots) {
    const mf = path.join(root, 'MANIFEST.sha256');
    if (!fs.existsSync(mf)) continue;
    let bad = 0, missing = 0;
    for (const line of fs.readFileSync(mf, 'utf8').trim().split('\n')) {
      const [hash, name] = line.split(/\s+/);
      const f = path.join(root, name);
      if (!fs.existsSync(f)) { missing++; continue; }
      const actual = crypto.createHash('sha256').update(fs.readFileSync(f)).digest('hex');
      if (actual !== hash) {
        bad++;
        report('error', 'modified-vendor', f, 1, `${name} does not match its checksum`,
          'This file is generated and shipped. Editing a vendored copy makes the project agree with itself while diverging from the brand — and every compliance check will then pass, because it is measuring against the modified file. That is precisely how --fp-dv-7 survived four reviews.',
          'docs/contributing.md');
      }
    }
    return { root, bad, missing, checked: true };
  }
  return { checked: false };
}

/* -------------------------------------------------------------- run */
if (cmd !== 'check') {
  console.log(`faceplate — design system checker

  npx faceplate check [path]    verify a project stayed inside the system
    --strict                    treat warnings as errors
    --json                      machine-readable output

  Rules and reasoning: docs/`);
  process.exit(0);
}

const target = argv.find(a => !a.startsWith('-') && a !== 'check') || '.';
const files = fs.statSync(target).isDirectory() ? walk(target) : [target];

for (const f of files) {
  const text = fs.readFileSync(f, 'utf8');
  const isStyle = /\.(css|scss|html?)$/.test(f);
  if (isStyle) {
    const defs = localDefs(text);
    checkTokens(text, f); checkHex(text, f); checkRadius(text, f, defs);
    checkFonts(text, f, defs); checkReset(text, f); checkSpacing(text, f); checkIntensity(text, f);
  }
  checkIcon(text, f);
}
const mf = checkManifest();

/* ------------------------------------------------------------ output */
const errors = findings.filter(f => f.level === 'error');
const warns = findings.filter(f => f.level === 'warn');

if (flag('--json')) {
  console.log(JSON.stringify({ errors: errors.length, warnings: warns.length, findings }, null, 2));
  process.exit(errors.length || (STRICT && warns.length) ? 1 : 0);
}

console.log(`\nfaceplate check — v${TOKENS.version}`);
console.log(`  tokens: ${path.relative(process.cwd(), TOKENS_FROM) || TOKENS_FROM}`);
console.log(`  scanned: ${files.length} file(s)`);
console.log(`  vendored copy: ${mf.checked ? (mf.bad ? `${mf.bad} file(s) MODIFIED` : 'matches manifest') : 'no manifest found'}\n`);

const byRule = {};
for (const f of findings) (byRule[f.rule] ||= []).push(f);

for (const [rule, list] of Object.entries(byRule)) {
  const mark = list[0].level === 'error' ? '✗' : '!';
  console.log(`${mark} ${rule} — ${list.length}`);
  for (const f of list.slice(0, 8)) console.log(`    ${f.file}:${f.line}  ${f.msg}`);
  if (list.length > 8) console.log(`    … and ${list.length - 8} more`);
  console.log(`    ${list[0].why}`);
  console.log(`    → ${list[0].link}\n`);
}

if (!findings.length) console.log('No findings. Everything is inside the system.\n');
else console.log(`${errors.length} error(s), ${warns.length} warning(s)\n`);

process.exit(errors.length || (STRICT && warns.length) ? 1 : 0);

const $ = id => document.getElementById(id);
const num = n => Number(n || 0);
const money = n => '₹' + Number(n || 0).toLocaleString('en-IN', { maximumFractionDigits: 2 });
let latest = null;

const pageMeta = {
  home: ['Overview', 'NIFTY options · APEX CORE · paper-first'],
  strategy: ['APEX Core', '5m regime → 3m breakout/retest → 1m trigger'],
  trades: ['Trades', 'TRUE NET P&L and completed journal'],
  adaptive: ['Adaptive Learning', 'Bounded entry + trailing tuning · shadow-first'],
  broker: ['Broker', '5paisa Xstream · diagnostics · reconciliation'],
  research: ['Research', 'Evidence center · exports · no fabricated metrics'],
  system: ['System', 'Health · runtime truth · append-only journal'],
  settings: ['Settings', 'Configuration · safety locks · restart-aware'],
  more: ['More', 'All operational areas and safety invariants'],
};

function esc(v) {
  return String(v ?? '').replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
}
function clsPnL(n) { return num(n) >= 0 ? 'positive' : 'negative'; }

function setPage(name) {
  document.querySelectorAll('.page').forEach(p => p.classList.toggle('active', p.id === `page-${name}`));
  document.querySelectorAll('[data-page]').forEach(b => b.classList.toggle('active', b.dataset.page === name));
  if (pageMeta[name]) {
    $('pageTitle').textContent = pageMeta[name][0];
    $('pageSubtitle').textContent = pageMeta[name][1];
  }
  window.scrollTo({ top: 0, behavior: 'smooth' });
  if (name === 'settings') loadSettings();
}

document.querySelectorAll('[data-page]').forEach(b => b.addEventListener('click', () => setPage(b.dataset.page)));

const aSync = async function(url, options) {
  const r = await fetch(url, options);
  const text = await r.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
  return data;
};

function download(url) {
  const a = document.createElement('a');
  a.href = url;
  a.download = '';
  document.body.appendChild(a);
  a.click();
  a.remove();
}

async function action(name) {
  if (name === 'refresh') return refresh(true);
  if (name === 'export-trades') return download('/api/export/trades.csv');
  if (name === 'export-events') return download('/api/export/events.csv');
  if (name === 'export-training') return download('/api/export/training.csv');
  if (name === 'research-audit') {
    alert('Run scripts/research_audit.py from the project directory. No synthetic research result is displayed.');
    return;
  }
  if (name === 'oauth') {
    try {
      const j = await aSync('/api/5paisa/oauth-url');
      $('brokerOutput').textContent = j.url || JSON.stringify(j, null, 2);
      if (j.url) window.open(j.url, '_blank');
    } catch (e) { $('brokerOutput').textContent = e.message; }
    return;
  }
  if (name === 'live-check') {
    try { $('brokerOutput').textContent = JSON.stringify(await aSync('/api/5paisa/live-data-check'), null, 2); }
    catch (e) { $('brokerOutput').textContent = e.message; }
    return;
  }
  if (name === 'reconcile') {
    try { $('brokerOutput').textContent = JSON.stringify(await aSync('/api/5paisa/reconcile'), null, 2); }
    catch (e) { $('brokerOutput').textContent = e.message; }
  }
}

document.querySelectorAll('[data-action]').forEach(b => b.addEventListener('click', () => action(b.dataset.action)));
$('settingsForm').addEventListener('submit', saveSettings);

function chartFromTrades(s) {
  const start = num(s.capital_start || 15000);
  let running = start;
  const closed = [...(s.trades_recent || [])].reverse();
  const vals = [start];
  closed.forEach(t => { running += num(t.net); vals.push(running); });
  while (vals.length < 8) {
    const end = num(s.paper_equity || start);
    vals.push(start + (end - start) * (vals.length / 7));
  }
  const w = 540, h = 190, pad = 12;
  const lo = Math.min(...vals), hi = Math.max(...vals), range = Math.max(1, hi - lo);
  const pts = vals.map((v, i) => [pad + (w - pad * 2) * (i / (vals.length - 1)), h - pad - (h - pad * 2) * ((v - lo) / range)]);
  const line = 'M ' + pts.map(p => p.map(x => x.toFixed(1)).join(' ')).join(' L ');
  $('linePath').setAttribute('d', line);
  $('areaPath').setAttribute('d', `${line} L ${pts[pts.length-1][0]} ${h} L ${pts[0][0]} ${h} Z`);
  $('chartDot').setAttribute('cx', pts[pts.length-1][0]);
  $('chartDot').setAttribute('cy', pts[pts.length-1][1]);
  $('capitalStart').textContent = money(start);
  $('chartNow').textContent = money(vals[vals.length-1]);
}

function renderPosition(p) {
  $('positionTag').textContent = p ? esc(p.side || 'OPEN') : 'FLAT';
  $('positionView').innerHTML = p ? `<div class="kv-grid">
    <div class="kv"><span>Symbol</span><b>${esc(p.symbol || '—')}</b></div>
    <div class="kv"><span>Entry</span><b>${money(p.entry_price || p.entry)}</b></div>
    <div class="kv"><span>Qty</span><b>${esc(p.qty || p.quantity || '—')}</b></div>
    <div class="kv"><span>Stop</span><b>${num(p.stop_underlying).toFixed(2)}</b></div>
    <div class="kv"><span>Target</span><b>${num(p.target_underlying).toFixed(2)}</b></div>
    <div class="kv"><span>Highest R</span><b>${num(p.highest_r).toFixed(2)}R</b></div>
  </div>` : '<div class="empty-state">No open position.</div>';
}

function renderQuote(q) {
  $('quoteView').innerHTML = q ? `<div class="kv-grid">
    <div class="kv"><span>Contract</span><b>${esc(q.symbol || '—')}</b></div>
    <div class="kv"><span>Bid</span><b>${money(q.bid)}</b></div>
    <div class="kv"><span>Ask</span><b>${money(q.ask)}</b></div>
    <div class="kv"><span>LTP</span><b>${money(q.ltp || q.mid)}</b></div>
  </div>` : '<div class="empty-state">No executable quote.</div>';
}

function renderFactors(f = {}) {
  const names = { vwap:'VWAP', ema:'EMA', slope:'SLOPE', structure:'STRUCTURE', impulse:'IMPULSE' };
  $('factors').innerHTML = Object.keys(names).map(k => `<div class="factor ${f[k] ? 'on' : ''}">${names[k]}<small>${f[k] ? 'PASS' : 'WAIT'}</small></div>`).join('');
  $('strategyFactors').innerHTML = Object.keys(names).map(k => `<div class="factor-line"><span>${names[k]}</span><b class="${f[k] ? 'pass' : 'fail'}">${f[k] ? 'CONFIRMED' : 'NOT CONFIRMED'}</b></div>`).join('');
}

function renderTrades(s) {
  $('tradeCards').innerHTML = (s.trades_recent || []).map(t => `<div class="trade-card">
    <div class="trade-symbol"><b>${esc(t.symbol || 'NIFTY OPTION')}</b><span>${esc((t.closed_at || '').replace('T',' ').slice(0,19))} · ${esc(t.reason || '—')}</span></div>
    <div class="trade-cell"><span>ENTRY</span><b>${money(t.entry)}</b></div>
    <div class="trade-cell"><span>EXIT</span><b>${money(t.exit)}</b></div>
    <div class="trade-cell"><span>COSTS</span><b>${money(t.costs)}</b></div>
    <div class="trade-cell"><span>TRUE NET</span><b class="${clsPnL(t.net)}">${money(t.net)}</b></div>
  </div>`).join('') || '<div class="empty-state">No completed trades.</div>';
}

function renderHealth(h = {}) {
  const e = Object.entries(h);
  const ok = e.filter(([,v]) => v === 'CONFIRMED').length;
  $('health').innerHTML = e.map(([k,v]) => `<div class="health-row"><span>${esc(k)}</span><b class="${v === 'CONFIRMED' ? 'pass' : 'warn'}">${esc(v)}</b></div>`).join('');
  $('systemOverall').textContent = e.length && ok === e.length ? 'ALL CONFIRMED' : `${ok}/${e.length} CONFIRMED`;
}
function renderEvents(es = []) {
  $('events').innerHTML = es.map(e => `<div class="event-row"><time>${esc((e.ts || '').slice(11,19))}</time><b>${esc(e.kind || 'EVENT')}</b><span>${esc(JSON.stringify(e.payload || {}).slice(0,420))}</span></div>`).join('') || '<div class="empty-state">No events.</div>';
}

function renderAdaptive(a = {}) {
  const shadow = !!a.shadow_mode;
  const setPill = (id, live, label) => { const e=$(id); e.textContent = live ? `${label} · LIVE` : 'SHADOW'; e.className = `adaptive-pill ${live ? 'live' : 'shadow'}`; };
  setPill('adaptiveModePill', !shadow, 'ADAPTIVE');
  setPill('adaptiveTrailPill', !a.trail_shadow_only, 'TRAIL');
  setPill('adaptiveEntryPill', !a.entry_shadow_only, 'ENTRY');
  const n = num(a.labeled_trades), target = Math.max(50, num(a.min_shadow_trades));
  $('adaptiveSampleCount').textContent = n;
  $('adaptiveSampleLabel').textContent = `${n} / ${target}`;
  $('adaptiveLabeled').textContent = n;
  $('adaptiveRemaining').textContent = Math.max(0, target - n);
  $('adaptiveAdjustments').textContent = num(a.adjustment_count);
  $('adaptiveSampleFill').style.width = Math.min(100, n / target * 100) + '%';
  const d = a.defaults || {};
  $('adaptiveDefaults').innerHTML = Object.entries({'BE trigger':d.breakeven_trigger_r,'Stop buffer':d.stop_buffer_r,'Stop trigger':d.stop_trigger_r,'Target trigger':d.target_trigger_r,'Target gap':d.target_gap_r,'Max target':d.max_target_r}).map(([k,v]) => `<div class="kv"><span>${k}</span><b>${num(v).toFixed(2)}R</b></div>`).join('');
  const bs = a.buckets || [];
  $('adaptiveBucketCount').textContent = `${bs.length} BUCKETS`;
  $('adaptiveBuckets').innerHTML = bs.map(b => `<div class="trade-card"><div class="trade-symbol"><b>${esc(b.key)}</b><span>${b.count} labeled · ${b.losses} losses</span></div><div class="trade-cell"><span>HIGHEST R</span><b>${num(b.avg_highest_r).toFixed(2)}R</b></div><div class="trade-cell"><span>CAPTURED R</span><b>${num(b.avg_captured_r).toFixed(2)}R</b></div><div class="trade-cell"><span>NET</span><b class="${clsPnL(b.net)}">${money(b.net)}</b></div><div class="trade-cell"><span>STATE</span><b>${b.adjustments ? 'ADJUSTED' : 'OBSERVING'}</b></div></div>`).join('') || '<div class="empty-state">HYDRA abhi seekh raha hai — koi adjustment nahi hua.</div>';
  $('adaptiveEvents').innerHTML = (a.events || []).filter(e => /^(ADAPTIVE_|AI_GATE_)/.test(e.kind || '')).map(e => `<div class="event-row"><time>${esc((e.ts || '').slice(11,19))}</time><b>${esc(e.kind)}</b><span>${esc(JSON.stringify(e.payload || {}).slice(0,420))}</span></div>`).join('') || '<div class="empty-state">No adaptive events.</div>';
}

function renderBroker(s) {
  $('brokerAuthPill').textContent = s.fivepaisa?.authenticated ? 'AUTHENTICATED' : 'NOT READY';
  const rows = [['Mode',String(s.mode || 'paper').toUpperCase()],['Data source',String(s.data_mode || 'demo').toUpperCase()],['Feed',s.feed_status || '—'],['5paisa auth',s.fivepaisa?.authenticated ? 'CONFIRMED' : 'NOT READY'],['NIFTY ScripCode',s.fivepaisa?.nifty_scrip_code || 'AUTO'],['Selected option',s.fivepaisa?.selected_contract?.symbol || '—'],['Quote age',s.fivepaisa?.option_quote_age_sec == null ? '—' : s.fivepaisa.option_quote_age_sec + 's']];
  $('brokerInfo').innerHTML = rows.map(([k,v]) => `<div class="info-row"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('');
}
function renderRuntime(s) {
  const rows = [['Version',s.version],['Mode',String(s.mode || 'paper').toUpperCase()],['Data mode',String(s.data_mode || 'demo').toUpperCase()],['Feed',s.feed_status || '—'],['Position',s.position ? 'OPEN' : 'FLAT'],['LIVE',s.live_enabled ? 'ENABLED' : 'BLOCKED'],['Adaptive',s.adaptive?.shadow_mode ? 'SHADOW' : 'LIVE']];
  $('runtimeInfo').innerHTML = rows.map(([k,v]) => `<div class="info-row"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('');
}
function renderResearch(s) {
  $('researchLabels').textContent = s.adaptive?.labeled_trades ?? 0;
  $('researchAdjustments').textContent = s.adaptive?.adjustment_count ?? 0;
  $('researchTrades').textContent = s.trades ?? 0;
  $('researchMode').textContent = String(s.data_mode || 'demo').toUpperCase();
}

const settingGroups = {
  settingsExecution: ['HYDRA_SCORE_THRESHOLD','HYDRA_MAX_SPREAD_PCT','HYDRA_PREFERRED_SPREAD_PCT','HYDRA_MAX_PRICE_DRIFT_PCT','HYDRA_MAX_QUOTE_AGE_SEC'],
  settingsTrailing: ['HYDRA_BREAKEVEN_TRIGGER_R','HYDRA_BREAKEVEN_LOCK_R','HYDRA_TRAILING_STOP_TRIGGER_R','HYDRA_TRAILING_STOP_BUFFER_R','HYDRA_TRAILING_TARGET_TRIGGER_R','HYDRA_TRAILING_TARGET_GAP_R','HYDRA_TRAILING_TARGET_STEP_R','HYDRA_MAX_TARGET_R'],
  settingsAdaptive: ['HYDRA_ADAPTIVE_ENABLED','HYDRA_ADAPTIVE_SHADOW_ONLY','HYDRA_ADAPTIVE_TRAIL_SHADOW_ONLY','HYDRA_ADAPTIVE_LOSS_DAYS','HYDRA_ADAPTIVE_LOSS_COUNT','HYDRA_ADAPTIVE_THRESHOLD_STEP','HYDRA_ADAPTIVE_MAX_THRESHOLD','HYDRA_ADAPTIVE_MIN_BUFFER_R','HYDRA_ADAPTIVE_MAX_BUFFER_R','HYDRA_ADAPTIVE_BUFFER_STEP_R','HYDRA_ADAPTIVE_MIN_BREAKEVEN_R','HYDRA_ADAPTIVE_MAX_BREAKEVEN_R','HYDRA_ADAPTIVE_BREAKEVEN_STEP_R','HYDRA_ADAPTIVE_MIN_SHADOW_TRADES'],
  settingsLocked: ['HYDRA_MODE','HYDRA_DATA_MODE','HYDRA_HARD_RISK','HYDRA_DAILY_LOSS','HYDRA_MAX_TRADES','HYDRA_LIVE_ENABLED','HYDRA_STATIC_IP_CONFIRMED','HYDRA_ALGO_ID'],
};
function renderSettingsFields(values) {
  for (const [id, keys] of Object.entries(settingGroups)) {
    const box = $(id); if (!box) continue;
    const locked = id === 'settingsLocked';
    box.innerHTML = keys.map(k => {
      const v = values[k]; const bool = typeof v === 'boolean';
      const control = bool ? `<input data-setting="${k}" type="checkbox" ${v ? 'checked' : ''} ${locked ? 'disabled' : ''}>` : `<input data-setting="${k}" type="number" step="any" value="${esc(v ?? '')}" ${locked ? 'disabled' : ''}>`;
      return `<label class="setting-row"><span>${k.replace('HYDRA_','').replaceAll('_',' ')}</span>${control}</label>`;
    }).join('');
  }
}
async function loadSettings() {
  try { const j = await aSync('/api/settings'); renderSettingsFields(j.values || {}); }
  catch (e) { $('settingsMessage').textContent = 'Unable to load settings: ' + e.message; }
}
async function saveSettings(e) {
  e.preventDefault();
  const values = {};
  document.querySelectorAll('[data-setting]:not(:disabled)').forEach(x => { values[x.dataset.setting] = x.type === 'checkbox' ? x.checked : Number(x.value); });
  try {
    const j = await aSync('/api/settings/save', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ values }) });
    $('settingsMessage').textContent = j.ok ? `Saved ${j.saved.length} settings. Controlled restart required.` : `Save failed: ${j.error}`;
  } catch (e) { $('settingsMessage').textContent = 'Save failed: ' + e.message; }
}

function render(s) {
  latest = s;
  const pnl = num(s.true_net_pnl), riskPct = Math.min(100, Math.abs(num(s.risk?.daily_net)) / Math.max(1, num(s.risk?.daily)) * 100);
  $('statusPill').textContent = s.status || 'UNKNOWN';
  $('statusPill').className = 'status-pill ' + ((s.status || '').includes('READY') ? '' : 'warn');
  $('equity').textContent = money(s.paper_equity); $('pnl').textContent = (pnl >= 0 ? '+' : '') + money(pnl); $('pnl').className = clsPnL(pnl);
  $('modeBadge').textContent = String(s.mode || 'paper').toUpperCase(); $('feedBadge').textContent = 'FEED ' + String(s.feed_status || '—').toUpperCase(); $('regimeBadge').textContent = 'REGIME ' + String(s.regime?.side || '—').toUpperCase();
  $('regime').textContent = s.regime?.side || '—'; $('regimeScore').textContent = `${Math.round(num(s.regime?.score))}/100`; $('trades').textContent = s.trades ?? 0; $('wl').textContent = `${s.wins || 0}W / ${s.losses || 0}L`;
  $('riskPercent').textContent = riskPct.toFixed(0) + '%'; $('dailyRiskText').textContent = `${money(s.risk?.daily_net)} of -${money(s.risk?.daily)}`; $('lastPrice').textContent = s.last_candle ? num(s.last_candle.close).toFixed(2) : '—'; $('feed').textContent = `${String(s.data_mode || 'demo').toUpperCase()} · ${s.feed_status || '—'}`;
  $('riskBand').textContent = `${money(s.risk?.normal)} / ${money(s.risk?.hard)}`; $('dailyGuard').textContent = `${money(s.risk?.daily_net)} / -${money(s.risk?.daily)}`; $('tradeGuard').textContent = `${s.risk?.used_today || 0} / ${s.risk?.max_trades || 0}`; $('riskUse').style.width = riskPct + '%';
  $('tradesPnl').textContent = (pnl >= 0 ? '+' : '') + money(pnl); $('tradesPnl').className = clsPnL(pnl); $('tradesWl').textContent = `${s.wins || 0} / ${s.losses || 0}`; $('tradesCount').textContent = s.trades || 0;
  const score = num(s.last_signal?.score || s.regime?.score); $('scoreValue').textContent = Math.round(score); $('strategyScoreLarge').textContent = Math.round(score); $('scoreRing').style.setProperty('--score', Math.min(100, score)); $('lastSignal').textContent = s.last_signal ? `${s.last_signal.side} · ${Math.round(score)}/100` : 'No signal'; $('strategyRegime').textContent = `${s.regime?.side || '—'} ${Math.round(num(s.regime?.score))}/100`;
  const sig = s.last_signal; $('signalDetails').innerHTML = sig ? Object.entries({Direction:sig.side,Score:`${sig.score}/100`,Underlying:num(sig.underlying).toFixed(2),Stop:num(sig.stop_underlying).toFixed(2),Target:num(sig.target_underlying).toFixed(2)}).map(([k,v]) => `<div class="info-row"><span>${k}</span><b>${esc(v)}</b></div>`).join('') : '<div class="empty-state">No active signal.</div>';
  renderFactors(s.regime?.factors || {}); renderPosition(s.position); renderQuote(s.quote); renderTrades(s); renderHealth(s.health || {}); renderEvents(s.events || []); renderAdaptive(s.adaptive || {}); renderBroker(s); renderRuntime(s); renderResearch(s); chartFromTrades(s);
}
async function refresh(manual = false) {
  try { render(await aSync('/api/status', { cache:'no-store' })); }
  catch (e) { $('statusPill').textContent = 'API OFFLINE'; $('statusPill').className = 'status-pill warn'; }
}
refresh(); setInterval(refresh, 1500);

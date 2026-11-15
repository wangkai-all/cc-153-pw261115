/* Object ingest demo — browser entry point.
 * All state lives server-side; this file only renders it and wires actions.
 */
'use strict';

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const state = {
  browsing: {
    prefix: '',
    pageSize: 10,
    depth: 0,           // 0 = first page
    cursors: [],        // cursors used to reach depth 1..n from the start
    cursor: null,       // cursor used to fetch current page
    nextCursor: null,
    items: [],
    versions: new Map(), // key -> version list
    expanded: new Set(),
  },
  selection: new Map(), // `${key}\0${version}` -> {key, version_id, follow_current, current, size, etag, is_current}
  jobId: null,
  job: null,
  tab: 'versions',
  pollTimer: null,
  manifestLoaded: false,
  eventCount: 0,
};

/* ------------------------------------------------------------------ utils */
function fmtBytes(n) {
  if (n === null || n === undefined) return '—';
  n = Number(n);
  if (!isFinite(n)) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`;
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function fp(etag) {
  if (!etag) return '—';
  return `<span title="${esc(etag)}">${esc(etag.slice(0, 10))}…${esc(etag.slice(-4))}</span>`;
}

function shortId(id) {
  return id ? id.replace(/^tr_/, '').slice(0, 10) : '—';
}

function fmtTime(ts) {
  if (!ts) return '—';
  const d = new Date(ts);
  if (isNaN(d.getTime())) return esc(ts);
  return d.toLocaleTimeString([], { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function toast(msg, kind = '') {
  const el = document.createElement('div');
  el.className = 'toast ' + kind;
  el.textContent = msg;
  $('#toasts').appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity .25s'; }, 3600);
  setTimeout(() => el.remove(), 3900);
}

async function api(path, opts = {}) {
  const init = { headers: { 'Accept': 'application/json' }, ...opts };
  if (opts.body !== undefined && typeof opts.body !== 'string') {
    init.body = JSON.stringify(opts.body);
    init.headers['Content-Type'] = 'application/json';
  }
  const res = await fetch(path, init);
  let payload = null;
  const text = await res.text();
  if (text) {
    try { payload = JSON.parse(text); } catch { payload = { raw: text }; }
  }
  if (!res.ok) {
    const err = new Error((payload && (payload.error || payload.message)) || `HTTP ${res.status}`);
    err.status = res.status;
    err.payload = payload;
    throw err;
  }
  return payload;
}

const STATUS_TEXT = {
  created: '已创建', scanning: '扫描中', ready: '就绪', importing: '导入中',
  cancelled: '已取消', error: '出错', complete: '已完成',
};
const PHASE_TEXT = {
  idle: '空闲', listing: '分页扫描中', resolving_history: '读取历史版本',
  ready: '就绪', importing: '导入中', cancelling: '正在取消', cancelled: '已取消', error: '出错',
};
const TRANSFER_TEXT = {
  pending: '待开始', queued: '排队中', downloading: '下载中', resuming: '续传中',
  interrupted: '已中断（保留进度）', complete: '已完成', failed: '失败',
  failed_source_gone: '源版本已消失', failed_integrity: '校验失败',
  superseded_version_changed: '源版本已变化（旧任务作废）', reused: '复用已落地文件',
  skipped: '已跳过',
};
const STATUS_CLASS = {
  complete: 'complete', reused: 'complete', skipped: 'skipped',
  interrupted: 'interrupted', resuming: 'resuming', downloading: 'downloading',
  pending: 'pending', queued: 'queued', failed: 'failed',
  failed_source_gone: 'failed_source_gone', failed_integrity: 'failed_integrity',
  superseded_version_changed: 'superseded_version_changed',
  tombstone: 'tombstone', not_selected: 'not_selected',
};

function stateBadge(status, label) {
  const cls = STATUS_CLASS[status] || 'not_selected';
  return `<span class="state ${cls}"><i class="dot"></i>${esc(label || TRANSFER_TEXT[status] || status)}</span>`;
}

/* --------------------------------------------------------------- browsing */
function currentPrefix() {
  return $('#prefix').value.trim();
}

async function browse({ reset = true } = {}) {
  const prefix = currentPrefix();
  const limit = Number($('#page-size').value) || 10;
  const note = $('#cursor-note');
  note.hidden = true;
  try {
    await fetchPage(null, 0, prefix, limit, reset ? 'reset' : 'keep');
  } catch (err) {
    if (err.status === 410) {
      // A first-page cursor should never expire; if it does, retry once.
      state.browsing.cursors = [];
      state.browsing.depth = 0;
      await fetchPage(null, 0, prefix, limit, 'reset');
      note.hidden = false;
      note.className = 'cursor-note';
      note.textContent = '分页游标已失效，已从头重新扫描。请重新确认当前选择。';
      return;
    }
    note.hidden = false;
    note.className = 'cursor-note error';
    note.textContent = `浏览失败：${err.message}`;
    toast(`浏览失败：${err.message}`, 'bad');
  }
}

/**
 * Fetch one listing page. On expiry (410) we restart from the first page
 * because an expired cursor means the server can no longer guarantee the
 * remaining order; dedupe happens by (key, version_id) server-side.
 */
async function fetchPage(cursor, depth, prefix, limit, mode) {
  const qs = `/api/store/list?prefix=${encodeURIComponent(prefix)}&limit=${limit}` +
    (cursor ? `&cursor=${encodeURIComponent(cursor)}` : '');
  const page = await api(qs);
  state.browsing.prefix = prefix;
  state.browsing.pageSize = limit;
  state.browsing.cursor = cursor;
  state.browsing.nextCursor = page.next_cursor;
  state.browsing.items = page.items || [];
  state.browsing.depth = depth;
  state.browsing.expanded.clear();
  if (mode === 'reset') state.browsing.cursors = [];
  renderBrowse();
  loadVersionsForVisible();
}

async function loadVersionsForVisible() {
  const keys = state.browsing.items.map((i) => i.key);
  await Promise.all(keys.map(async (key) => {
    try {
      const payload = await api(`/api/store/versions?key=${encodeURIComponent(key)}`);
      state.browsing.versions.set(key, payload.versions || []);
    } catch (err) { /* a key can vanish between pages; the row shows that */ }
  }));
  if (state.browsing.expanded.size) renderBrowse();
  updateSelectionSummary();
}

function renderBrowse() {
  const body = $('#browse-body');
  const items = state.browsing.items;
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="6" class="empty">该前缀下没有对象</td></tr>`;
    $('#browse-count').textContent = '0';
    $('#page-label').textContent = '无结果';
    $('#btn-prev').disabled = true;
    $('#btn-next').disabled = true;
    return;
  }
  $('#browse-count').textContent = `${items.length} 个键`;
  const pageNo = state.browsing.depth + 1;
  $('#page-label').textContent = `第 ${pageNo} 页`;
  $('#btn-prev').disabled = state.browsing.depth === 0;
  $('#btn-next').disabled = !state.browsing.nextCursor;

  body.innerHTML = items.map((item) => {
    const versions = state.browsing.versions.get(item.key) || [];
    const expanded = state.browsing.expanded.has(item.key);
    const current = versions.find((v) => v.is_current) || item;
    const tomb = current.is_delete_marker;
    const selCurrent = state.selection.get(`${item.key}\0${current.version_id}`)
      || state.selection.get(`${item.key}\0${item.version_id}`);
    const check = selCurrent
      ? `<input type="checkbox" data-act="toggle-current" data-key="${esc(item.key)}" checked>`
      : `<input type="checkbox" data-act="toggle-current" data-key="${esc(item.key)}">`;
    const versionRows = expanded ? `
      <tr class="version-extra"><td></td><td colspan="5">
        <div class="version-list">
          ${versions.length ? versions.map((v) => {
            const id = `${item.key}\0${v.version_id}`;
            const chosen = state.selection.get(id);
            const isCurrent = v.version_id === current.version_id;
            return `<div class="version-row ${chosen ? 'selected' : ''}">
              <input type="checkbox" data-act="toggle-version" data-key="${esc(item.key)}"
                     data-version="${esc(v.version_id)}" ${chosen ? 'checked' : ''}
                     ${v.is_delete_marker ? 'disabled' : ''}>
              <span class="vid">${esc(v.version_id)}</span>
              <span class="vmeta">${fmtBytes(v.size)}</span>
              <span class="vmeta">${fp(v.etag)}</span>
              <span class="vtag ${isCurrent ? 'current' : ''}">${isCurrent ? '当前' : '历史'}</span>
              ${v.is_delete_marker ? '<span class="vtag tombstone">删除标记</span>' : ''}
              ${chosen ? `<label class="toggle" title="下载期间若出现更新版本，是否自动改抓最新版">
                <input type="checkbox" data-act="toggle-follow" data-key="${esc(item.key)}"
                  data-version="${esc(v.version_id)}" ${chosen.follow_current ? 'checked' : ''}>
                <span>跟随最新</span></label>` : ''}
            </div>`;
          }).join('') : '<div class="hint">版本列表不可用</div>'}
        </div>
      </td></tr>` : '';
    return `<tr class="${item.is_delete_marker ? 'tomb-row' : ''}">
      <td class="check">${tomb ? '' : check}</td>
      <td>
        <div class="key-cell">${esc(item.key)}</div>
        <button class="btn small ghost" data-act="expand" data-key="${esc(item.key)}"
          style="padding:1px 6px;margin-top:3px;font-size:11px">${expanded ? '收起版本' : `查看 ${item.version_count ?? ''} 个版本`}</button>
      </td>
      <td class="num">${fmtBytes(current.size)}</td>
      <td><span class="mono">${esc(current.version_id)}</span></td>
      <td><span class="fp">${fp(current.etag)}</span></td>
      <td>${current.is_delete_marker
        ? '<span class="state tombstone"><i class="dot"></i>删除标记</span>'
        : '<span class="state complete"><i class="dot"></i>可下载</span>'}</td>
    </tr>${versionRows}`;
  }).join('');
}

function toggleSelection(key, version, follow = null) {
  const info = (state.browsing.versions.get(key) || []).find((v) => v.version_id === version);
  const item = state.browsing.items.find((i) => i.key === key);
  const id = `${key}\0${version}`;
  if (state.selection.has(id)) {
    if (follow === null) state.selection.delete(id);
    else if (follow) state.selection.get(id).follow_current = true;
    else state.selection.get(id).follow_current = false;
  } else {
    if (!info && !item) return;
    const isCurrent = info ? info.is_current : item.version_id === version;
    state.selection.set(id, {
      key, version_id: version, is_current: isCurrent, kind: 'version',
      size: info ? info.size : item.size,
      etag: info ? info.etag : item.etag,
      is_delete_marker: info ? info.is_delete_marker : item.is_delete_marker,
      // Picking a concrete version pins it; following the head is opt-in via
      // the "跟随最新" toggle. The row-level (current) checkbox is the only
      // path that follows by default.
      follow_current: follow === null ? false : follow,
    });
  }
  renderBrowse();
  updateSelectionSummary();
}

function updateSelectionSummary() {
  const n = state.selection.size;
  $('#sel-count').textContent = n;
  $('#btn-start').disabled = n === 0 && !$('#chk-auto').checked;
  $('#btn-sel-clear').disabled = n === 0;
  const keys = new Set(Array.from(state.selection.values()).map((s) => s.key));
  $('#sel-summary').textContent = n
    ? `（${keys.size} 个键，${Array.from(state.selection.values()).filter((s) => s.follow_current).length} 个跟随最新）`
    : '';
}

/* -------------------------------------------------------------------- job */
async function startJob() {
  const selections = Array.from(state.selection.values()).map((s) => ({
    kind: s.kind === 'current' ? 'current' : 'version',
    key: s.key,
    version_id: s.version_id,
    follow_current: s.follow_current,
  }));
  const auto = $('#chk-auto').checked;
  if (!selections.length && !auto) return;
  $('#btn-start').disabled = true;
  try {
    const payload = await api('/api/jobs', {
      method: 'POST',
      body: {
        prefix: currentPrefix(),
        page_size: Number($('#page-size').value) || 10,
        page_delay_ms: 900,
        include_history: $('#chk-history').checked,
        auto_import: auto,
        selections,
      },
    });
    const job = payload.job;
    state.jobId = job.id;
    state.manifestLoaded = false;
    state.eventCount = 0;
    toast(`任务已创建：${job.id}`, 'good');
    await refreshJobList();
    selectJob(job.id);
    startPolling();
  } catch (err) {
    toast(`创建任务失败：${err.message}`, 'bad');
  } finally {
    updateSelectionSummary();
  }
}

async function refreshJobList() {
  try {
    const payload = await api('/api/jobs');
    const sel = $('#job-select');
    const byId = new Map(payload.jobs.map((j) => [j.id, j]));
    sel.innerHTML = '';
    if (!payload.jobs.length) {
      sel.innerHTML = '<option value="">还没有任务</option>';
      return;
    }
    for (const job of payload.jobs) {
      const opt = document.createElement('option');
      opt.value = job.id;
      opt.textContent = `${job.prefix || '(全部)'} · ${job.id.slice(-6)} · ${STATUS_TEXT[job.status] || job.status}`;
      if (job.id === state.jobId) opt.selected = true;
      sel.appendChild(opt);
    }
  } catch (err) { /* header pills show connectivity */ }
}

function selectJob(jobId) {
  if (!jobId) return;
  state.jobId = jobId;
  state.manifestLoaded = false;
  state.eventCount = 0;
  $('#job-empty').hidden = true;
  $('#job-content').hidden = false;
  fetchJob();
  startPolling();
}

function startPolling() {
  stopPolling();
  state.pollTimer = setInterval(() => {
    if (state.jobId) fetchJob({ quiet: true });
  }, 1000);
}
function stopPolling() {
  if (state.pollTimer) { clearInterval(state.pollTimer); state.pollTimer = null; }
}

async function fetchJob({ quiet = false } = {}) {
  if (!state.jobId) return;
  try {
    const [job, events] = await Promise.all([
      api(`/api/jobs/${state.jobId}`),
      api(`/api/jobs/${state.jobId}/events?after=${state.eventCount}`),
    ]);
    state.job = job;
    if (job.status === 'error' && !quiet) toast(`任务出错：${job.error || ''}`, 'bad');
    if (events.events.length) {
      state.eventCount = events.events[events.events.length - 1].id;
    }
    renderJob(job, events.events);
    updateHeader(events.stats);
    if (job.status === 'ready' && !quiet) stopPolling();
  } catch (err) {
    if (!quiet) toast(`读取任务失败：${err.message}`, 'bad');
  }
}

function updateHeader(stats) {
  if (stats) {
    $('#ingest-pill').textContent =
      `采集 ${stats.ingested} 完成 · ${stats.interrupted} 中断 · ${stats.version_conflicts} 版本变更`;
    $('#ingest-pill').className = 'pill' + (stats.active_jobs ? ' ok' : ' muted');
  }
  api('/api/store/stats').then((s) => {
    const pill = $('#store-pill');
    if (s.error) { pill.textContent = '存储不可用'; pill.className = 'pill bad'; return; }
    pill.textContent = `存储 ${s.keys} 键 · ${s.versions} 版本 · ${fmtBytes(s.bytes)}`;
    pill.className = 'pill ok';
  }).catch(() => {
    $('#store-pill').textContent = '存储不可用';
    $('#store-pill').className = 'pill bad';
  });
}

function renderJob(job, newEvents) {
  $('#job-subtitle').textContent =
    `${job.prefix ? '前缀 “' + job.prefix + '”' : '全量'} · 任务 ${job.id} · ${job.created_at}`;
  $('#job-status').textContent = STATUS_TEXT[job.status] + (job.phase && job.phase !== 'ready' && job.phase !== job.status ? ` / ${PHASE_TEXT[job.phase] || job.phase}` : '');
  $('#job-gen').textContent = job.generation ?? '—';
  $('#job-pages').textContent = job.pages ?? 0;
  $('#job-counts').textContent = `${job.keys ?? 0} / ${job.versions ?? 0}`;
  const statusCounts = {};
  for (const row of job.discoveries || []) {
    const t = (job.transfers || []).find((x) => x.key === row.key && x.version_id === row.version_id);
    const s = row.is_delete_marker ? 'tombstone' : (t ? t.state : 'not_selected');
    statusCounts[s] = (statusCounts[s] || 0) + 1;
  }
  $('#job-done').textContent = statusCounts.complete || 0;
  $('#job-skipped').textContent = (statusCounts.reused || 0) + (statusCounts.tombstone || 0);
  $('#chk-job-auto').checked = !!job.auto_import;

  const transfers = job.transfers || [];
  const totals = transfers.length || 1;
  const complete = transfers.filter((t) => t.state === 'complete').length;
  const bytesDone = transfers.reduce((a, t) => a + (t.bytes || 0), 0);
  const bytesAll = transfers.reduce((a, t) => a + (t.size || 0), 0) || 1;
  const pct = job.status === 'ready' || job.status === 'complete'
    ? 100
    : Math.max(2, Math.min(100, Math.round(bytesDone / bytesAll * 100)));
  $('#job-progress-fill').style.width = `${pct}%`;
  $('#job-progress-text').textContent =
    `${fmtBytes(bytesDone)} / ${fmtBytes(bytesAll)} · ${complete}/${transfers.length} 文件`;

  $('#btn-job-import').disabled = job.status === 'importing' || job.status === 'scanning';
  $('#btn-job-rescan').disabled = job.status === 'scanning';
  $('#btn-job-cancel').disabled = job.status === 'cancelled';

  renderVersionsTab(job);
  renderTransfersTab(job);
  if (newEvents && newEvents.length) {
    state.recentEvents = (state.recentEvents || []).concat(newEvents).slice(-400);
    renderEvents(state.recentEvents);
  }
  if (state.tab === 'manifest' && !state.manifestLoaded) loadManifestPreview({ quiet: true });
}

function renderVersionsTab(job) {
  const rows = job.discoveries || [];
  const transfers = job.transfers || [];
  const body = $('#versions-body');
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" class="empty">尚无发现记录</td></tr>`;
    return;
  }
  body.innerHTML = rows.map((d) => {
    const chain = transfers.filter((t) => t.key === d.key && t.version_id === d.version_id);
    const latest = chain[chain.length - 1];
    let status = 'not_selected';
    let reason = '';
    let bytes = 0;
    let sha = '';
    if (d.is_delete_marker) {
      status = 'tombstone'; reason = '删除标记，仅记录不下载';
    } else if (latest) {
      status = latest.state;
      bytes = latest.bytes || 0;
      sha = latest.sha256 || '';
      if (latest.state === 'complete') reason = latest.origin === 'reused' ? '复用已落地文件' : '已校验指纹';
      else if (['interrupted', 'resuming', 'downloading'].includes(latest.state)) {
        reason = `已下载 ${fmtBytes(latest.bytes)} / ${fmtBytes(latest.size)}`;
      } else if (latest.state === 'superseded_version_changed') reason = latest.note || '源已变化，旧任务作废';
      else if (latest.state === 'failed_source_gone') reason = '源版本已删除或消失';
      else if (latest.state === 'failed_integrity') reason = '内容指纹不匹配';
      else if (latest.state === 'pending') reason = '等待下载';
    } else {
      reason = job.status === 'ready' ? '已发现，未选择导入' : '等待扫描完成';
    }
    const retry = latest && ['interrupted', 'failed', 'failed_source_gone', 'superseded_version_changed'].includes(latest.state)
      ? `<button class="btn small ghost" data-act="retry-one" data-key="${esc(d.key)}" data-version="${esc(d.version_id)}">重试</button>`
      : '';
    return `<tr>
      <td>
        <div class="key-cell">${esc(d.key)}</div>
        <div class="fp">${d.discovered_by === 'history' ? '历史版本' : d.discovered_by === 'browse' ? '浏览选择' : '列表发现'} · 第 ${d.generation} 代</div>
      </td>
      <td><span class="mono">${esc(d.version_id)}</span>${d.is_current ? ' <span class="vtag current">当前</span>' : ''}</td>
      <td class="num">${fmtBytes(d.size)}</td>
      <td class="fp">${fmtTime(d.discovered_at)}</td>
      <td>${latest ? `${stateBadge(latest.state)}<div class="fp">${fmtBytes(bytes)} / ${fmtBytes(d.size)} · 尝试 ${latest.attempts}</div>` : '<span class="muted">—</span>'}</td>
      <td>${stateBadge(status === 'not_selected' ? 'not_selected' : status, status === 'complete' ? '已入库' : (TRANSFER_TEXT[status] || '未选择'))}
        <div class="fp">${esc(reason)}</div>
        ${sha ? `<div class="fp mono" title="${esc(sha)}">sha256 ${esc(sha.slice(0, 12))}…</div>` : ''}</td>
      <td>${retry}</td>
    </tr>`;
  }).join('');
}

function renderTransfersTab(job) {
  const transfers = (job.transfers || []).slice().reverse();
  const body = $('#transfers-body');
  if (!transfers.length) {
    body.innerHTML = `<tr><td colspan="6" class="empty">暂无传输记录</td></tr>`;
    return;
  }
  body.innerHTML = transfers.map((t) => {
    const pct = t.size ? Math.min(100, Math.round((t.bytes || 0) / t.size * 100)) : (t.state === 'complete' ? 100 : 0);
    return `<tr>
      <td><span class="mono">${esc(shortId(t.id))}</span><div class="fp">${esc((t.origin || ''))}</div></td>
      <td><div class="key-cell">${esc(t.key)}</div><div class="fp">${esc(t.version_id)}</div></td>
      <td class="num">${fmtBytes(t.bytes)}<div class="fp">/ ${fmtBytes(t.size)} · ${pct}%</div></td>
      <td class="num">${t.attempts ?? 0}</td>
      <td>${stateBadge(t.state)}</td>
      <td class="fp">${esc(t.note || t.error || '')}${t.reused_from ? `<div>复用自 ${esc(shortId(t.reused_from))}</div>` : ''}</td>
    </tr>`;
  }).join('');
}

function renderEvents(events) {
  const log = $('#event-log');
  if (!events.length) {
    log.innerHTML = '<div class="hint" style="padding:10px">尚无事件</div>';
    return;
  }
  log.innerHTML = events.map((e) => {
    let cls = '';
    if (/error|failed|superseded|gone|integrity|expired|cancel/.test(e.kind)) cls = 'bad';
    else if (/interrupt|retarget|replan|changed|reuse|skip/.test(e.kind)) cls = 'warn';
    else if (/complete|landed/.test(e.kind)) cls = 'good';
    else if (/tombstone|delete/.test(e.kind)) cls = 'purple';
    return `<div class="event-line ${cls}">
      <span class="ts">${fmtTime(e.ts)}</span>
      <span class="kind" title="${esc(e.kind)}">${esc(e.kind)}</span>
      <span class="msg">${esc(e.message || '')}</span>
    </div>`;
  }).join('');
  log.scrollTop = log.scrollHeight;
}

async function loadManifestPreview({ quiet = false, force = false } = {}) {
  if (!state.jobId) return;
  if (state.manifestLoaded && !force) return;
  try {
    const manifest = await fetch(`/api/jobs/${state.jobId}/manifest`).then((r) => r.json());
    state.manifestLoaded = true;
    const summary = manifest.summary || {};
    $('#manifest-meta').textContent =
      `生成于 ${manifest.generated_at || '—'} · ` +
      Object.entries(summary).map(([k, v]) => `${TRANSFER_TEXT[k] || k}: ${v}`).join(' · ');
    const compact = {
      job: manifest.job,
      summary: manifest.summary,
      objects: (manifest.objects || []).map((o) => ({
        key: o.key, version_id: o.version_id, size: o.size, etag: o.etag,
        import_status: o.import_status, sha256: o.sha256, reason: o.reason,
      })),
      totals: manifest.totals,
    };
    $('#manifest-preview').textContent = JSON.stringify(compact, null, 2);
  } catch (err) {
    if (!quiet) toast(`清单读取失败：${err.message}`, 'bad');
  }
}

function downloadManifest(format) {
  if (!state.jobId) return;
  window.location.href = `/api/jobs/${state.jobId}/manifest?format=${format}`;
}

/* -------------------------------------------------------------- mutations */
async function mutate(kind, body, successMsg) {
  try {
    await api(`/api/demo/${kind}`, { method: 'POST', body });
    if (successMsg) toast(successMsg, 'good');
    await loadFaults();
    updateHeader();
    if (state.browsing.items.length) browse({ reset: false });
  } catch (err) {
    toast(`${kind} 失败：${err.message}`, 'bad');
  }
}

async function loadFaults() {
  try {
    const payload = await api('/api/store/faults');
    const list = payload.faults || [];
    const el = $('#fault-list');
    if (!list.length) { el.innerHTML = ''; return; }
    el.innerHTML = list.map((f) =>
      `<div class="fault-item">⚠ ${esc(f.key)} 将在 ${fmtBytes(f.after_bytes)} 后断流 · 剩余 ${f.times_left} 次</div>`
    ).join('');
  } catch { /* store may be down */ }
}

/* ------------------------------------------------------------------ tabs */
function setTab(name) {
  state.tab = name;
  $$('.tab').forEach((t) => t.classList.toggle('active', t.dataset.tab === name));
  for (const id of ['versions', 'transfers', 'events', 'manifest']) {
    $(`#tab-${id}`).hidden = id !== name;
  }
  if (name === 'events' && state.jobId) {
    api(`/api/jobs/${state.jobId}/events`).then((payload) => {
      state.recentEvents = payload.events || [];
      if (state.recentEvents.length) state.eventCount = state.recentEvents[state.recentEvents.length - 1].id;
      renderEvents(state.recentEvents);
    });
  }
  if (name === 'manifest') loadManifestPreview();
}

/* ---------------------------------------------------------------- events */
function bindEvents() {
  $('#btn-browse').addEventListener('click', () => browse({ reset: true }));
  $('#prefix').addEventListener('keydown', (e) => { if (e.key === 'Enter') browse({ reset: true }); });
  $('#page-size').addEventListener('change', () => browse({ reset: true }));
  $('#chk-history').addEventListener('change', () => {
    if (state.browsing.items.length) loadVersionsForVisible();
  });

  $('#btn-prev').addEventListener('click', async () => {
    if (state.browsing.depth === 0) return;
    const prevCursor = state.browsing.depth >= 2
      ? state.browsing.cursors[state.browsing.depth - 2]
      : null;
    const depth = state.browsing.depth - 1;
    try {
      await fetchPage(prevCursor, depth, currentPrefix(),
        Number($('#page-size').value) || 10, 'keep');
      state.browsing.cursors = state.browsing.cursors.slice(0, depth);
      $('#cursor-note').hidden = true;
    } catch (err) {
      if (err.status === 410) {
        toast('上一页的游标已失效，已从头重新扫描', 'bad');
        state.browsing.cursors = [];
        await browse({ reset: true });
      } else toast(`翻页失败：${err.message}`, 'bad');
    }
  });
  $('#btn-next').addEventListener('click', async () => {
    const next = state.browsing.nextCursor;
    if (!next) return;
    const cursorUsed = state.browsing.cursor;
    const depth = state.browsing.depth + 1;
    try {
      await fetchPage(next, depth, currentPrefix(),
        Number($('#page-size').value) || 10, 'keep');
      state.browsing.cursors = state.browsing.cursors
        .slice(0, depth - 1).concat([cursorUsed]);
      $('#cursor-note').hidden = true;
    } catch (err) {
      if (err.status === 410) {
        state.browsing.cursors = [];
        await browse({ reset: true });
        $('#cursor-note').hidden = false;
        $('#cursor-note').className = 'cursor-note';
        $('#cursor-note').textContent =
          '分页游标已失效，已从头重新扫描。失效原因可能是存储侧重新排序或数据被改动，请重新确认选择。';
        return;
      }
      toast(`翻页失败：${err.message}`, 'bad');
    }
  });

  $('#browse-body').addEventListener('click', (e) => {
    const btn = e.target.closest('[data-act]');
    if (!btn) return;
    const act = btn.dataset.act;
    if (act === 'expand') {
      const key = btn.dataset.key;
      if (state.browsing.expanded.has(key)) state.browsing.expanded.delete(key);
      else state.browsing.expanded.add(key);
      renderBrowse();
    } else if (act === 'toggle-version') {
      toggleSelection(btn.dataset.key, btn.dataset.version);
    } else if (act === 'toggle-follow') {
      toggleSelection(btn.dataset.key, btn.dataset.version, btn.checked);
    }
  });
  // Current-version checkboxes are rendered with the listing's version id;
  // resolve it on click so the selection always targets a concrete version.
  $('#browse-body').addEventListener('change', (e) => {
    const box = e.target;
    if (box.matches('[data-act="toggle-current"]')) {
      const key = box.dataset.key;
      const item = state.browsing.items.find((i) => i.key === key);
      if (!item) return;
      const versions = state.browsing.versions.get(key) || [];
      const current = versions.find((v) => v.is_current);
      const version = current ? current.version_id : item.version_id;
      const id = `${key}\0${version}`;
      if (state.selection.has(id)) state.selection.delete(id);
      else {
        const info = current || item;
        state.selection.set(id, {
          key, version_id: version, is_current: true, kind: 'current', size: info.size,
          etag: info.etag, is_delete_marker: info.is_delete_marker,
          follow_current: true,
        });
      }
      renderBrowse();
      updateSelectionSummary();
    }
  });

  $('#btn-sel-clear').addEventListener('click', () => {
    state.selection.clear();
    renderBrowse();
    updateSelectionSummary();
  });
  $('#chk-auto').addEventListener('change', updateSelectionSummary);
  $('#btn-start').addEventListener('click', startJob);

  $('#job-select').addEventListener('change', (e) => selectJob(e.target.value));

  $('#btn-job-import').addEventListener('click', async () => {
    if (!state.jobId) return;
    try {
      const payload = await api(`/api/jobs/${state.jobId}/import`, { method: 'POST', body: {} });
      toast('已提交导入', 'good');
      state.manifestLoaded = false;
      renderJob(payload.job);
      startPolling();
    } catch (err) { toast(`导入失败：${err.message}`, 'bad'); }
  });
  $('#btn-job-resume').addEventListener('click', async () => {
    if (!state.jobId) return;
    try {
      await api(`/api/jobs/${state.jobId}/resume`, { method: 'POST', body: {} });
      toast('已恢复；续传前会重新确认服务端版本', 'good');
      startPolling();
    } catch (err) { toast(`续传失败：${err.message}`, 'bad'); }
  });
  $('#btn-job-rescan').addEventListener('click', async () => {
    if (!state.jobId) return;
    try {
      await api(`/api/jobs/${state.jobId}/rescan`, { method: 'POST', body: {} });
      toast('已触发重新扫描', 'good');
      state.manifestLoaded = false;
      startPolling();
    } catch (err) { toast(`重新扫描失败：${err.message}`, 'bad'); }
  });
  $('#btn-job-cancel').addEventListener('click', async () => {
    if (!state.jobId) return;
    try {
      await api(`/api/jobs/${state.jobId}/cancel`, { method: 'POST', body: {} });
      toast('已请求取消；已下载的进度会保留', '');
    } catch (err) { toast(`取消失败：${err.message}`, 'bad'); }
  });
  $('#chk-job-auto').addEventListener('change', async (e) => {
    if (!state.jobId) return;
    try {
      await api(`/api/jobs/${state.jobId}/auto-import`, {
        method: 'POST', body: { enabled: e.target.checked },
      });
      toast(e.target.checked ? '已启用自动导入' : '已关闭自动导入', 'good');
    } catch (err) { toast(`设置失败：${err.message}`, 'bad'); }
  });
  $('#btn-manifest-json').addEventListener('click', () => downloadManifest('json'));
  $('#btn-manifest-csv').addEventListener('click', () => downloadManifest('csv'));
  $('#btn-manifest-refresh').addEventListener('click', () => {
    state.manifestLoaded = false;
    loadManifestPreview({ force: true });
  });

  $('#versions-body').addEventListener('click', async (e) => {
    const btn = e.target.closest('[data-act="retry-one"]');
    if (!btn) return;
    if (!state.jobId) return;
    try {
      await api(`/api/jobs/${state.jobId}/import`, {
        method: 'POST',
        body: { selections: [{ kind: 'version', key: btn.dataset.key, version_id: btn.dataset.version, follow_current: false }] },
      });
      toast('已为选中版本创建独立传输', 'good');
      startPolling();
    } catch (err) { toast(`重试失败：${err.message}`, 'bad'); }
  });

  $$('.tab').forEach((t) => t.addEventListener('click', () => setTab(t.dataset.tab)));

  // Demo mutations
  $('#btn-put').addEventListener('click', () => {
    const key = $('#mut-put-key').value.trim();
    if (!key) { toast('请填写对象键', 'bad'); return; }
    mutate('put', { key, content: $('#mut-put-content').value.trim() || undefined },
      `已覆盖 ${key}`);
  });
  $('#btn-del').addEventListener('click', () => {
    const key = $('#mut-del-key').value.trim();
    if (!key) { toast('请填写对象键', 'bad'); return; }
    mutate('delete', { key }, `已为 ${key} 生成删除标记`);
  });
  $('#btn-del-recreate').addEventListener('click', () => {
    const key = $('#mut-del-key').value.trim();
    if (!key) { toast('请填写对象键', 'bad'); return; }
    mutate('delete', { key, recreate: true }, `已删除并重建 ${key}`);
  });
  $('#btn-fault').addEventListener('click', () => {
    const key = $('#mut-fault-key').value.trim() || '*';
    const after = Number($('#mut-fault-bytes').value) || 1024;
    const times = Number($('#mut-fault-times').value) || 1;
    mutate('fault', { key, after_bytes: after, times }, '已安排断流');
  });
  $('#btn-fault-clear').addEventListener('click', async () => {
    try {
      await fetch('/api/demo/faults/clear', { method: 'POST' });
      await loadFaults();
      toast('已清除断流', 'good');
    } catch (err) { toast(`清除失败：${err.message}`, 'bad'); }
  });
  $('#btn-invalidate').addEventListener('click', async () => {
    try {
      const out = await api('/api/demo/invalidate-cursors', { method: 'POST', body: {} });
      toast(`游标已失效（纪元 ${out.cursor_epoch}）`, 'good');
    } catch (err) { toast(`操作失败：${err.message}`, 'bad'); }
  });
  $('#btn-reseed').addEventListener('click', async () => {
    try {
      await api('/api/demo/seed', { method: 'POST', body: { reset: true } });
      toast('演示数据已重建', 'good');
      state.browsing.versions.clear();
      if (state.browsing.items.length) browse({ reset: true });
      updateHeader();
    } catch (err) { toast(`重建失败：${err.message}`, 'bad'); }
  });
  $('#btn-reset').addEventListener('click', async () => {
    if (!confirm('这会清空全部采集任务、落地文件并重建演示存储，确定吗？')) return;
    try {
      await api('/api/demo/reset', { method: 'POST', body: {} });
      state.selection.clear();
      state.jobId = null;
      state.job = null;
      state.browsing.versions.clear();
      state.browsing.cursors = [];
      state.recentEvents = [];
      $('#job-content').hidden = true;
      $('#job-empty').hidden = false;
      $('#job-subtitle').textContent = '还没有任务。选择版本后点「发现并导入」。';
      stopPolling();
      await refreshJobList();
      updateHeader();
      if (state.browsing.items.length) browse({ reset: true });
      toast('演示已重置', 'good');
    } catch (err) { toast(`重置失败：${err.message}`, 'bad'); }
  });
}

/* ------------------------------------------------------------------- boot */
async function boot() {
  bindEvents();
  const chips = ['datasets/', 'incoming/', 'logs/', 'archive/', ''];
  $('#prefix-chips').innerHTML = chips.map((p) =>
    `<button class="chip" data-prefix="${esc(p)}">${p ? esc(p) : '（全部）'}</button>`).join('');
  $('#prefix-chips').addEventListener('click', (e) => {
    const chip = e.target.closest('.chip');
    if (!chip) return;
    $('#prefix').value = chip.dataset.prefix;
    browse({ reset: true });
  });

  await refreshJobList();
  updateHeader();
  await loadFaults();

  const jobs = await api('/api/jobs').catch(() => ({ jobs: [] }));
  if (jobs.jobs && jobs.jobs.length) {
    selectJob(jobs.jobs[0].id);
    state.jobId = jobs.jobs[0].id;
  }
  setTab('versions');
  $('#prefix').value = 'incoming/';
  browse({ reset: true });
}

document.addEventListener('DOMContentLoaded', boot);

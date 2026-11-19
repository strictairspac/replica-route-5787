/* 写入一致性演练场 · 页面控制器（零依赖原生 ES Module，消费 engine.js 的 UMD 全局 WCEngine） */
const { createSim, WRITE_STATUS } = window.WCEngine;

const LS_HISTORY = 'wc-lab-history-v1';
const LS_CURRENT = 'wc-lab-current-v1';

const el = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

let sim = null;
let config = { nodeCount: 3, seed: 20261119, autoRetry: false };
let eventFilter = 'all';
let toastTimer = null;

function toast(msg) {
  document.querySelectorAll('.toast').forEach(t => t.remove());
  const d = document.createElement('div');
  d.className = 'toast';
  d.textContent = msg;
  document.body.appendChild(d);
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => d.remove(), 2200);
}

// ---------------- 生命周期 ----------------
function newSim() {
  sim = createSim(config);
  render();
  refreshReadNodeOptions();
  persistCurrent();
}

function act(fn, msg) {
  try { fn(); } catch (e) { console.error(e); toast('动作执行异常：' + e.message); }
  // 每个交互动作后推进虚拟时钟到稳定态：真实展现“等回执”和“等换主”的时间代价
  sim.runUntilIdle();
  render();
  refreshReadNodeOptions();
  persistCurrent();
  if (msg) toast(msg);
}

// ---------------- 历史（旧试验回看） ----------------
function loadHistory() {
  try { return JSON.parse(localStorage.getItem(LS_HISTORY) || '[]'); } catch { return []; }
}
function saveHistory(list) { localStorage.setItem(LS_HISTORY, JSON.stringify(list.slice(0, 50))); }

function saveTrial() {
  const st = sim.getState();
  const name = prompt('给这次试验起个名字', `试验-${new Date().toLocaleString('zh-CN', { hour12: false })}`);
  if (!name) return;
  const list = loadHistory();
  list.unshift({
    id: 'T' + Date.now().toString(36),
    name,
    savedAt: Date.now(),
    config: { ...config },
    actions: sim.actions,
    checksum: sim.checksum(),
    summary: {
      writes: st.writes.map(w => ({ writeId: w.writeId, status: w.status, index: w.index })),
      downtime: st.totalDowntimeMs, leaderChanges: st.leaderChanges,
      dropped: st.droppedCount, leaderId: st.leaderId, clock: st.now
    }
  });
  saveHistory(list);
  renderHistory();
  toast('试验已存档，可在右侧“历史试验”回看');
}

function replayTrial(id) {
  const item = loadHistory().find(t => t.id === id);
  if (!item) return;
  config = { ...item.config };
  sim = createSim(config);
  for (const a of item.actions) sim.applyAction(a);
  render();
  refreshReadNodeOptions();
  persistCurrent();
  toast(`已回看「${item.name}」：重放 ${item.actions.length} 个动作，校验和 ${sim.checksum()}`);
}

function deleteTrial(id) {
  saveHistory(loadHistory().filter(t => t.id !== id));
  renderHistory();
}

function persistCurrent() {
  localStorage.setItem(LS_CURRENT, JSON.stringify({ config, actions: sim.actions }));
}
function restoreCurrent() {
  try {
    const raw = JSON.parse(localStorage.getItem(LS_CURRENT) || 'null');
    if (!raw || !Array.isArray(raw.actions) || !raw.actions.length) return false;
    config = { ...config, ...raw.config };
    sim = createSim(config);
    for (const a of raw.actions) sim.applyAction(a);
    return true;
  } catch { return false; }
}

// ---------------- 预置剧本 ----------------
const SCENARIOS = [
  {
    key: 'ack-lost', name: '① 写完没收到回执，怎么查清',
    desc: '关键写等待多数派回执；回执被丢后客户端停在“待确认”，用写号向主库查明，确认数据只生效一次。',
    run() {
      config = { nodeCount: 3, seed: 20261119, autoRetry: false };
      sim = createSim(config);
      sim.submitWrite({ writeId: 'W-2001', key: 'payment', value: '¥300', critical: true, session: 's1' });
      sim.armDropAck();
      sim.flush();   // 集群侧已多数派提交，但客户端回执被丢 → 台账停在“待确认”
      sim.inquire('W-2001');
      sim.flush();   // 查明：补回确认，数据不重复生效
      sim.retryWrite('W-2001');
      sim.flush();   // 再试一次：幂等命中，日志长度不变
    }
  },
  {
    key: 'conflict', name: '② 同号换内容必须拒绝',
    desc: '写号一次绑定内容。故意用同一写号提交不同的值，主库与状态机两道防线拒绝，原值不动。',
    run() {
      config = { nodeCount: 3, seed: 11, autoRetry: false };
      sim = createSim(config);
      sim.submitWrite({ writeId: 'W-2002', key: 'title', value: '初稿', critical: true, session: 's1' });
      sim.runUntilIdle();
      sim.resubmitDifferent('W-2002', '被篡改的内容');
      sim.flush();
    }
  },
  {
    key: 'failover', name: '③ 主库下线换主，已确认写入不丢',
    desc: '多数派确认的写入存在于至少两台节点。主库下线后日志够新的副本当选，新主状态机保有全部已确认数据；顶部显示本次不可用代价。',
    run() {
      config = { nodeCount: 3, seed: 23, autoRetry: false };
      sim = createSim(config);
      sim.submitWrite({ writeId: 'W-2003', key: 'order', value: 'A-100', critical: true, session: 's7' });
      sim.runUntilIdle();
      sim.submitWrite({ writeId: 'W-2004', key: 'order', value: 'A-200', critical: true, session: 's7' });
      sim.runUntilIdle();
      sim.killNode('n1');
      sim.runUntilIdle();
      sim.read({ session: 's7', key: 'order' });
      sim.flush();
    }
  },
  {
    key: 'local-ack', name: '④ 副本没追齐：非关键写继续，但换主会丢',
    desc: '隔离主库到副本的链路后，非关键写只拿本地回执即可继续；一旦旧主下线，未进多数派的确认丢失。关键写则被挡住等待。',
    run() {
      config = { nodeCount: 3, seed: 31, autoRetry: false };
      sim = createSim(config);
      sim.blockLink('n1', 'n2');
      sim.blockLink('n1', 'n3');
      sim.submitWrite({ writeId: 'W-2005', key: 'log', value: 'besteffort', critical: false, session: 's2' });
      sim.flush();
      sim.submitWrite({ writeId: 'W-2006', key: 'log', value: 'must-durable', critical: true, session: 's2' });
      sim.flush();
      sim.killNode('n1');
      sim.runUntilIdle();
    }
  },
  {
    key: 'no-quorum', name: '⑤ 无多数派：写入排队待确认，恢复后同号重试',
    desc: '两台节点下线后无多数派，关键写保持“待确认”不猜测结果；恢复并选出新主后，按同一写号重试成功，全程没有重复生效。',
    run() {
      config = { nodeCount: 3, seed: 47, autoRetry: false };
      sim = createSim(config);
      sim.killNode('n1');
      sim.killNode('n2');
      sim.submitWrite({ writeId: 'W-2007', key: 'config', value: 'v=1', critical: true, session: 's3' });
      sim.recoverNode('n2');
      sim.runUntilIdle();
      sim.retryWrite('W-2007');
      sim.runUntilIdle();
      sim.inquire('W-2007');
      sim.flush();
    }
  },
  {
    key: 'stale-read', name: '⑥ 同会话读己之写：滞后副本读什么',
    desc: '已确认写入后隔离一个副本再写新值。直接读滞后副本会明确提示本会话哪些写号尚未应用；走主库则读到自己的最新值。',
    run() {
      config = { nodeCount: 3, seed: 59, autoRetry: false };
      sim = createSim(config);
      sim.submitWrite({ writeId: 'W-2008', key: 'balance', value: '100', critical: true, session: 's9' });
      sim.runUntilIdle();
      sim.blockLink('n1', 'n3');
      sim.submitWrite({ writeId: 'W-2009', key: 'balance', value: '80', critical: true, session: 's9' });
      sim.runUntilIdle();
      sim.read({ session: 's9', key: 'balance', nodeId: 'n3' });
      sim.flush();
      sim.read({ session: 's9', key: 'balance' });
      sim.flush();
    }
  }
];

// ---------------- 渲染 ----------------
function statusBadge(st) {
  const meta = WRITE_STATUS[st] || { label: st, cls: 'info' };
  const pulse = (st === 'PENDING' || st === 'LOCAL_ACKED') ? ' pending-pulse' : '';
  return `<span class="badge ${meta.cls}${pulse}">${meta.label}</span>`;
}

function render() {
  const st = sim.getState();
  el('#clock').textContent = `虚拟时钟 ${st.now} ms`;
  el('#checksum').textContent = `重放校验和 ${sim.checksum()}`;
  renderStats(st);
  renderNodes(st);
  renderWrites(st);
  renderTimeline(st);
  renderLinks(st);
  el('#auto-retry').checked = st.autoRetry;
  el('#node-count').value = String(config.nodeCount);
}

function renderStats(st) {
  const ldr = st.nodes.find(n => n.id === st.leaderId);
  const pending = st.writes.filter(w => w.status === 'PENDING' || w.status === 'LOCAL_ACKED').length;
  const quorum = st.quorum;
  const votes = Array.from({ length: st.nodes.length }, (_, i) =>
    `<span class="qvote ${i < quorum ? (i === 0 ? 'self' : 'yes') : ''}" title="${i + 1} 票"></span>`).join('');
  el('#stats').innerHTML = `
    <div class="stats">
      <div class="stat"><div class="v accent">${ldr ? esc(ldr.name) : '无主'}</div><div class="k">当前主库 / term ${ldr ? ldr.term : '—'}</div></div>
      <div class="stat"><div class="v ${pending ? 'warn' : 'ok'}">${pending}</div><div class="k">待确认写入</div></div>
      <div class="stat"><div class="v warn">${st.totalDowntimeMs} ms</div><div class="k">累计无主不可用（换主代价）</div></div>
      <div class="stat"><div class="v">${st.leaderChanges}</div><div class="k">换主次数</div></div>
      <div class="stat"><div class="v danger">${st.droppedCount}</div><div class="k">被丢弃消息/回执</div></div>
      <div class="stat"><div class="v">${st.now} ms</div><div class="k">写入等待累计代价（时钟）</div></div>
    </div>
    <div class="quorum-bar" title="多数派票数">${votes}<span style="font-size:11px;color:var(--dim);margin-left:8px">确认需 ${quorum}/${st.nodes.length} 票多数派</span></div>
    ${st.dropArmed ? '<div class="note danger">已布控：下一条发往客户端的回执将被丢弃。</div>' : ''}
    ${!ldr ? '<div class="note danger">集群当前无多数派主库：关键写一律保持“待确认”，不得猜测成功或换新号重写。</div>' : ''}`;
}

function renderNodes(st) {
  el('#nodes').innerHTML = st.nodes.map(n => {
    const isLeader = n.id === st.leaderId;
    const logChips = n.log.map(e => e.noop
      ? `<span title="no-op">∅${e.index}</span>`
      : `<span title="term ${e.term}">${e.index}:${esc(e.writeId)}</span>`).join(' ');
    const kv = Object.entries(n.kv).map(([k, v]) => `<code class="k">${esc(k)}=${esc(v)}</code>`).join(' ') || '∅';
    return `<div class="node ${isLeader ? 'leader' : ''} ${n.alive ? '' : 'down'}">
      <div class="nm"><span class="dot ${n.alive ? (isLeader ? 'warn' : 'ok') : 'down'}"></span>
        ${esc(n.name)} <span class="role ${isLeader ? 'leader' : ''}">${isLeader ? 'leader' : n.role}${n.alive ? '' : ' / 下线'}</span>
        <span class="spacer" style="flex:1"></span>
        <span class="mono" style="font-size:11px;color:var(--dim)">term ${n.term}</span></div>
      <div class="meta">
        <span>日志长度 ${n.logLen}</span><span>提交位点 ${n.commitIndex}</span>
        <span>应用位点 ${n.appliedIndex}</span><span>${isLeader ? '主库' : '跟随者'}</span>
      </div>
      <div class="logline">${logChips || '（空日志）'}</div>
      <div class="kv">状态机：${kv}</div>
      <div class="ops-mini" style="margin-top:8px">
        ${n.alive
          ? `<button class="btn danger" data-act="kill" data-id="${n.id}">下线</button>`
          : `<button class="btn ok" data-act="recover" data-id="${n.id}">恢复</button>`}
        <button class="btn" data-act="elect" data-id="${n.id}" ${!n.alive ? 'disabled' : ''}>立即参选</button>
      </div>
    </div>`;
  }).join('');
}

function renderWrites(st) {
  if (!st.writes.length) {
    el('#writes').innerHTML = '<div class="note">还没有写入。从左侧提交一笔，或加载一个预置剧本。</div>';
    return;
  }
  el('#writes').innerHTML = `<div class="scroll"><table>
    <thead><tr><th>写号 / 会话</th><th>内容</th><th>级别</th><th>客户端证据</th><th>集群事实</th><th>位点</th><th>操作</th></tr></thead>
    <tbody>${st.writes.map(w => `<tr>
      <td><strong class="mono">${esc(w.writeId)}</strong><br><span class="mono" style="color:var(--dim);font-size:11px">${esc(w.session)}</span></td>
      <td class="mono">${esc(w.key)}=${esc(w.value)}</td>
      <td>${w.critical ? '<span class="badge info">关键写·等多数派</span>' : '<span class="badge purple">非关键·本地可回执</span>'}</td>
      <td>${statusBadge(w.status)}<div style="font-size:11px;color:var(--dim);margin-top:3px">${esc(w.evidence || '尚无证据')}</div>
        <div style="font-size:11px;color:var(--dim)">尝试 ${w.attempts} 次${w.ackedAt != null ? ` · 回执@${w.ackedAt}ms` : ' · 无回执'} · 提交@${w.submittedAt}ms</div></td>
      <td>${w.quorumAcked
        ? '<span class="badge ok">多数派已落盘</span>'
        : (w.localAcked ? '<span class="badge warn">仅主库本地</span>' : '<span class="badge warn">未落盘</span>')}
        ${w.quorumAcked && w.status !== 'COMMITTED' ? '<div style="font-size:11px;color:var(--warn);margin-top:3px">⚠ 数据安全但你没证据，可查明</div>' : ''}</td>
      <td class="mono">${w.index != null ? `${w.term}/${w.index}` : '—'}</td>
      <td class="ops-mini">
        <button class="btn" data-wact="retry" data-id="${esc(w.writeId)}">同号重试</button>
        <button class="btn" data-wact="inquire" data-id="${esc(w.writeId)}">查明结果</button>
        <button class="btn warn" data-wact="conflict" data-id="${esc(w.writeId)}">同号换内容</button>
        <button class="btn" data-wact="read" data-id="${esc(w.writeId)}">读这个键</button>
      </td></tr>`).join('')}
    </tbody></table></div>`;
}

function renderTimeline(st) {
  const order = { danger: 0, warn: 1, ok: 2, info: 3, debug: 4 };
  let evs = st.events;
  if (eventFilter !== 'all') evs = evs.filter(e => e.level === eventFilter);
  el('#timeline').innerHTML = evs.slice(-220).map(e => `
    <div class="ev ${e.level}">
      <span class="t">${e.at}ms</span><span class="tag">${esc(e.tag)}</span><span class="tx">${esc(e.text)}</span>
    </div>`).join('') || '<div class="note">暂无事件</div>';
  el('#timeline').scrollTop = el('#timeline').scrollHeight;
}

function renderLinks(st) {
  el('#links').innerHTML = `<div class="link-grid">${st.links.map(l => {
    const a = st.nodes.find(n => n.id === l.from), b = st.nodes.find(n => n.id === l.to);
    return `<div class="link ${l.blocked ? 'blocked' : ''}">
      <div>${esc(a.name)} → ${esc(b.name)}</div>
      <div class="st">${l.blocked ? '⛔ 已隔离' : `时延 ${l.latency}ms`}${l.dropNext ? ` · 待丢${l.dropNext}` : ''}</div>
      <div class="ops-mini" style="margin-top:4px">
        <button class="btn ${l.blocked ? 'ok' : 'danger'}" data-link="${l.from}|${l.to}" data-lact="${l.blocked ? 'unblock' : 'block'}">${l.blocked ? '恢复' : '隔离'}</button>
        <button class="btn warn" data-link="${l.from}|${l.to}" data-lact="drop">丢下一条</button>
      </div></div>`;
  }).join('')}</div>
  <button class="btn" id="heal-links" style="margin-top:8px">全部链路恢复</button>`;
}

function renderHistory() {
  const list = loadHistory();
  el('#history').innerHTML = list.length ? list.map(t => `
    <div class="history-item" data-replay="${t.id}">
      <div><strong>${esc(t.name)}</strong><div class="meta">${new Date(t.savedAt).toLocaleString('zh-CN', { hour12: false })}
      · ${t.actions.length} 动作 · 无主 ${t.summary.downtime}ms</div>
      <div class="checksum">${t.checksum}</div></div>
      <div class="ops-mini"><button class="btn" data-replay-btn="${t.id}">回看</button>
      <button class="btn danger" data-del="${t.id}">删</button></div>
    </div>`).join('') : '<div class="note">还没有存档试验。做完一轮点击顶部“保存试验”，旧试验可随时一键重放回看。</div>';
}

// ---------------- 事件绑定 ----------------
function bind() {
  el('#node-count').addEventListener('change', e => { config.nodeCount = Number(e.target.value); });
  el('#auto-retry').addEventListener('change', e => { config.autoRetry = e.target.checked; newSim(); });
  el('#btn-reset').addEventListener('click', newSim);
  el('#btn-save').addEventListener('click', saveTrial);

  el('#btn-submit').addEventListener('click', () => {
    const writeId = el('#f-wid').value.trim();
    const key = el('#f-key').value.trim();
    const value = el('#f-val').value.trim();
    if (!writeId || !key || !value) return toast('写号、键、值都要填');
    const critical = el('#f-critical').checked;
    const session = el('#f-session').value.trim() || 's1';
    act(() => sim.submitWrite({ writeId, key, value, critical, session }), `已提交写入 ${writeId}`);
  });
  el('#btn-conflict-demo').addEventListener('click', () => {
    const writeId = el('#f-wid').value.trim();
    if (!writeId) return toast('先在写号框填一个已存在的写号');
    act(() => sim.resubmitDifferent(writeId, el('#f-val').value.trim() + '-X'), `已用同号 ${writeId} 提交不同内容（应被拒绝）`);
  });
  el('#btn-inquire').addEventListener('click', () => {
    const writeId = el('#f-wid').value.trim();
    if (!writeId) return toast('先填要查明的写号');
    act(() => sim.inquire(writeId), `已发起对 ${writeId} 的结果查明`);
  });
  el('#btn-retry').addEventListener('click', () => {
    const writeId = el('#f-wid').value.trim();
    if (!writeId) return toast('先填要重试的写号');
    act(() => sim.retryWrite(writeId), `同号重试 ${writeId}`);
  });
  el('#btn-read').addEventListener('click', () => {
    const key = el('#f-key').value.trim();
    const session = el('#f-session').value.trim() || 's1';
    const nodeId = el('#f-read-node').value || null;
    if (!key) return toast('先填要读取的键');
    act(() => sim.read({ session, key, nodeId }), '读取已发起');
  });
  el('#btn-arm-drop').addEventListener('click', () => act(() => sim.armDropAck(), '已布控：下一条回执将丢失'));
  el('#btn-wait').addEventListener('click', () => {
    const ms = Number(el('#f-wait-ms').value) || 200;
    act(() => sim.wait(ms), `推进 ${ms}ms`);
  });
  el('#btn-flush').addEventListener('click', () => act(() => sim.flush(), '在途消息已全部投递（立即同步）'));
  el('#btn-idle').addEventListener('click', () => act(() => sim.runUntilIdle(), '已推进到系统稳定'));

  // 事件委托：节点、链路、写入台账、历史
  document.addEventListener('click', e => {
    const t = e.target;
    if (t.dataset.act) {
      const map = { kill: () => sim.killNode(t.dataset.id), recover: () => sim.recoverNode(t.dataset.id), elect: () => sim.triggerElection(t.dataset.id) };
      act(map[t.dataset.act]);
    }
    if (t.dataset.wact) {
      const id = t.dataset.id;
      const w = sim.getState().writes.find(x => x.writeId === id);
      const map = {
        retry: () => sim.retryWrite(id),
        inquire: () => sim.inquire(id),
        conflict: () => sim.resubmitDifferent(id, w.value + '-X'),
        read: () => sim.read({ session: w.session, key: w.key })
      };
      act(map[t.dataset.wact]);
    }
    if (t.dataset.lact) {
      const [a, b] = t.dataset.link.split('|');
      const map = {
        block: () => sim.blockLink(a, b), unblock: () => sim.unblockLink(a, b),
        drop: () => sim.dropNextOnLink(a, b)
      };
      act(map[t.dataset.lact]);
    }
    if (t.id === 'heal-links') act(() => sim.healAllLinks(), '全部链路已恢复');
    if (t.dataset.replayBtn) { e.stopPropagation(); replayTrial(t.dataset.replayBtn); }
    if (t.dataset.del) { e.stopPropagation(); deleteTrial(t.dataset.del); }
    if (t.dataset.replay && !t.dataset.replayBtn && !t.dataset.del) replayTrial(t.dataset.replay);
  });

  el('#event-filter').addEventListener('change', e => { eventFilter = e.target.value; renderTimeline(sim.getState()); });

  el('#scenarios').addEventListener('click', e => {
    const btn = e.target.closest('[data-scenario]');
    if (!btn) return;
    const sc = SCENARIOS.find(s => s.key === btn.dataset.scenario);
    act(() => sc.run(), `剧本「${sc.name}」已重放`);
  });

function refreshReadNodeOptions() {
  if (!sim) return;
  const sel = el('#f-read-node');
  const cur = sel.value;
  sel.innerHTML = '<option value="">主库（线性一致读）</option>' +
    sim.getState().nodes.map(n => `<option value="${n.id}" ${n.id === cur ? 'selected' : ''}>${esc(n.name)}（副本读）</option>`).join('');
}
}

function renderScenarios() {
  el('#scenarios').innerHTML = SCENARIOS.map(s => `
    <button class="btn primary" data-scenario="${s.key}" style="text-align:left;margin-bottom:7px">
      <strong>${esc(s.name)}</strong><div style="color:var(--dim);font-size:11.5px;margin-top:3px;font-weight:400">${esc(s.desc)}</div>
    </button>`).join('');
}

// ---------------- 启动 ----------------
document.addEventListener('DOMContentLoaded', () => {
  bind();
  renderScenarios();
  renderHistory();
  if (!restoreCurrent()) newSim();
  else { render(); refreshReadNodeOptions(); toast('已恢复上次未完成的演练现场'); }
});

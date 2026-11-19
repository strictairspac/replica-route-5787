/*
 * 写入一致性演练引擎（Raft 风格的受控模型，非真实集群）
 * 同一工厂 + 同一 seed + 同一动作序列 => 完全确定的重放结果。
 * UMD：浏览器挂 globalThis.WCEngine，Node 下 module.exports，零依赖。
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.WCEngine = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  // ---------- 确定性随机数（仅用于选举超时抖动） ----------
  function mulberry32(seed) {
    let a = seed >>> 0;
    return function () {
      a |= 0; a = (a + 0x6D2B79F5) | 0;
      let t = Math.imul(a ^ (a >>> 15), 1 | a);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  // ---------- 32 位 FNV-1a，用于重放校验 ----------
  function stableStringify(v) {
    if (v === null || typeof v !== 'object') return JSON.stringify(v);
    if (Array.isArray(v)) return '[' + v.map(stableStringify).join(',') + ']';
    const keys = Object.keys(v).sort();
    return '{' + keys.map(k => JSON.stringify(k) + ':' + stableStringify(v[k])).join(',') + '}';
  }
  function fnv1a(str) {
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    return (h >>> 0).toString(16).padStart(8, '0');
  }

  const CLIENT = 'client';
  // 客户端定向消息（回执类），“丢回执”按钮作用于这些类型
  const CLIENT_REPLY_TYPES = new Set(['ClientAck', 'InquiryResult', 'ReadResult']);

  function createSim(config) {
    config = config || {};
    const cfg = {
      nodeCount: config.nodeCount === 5 ? 5 : 3,
      seed: config.seed != null ? config.seed : 20261119,
      linkLatency: config.linkLatency || 30,      // 节点间单程基础时延 ms
      clientLatency: config.clientLatency || 5,  // 客户端<->节点单程时延
      heartbeatMs: config.heartbeatMs || 100,
      electionMin: config.electionMin || 150,
      electionSpan: config.electionSpan || 150,  // 150~300ms
      clientTimeoutMs: config.clientTimeoutMs || 800,
      autoRetry: !!config.autoRetry,
      idleHorizonMs: config.idleHorizonMs || 15000
    };
    const nodeNames = ['主站-n1', '北站-n2', '南站-n3', '东站-n4', '西站-n5'].slice(0, cfg.nodeCount);
    const rng = mulberry32(cfg.seed);

    const sim = {
      now: 0,
      seq: 0,
      cfg,
      nodes: [],
      byId: {},
      links: {},          // links[a][b] = {latency, blocked, dropNext}
      queue: [],          // 虚拟消息与定时器
      events: [],
      actions: [],
      writes: new Map(),  // writeId -> 写入台账
      rng,
      leaderId: null,
      dropNextClientReply: false,
      noLeaderSince: null,
      totalDowntimeMs: 0,
      leaderChanges: 0,
      droppedCount: 0,
      blockedWriteCount: 0
    };

    // ---------- 节点 ----------
    nodeNames.forEach((name, i) => {
      const id = 'n' + (i + 1);
      const node = {
        id, name,
        alive: true,
        role: i === 0 ? 'leader' : 'follower',
        term: i === 0 ? 1 : 0,
        log: [],                 // {index, term, writeId, key, value, critical, session, noop}
        commitIndex: 0,
        appliedIndex: 0,
        kv: {},                  // 已应用状态机
        appliedWrites: {},       // writeId -> {index, key, value}
        votedFor: null,
        leaderId: i === 0 ? id : null,
        nextIndex: {},
        matchIndex: {},
        electionAt: null,
        hbAt: null
      };
      sim.nodes.push(node);
      sim.byId[id] = node;
      sim.links[id] = {};
    });
    for (const a of sim.nodes) {
      for (const b of sim.nodes) {
        if (a.id !== b.id) sim.links[a.id][b.id] = { latency: cfg.linkLatency, blocked: false, dropNext: 0 };
      }
    }

    sim.leaderId = 'n1';
    sim.nodes[0].hbAt = cfg.heartbeatMs;
    _pushTimer(cfg.heartbeatMs, { node: 'n1', ttype: 'heartbeat', token: 'n1:' + cfg.heartbeatMs });
    for (const n of sim.nodes.slice(1)) _resetElectionTimer(n);

    // ---------- 基础工具 ----------
    function ev(level, tag, text) {
      sim.events.push({ at: sim.now, seq: sim.seq, level, tag, text });
      if (sim.events.length > 1200) sim.events.splice(0, sim.events.length - 1200);
    }
    function _push(item) {
      item.seq = sim.seq++;
      sim.queue.push(item);
      sim.queue.sort((a, b) => a.at - b.at || a.seq - b.seq);
    }
    function _pushTimer(at, body) { _push(Object.assign({ kind: 'timer', at }, body)); }
    function quorum() { return Math.floor(sim.nodes.length / 2) + 1; }
    function aliveNodes() { return sim.nodes.filter(n => n.alive); }
    function leader() { return sim.leaderId && sim.byId[sim.leaderId].alive ? sim.byId[sim.leaderId] : null; }
    // 主库必须还能联系到多数派（双向链路未隔离、节点在线），否则无权确认新写入
    function _canReachQuorum(n) {
      let reach = 1;
      for (const f of sim.nodes) {
        if (f.id === n.id || !f.alive) continue;
        if (!sim.links[n.id][f.id].blocked && !sim.links[f.id][n.id].blocked) reach++;
      }
      return reach >= quorum();
    }
    function _availableLeader() {
      const ldr = leader();
      return ldr && _canReachQuorum(ldr) ? ldr : null;
    }
    function lastLogTerm(n) { return n.log.length ? n.log[n.log.length - 1].term : 0; }
    function lastLogIndex(n) { return n.log.length ? n.log[n.log.length - 1].index : 0; }

    function _record(type, args) { if (!_replaying) sim.actions.push({ type, args: JSON.parse(JSON.stringify(args || {})), at: sim.now }); }
    let _replaying = false;

    // ---------- 虚拟网络 ----------
    function _send(from, to, type, payload) {
      const toClient = to === CLIENT;
      const fromClient = from === CLIENT;
      // 下线判定
      if (!toClient && !sim.byId[to].alive) {
        ev('debug', '网络', `→ ${sim.byId[to].name} 已下线，丢弃 ${type}`);
        sim.droppedCount++;
        return;
      }
      if (!fromClient && !sim.byId[from].alive) return;
      // 丢回执按钮
      if (sim.dropNextClientReply && CLIENT_REPLY_TYPES.has(type)) {
        sim.dropNextClientReply = false;
        sim.droppedCount++;
        ev('warn', '丢回执', `下一条客户端回执 ${type} 被主动丢弃（${payload && payload.writeId ? payload.writeId : type}）`);
        return;
      }
      // 链路规则
      if (!toClient && !fromClient) {
        const link = sim.links[from][to];
        if (link.blocked) {
          sim.droppedCount++;
          ev('warn', '链路隔离', `${sim.byId[from].name} → ${sim.byId[to].name} 的 ${type} 被阻断`);
          return;
        }
        if (link.dropNext > 0) {
          link.dropNext--;
          sim.droppedCount++;
          ev('warn', '丢消息', `${sim.byId[from].name} → ${sim.byId[to].name} 的下一条 ${type} 被丢弃`);
          return;
        }
        _push({ kind: 'msg', at: sim.now + link.latency, from, to, type, payload });
      } else {
        _push({ kind: 'msg', at: sim.now + cfg.clientLatency, from, to, type, payload });
      }
    }

    // ---------- 定时器 ----------
    function _resetElectionTimer(n) {
      if (!n.alive || n.role === 'leader') return;
      const at = sim.now + cfg.electionMin + Math.floor(rng() * cfg.electionSpan);
      n.electionAt = at;
      _pushTimer(at, { node: n.id, ttype: 'election', token: n.id + ':' + at });
    }
    function _resetHeartbeat(n) {
      const at = sim.now + cfg.heartbeatMs;
      n.hbAt = at;
      _pushTimer(at, { node: n.id, ttype: 'heartbeat', token: n.id + ':' + at });
    }
    function _scheduleClientTimeout(w) {
      _pushTimer(sim.now + cfg.clientTimeoutMs, {
        ttype: 'clientTimeout', writeId: w.writeId, attempt: w.attempts, token: w.writeId + ':' + w.attempts
      });
    }

    // ---------- 客户端动作 ----------
    function submitWrite(opts) {
      _record('submitWrite', opts);
      const writeId = opts.writeId, session = opts.session || 's1', key = opts.key,
        value = String(opts.value), critical = !!opts.critical;
      const existing = sim.writes.get(writeId);
      if (existing) {
        if (existing.key !== key || existing.value !== value) {
          ev('danger', '幂等护栏', `客户端拒绝：写号 ${writeId} 已绑定 ${existing.key}=${existing.value}，同号不得换内容`);
          return { rejected: true, reason: 'writeId 冲突（客户端侧）' };
        }
        ev('info', '重试', `写号 ${writeId} 同内容重试，按幂等请求重发，不产生新条目`);
        return _dispatchWrite(existing, true);
      }
      const w = {
        writeId, session, key, value, critical,
        status: 'PENDING',          // PENDING / LOCAL_ACKED / COMMITTED / CONFLICT_REJECTED / LOST
        submittedAt: sim.now, ackedAt: null, commitAt: null,
        attempts: 0, quorumAcked: false, localAcked: false,
        index: null, term: null, leaderAtSubmit: sim.leaderId, evidence: ''
      };
      sim.writes.set(writeId, w);
      ev('info', '写入提交', `${critical ? '关键写' : '非关键写'} ${writeId}：${key}=${value}（会话 ${session}）`);
      return _dispatchWrite(w, false);
    }

    function _dispatchWrite(w, isRetry) {
      const ldr0 = leader();
      const ldr = _availableLeader();
      if (!ldr) {
        if (w.critical) {
          sim.blockedWriteCount++;
          if (ldr0) ev('danger', '失去多数派', `关键写 ${w.writeId} 被挡：主库联系不到多数派，无法确认，保持“待确认”`);
          else ev('danger', '无主', `关键写 ${w.writeId} 无处可发：集群无多数派主库，保持“待确认”，等待恢复后按同号重试`);
          return { rejected: false, noLeader: true };
        }
        if (!ldr0) {
          sim.blockedWriteCount++;
          ev('danger', '无主', `非关键写 ${w.writeId} 也无处可发（无任何主库），保持待确认，恢复后同号重试`);
          return { rejected: false, noLeader: true };
        }
        ev('warn', '降级写入', `非关键写 ${w.writeId} 在无多数派时继续：仅可能拿到本地回执，换主有丢失风险`);
      }
      const target = ldr || ldr0;
      w.attempts++;
      if (cfg.autoRetry) _scheduleClientTimeout(w);
      _send(CLIENT, target.id, 'ClientWrite', {
        writeId: w.writeId, session: w.session, key: w.key, value: w.value, critical: w.critical, attempt: w.attempts
      });
      ev('debug', '网络', `写号 ${w.writeId} 发往 ${target.name}（第 ${w.attempts} 次）`);
      return { sent: true };
    }

    function retryWrite(writeId) {
      _record('retryWrite', { writeId });
      const w = sim.writes.get(writeId);
      if (!w) { ev('warn', '重试', `未知写号 ${writeId}`); return; }
      if (w.status === 'COMMITTED') ev('info', '重试', `写号 ${writeId} 已确认，重试只会拿回同一回执，不会重复生效`);
      else ev('info', '重试', `写号 ${w.writeId} 未获证据，按同一写号同内容重试（禁止换新号重写）`);
      _dispatchWrite(w, true);
    }

    // 专门演示“同号换内容必须拒绝”
    function resubmitDifferent(writeId, newValue) {
      _record('resubmitDifferent', { writeId, newValue: String(newValue) });
      const w = sim.writes.get(writeId);
      if (!w) { ev('warn', '冲突演示', `未知写号 ${writeId}`); return; }
      const ldr = leader();
      if (!ldr) { ev('danger', '无主', `无主可验，写号 ${writeId} 的换内容请求被挡回`); return; }
      ev('warn', '冲突演示', `故意用写号 ${writeId} 提交新值 ${w.key}=${newValue}，等待主库判定…`);
      _send(CLIENT, ldr.id, 'ClientWrite', {
        writeId, session: w.session, key: w.key, value: String(newValue), critical: w.critical, attempt: w.attempts + 1, altered: true
      });
    }

    function inquire(writeId) {
      _record('inquire', { writeId });
      const w = sim.writes.get(writeId);
      if (!w) { ev('warn', '查询', `未知写号 ${writeId}`); return; }
      const ldr = _availableLeader();
      if (!ldr) {
        ev('warn', '查询', `写号 ${writeId}：当前无多数派主库，证据暂时查不到；结论维持“待确认”，不得按成功处理，也不得换新号重写`);
        w.evidence = '无主期：证据未到，保持待确认';
        return;
      }
      _send(CLIENT, ldr.id, 'Inquiry', { writeId });
      ev('info', '查询', `就写号 ${writeId} 向 ${ldr.name} 查明结果…`);
    }

    function read(opts) {
      _record('read', opts);
      const session = opts.session || 's1', key = opts.key;
      const target = opts.nodeId ? sim.byId[opts.nodeId] : leader();
      if (!target || !target.alive) { ev('danger', '读取', `读取目标不可用，拒绝不确定读取`); return; }
      _send(CLIENT, target.id, 'Read', { session, key, viaLeader: !opts.nodeId });
      ev('info', '读取', `会话 ${session} 向 ${target.name} 读取 ${key}${opts.nodeId ? '（指定副本读）' : '（主库读）'}`);
    }

    // ---------- 故障与网络动作 ----------
    function killNode(id) {
      _record('killNode', { id });
      const n = sim.byId[id];
      if (!n.alive) return;
      n.alive = false;
      sim.queue = sim.queue.filter(q => !(q.kind === 'timer' && q.node === id) && q.from !== id && q.to !== id);
      ev('danger', '节点下线', `${n.name} 下线，在途消息与其定时器一并清除`);
      if (sim.leaderId === id) {
        sim.leaderId = null;
        sim.noLeaderSince = sim.now;
        ev('warn', '换主', `主库 ${n.name} 失效，开始等待副本选举（不可用计时启动）`);
      }
    }

    function recoverNode(id) {
      _record('recoverNode', { id });
      const n = sim.byId[id];
      if (n.alive) return;
      n.alive = true;
      if (n.role !== 'follower') { n.role = 'follower'; n.leaderId = null; }
      ev('ok', '节点恢复', `${n.name} 恢复在线，以 follower 身份追赶日志`);
      _resetElectionTimer(n);
    }

    function blockLink(a, b) {
      _record('blockLink', { a, b });
      sim.links[a][b].blocked = true;
      ev('warn', '链路隔离', `阻断单向链路 ${sim.byId[a].name} → ${sim.byId[b].name}`);
    }
    function unblockLink(a, b) {
      _record('unblockLink', { a, b });
      sim.links[a][b].blocked = false;
      ev('ok', '链路恢复', `恢复单向链路 ${sim.byId[a].name} → ${sim.byId[b].name}`);
      _pokeLeaderHeartbeat();
    }
    function healAllLinks() {
      _record('healAllLinks', {});
      for (const a of sim.nodes) for (const b of sim.nodes) if (a.id !== b.id) sim.links[a.id][b.id].blocked = false;
      ev('ok', '链路恢复', '全部链路隔离解除，复制继续');
      _pokeLeaderHeartbeat();
    }
    // 链路恢复后让在任主库立即发一次心跳：使分区期的旧候选者在下次选举超时前归队，避免无谓升 term
    function _pokeLeaderHeartbeat() {
      const ldr = leader();
      if (ldr && _canReachQuorum(ldr)) {
        _broadcastAppend(ldr);
        ldr.hbAt = sim.now + cfg.heartbeatMs;
        _pushTimer(ldr.hbAt, { node: ldr.id, ttype: 'heartbeat', token: ldr.id + ':' + ldr.hbAt });
      }
    }
    function setLatency(a, b, ms) {
      _record('setLatency', { a, b, ms });
      sim.links[a][b].latency = Math.max(0, ms | 0);
      ev('info', '链路时延', `${sim.byId[a].name} → ${sim.byId[b].name} 单程时延设为 ${ms}ms`);
    }
    function dropNextOnLink(a, b) {
      _record('dropNextOnLink', { a, b });
      sim.links[a][b].dropNext++;
      ev('warn', '丢消息', `已布控：${sim.byId[a].name} → ${sim.byId[b].name} 下一条消息将被丢弃`);
    }
    function armDropAck() {
      _record('armDropAck', {});
      sim.dropNextClientReply = true;
      ev('warn', '丢回执', '已布控：下一条发往客户端的回执将被丢弃（模拟写后收不到回执）');
    }
    function triggerElection(id) {
      _record('triggerElection', { id });
      const n = sim.byId[id];
      if (!n.alive) { ev('warn', '选举', `${n.name} 不在线，无法参选`); return; }
      ev('info', '选举', `手动触发 ${n.name} 立即参选`);
      n.electionAt = sim.now;
      _pushTimer(sim.now, { node: id, ttype: 'election', token: id + ':manual' });
    }
    function wait(ms) {
      _record('wait', { ms });
      _runUntil(ms | 0, false);
    }

    // ---------- 选主 ----------
    function _startElection(n) {
      if (!n.alive || n.role === 'leader') return;
      // 失联少数派自检：联系不到多数派时参选必败，不自增 term、不制造选举风暴，安静等待链路恢复
      if (!_canReachQuorum(n)) {
        ev('debug', '选举搁置', `${n.name} 当前联系不到多数派，搁置本次选举，等待链路恢复`);
        n.role = 'follower';
        _resetElectionTimer(n);
        return;
      }
      n.role = 'candidate';
      n.term += 1;
      n.votedFor = n.id;
      n.leaderId = null;
      const lastIdx = lastLogIndex(n), lastTerm = lastLogTerm(n);
      ev('info', '选举', `${n.name} 在 term ${n.term} 发起选举（日志末尾 term=${lastTerm} index=${lastIdx}）`);
      let grants = 1;
      for (const v of sim.nodes) {
        if (v.id === n.id || !v.alive) continue;
        _send(n.id, v.id, 'RequestVote', { term: n.term, candidateId: n.id, lastLogIndex: lastIdx, lastLogTerm: lastTerm, grantsRef: n.id });
      }
      // 若只剩自己在线，不可能多数；定时器稍后重试验证
      _resetElectionTimer(n);
      // 内联计票：回复到达后处理
      n._grants = grants;
    }

    function _becomeLeader(n) {
      n.role = 'leader';
      n.leaderId = n.id;
      n.votedFor = null;
      const li = lastLogIndex(n);
      for (const f of sim.nodes) {
        if (f.id === n.id) continue;
        n.nextIndex[f.id] = li + 1;
        n.matchIndex[f.id] = 0;
      }
      sim.leaderId = n.id;
      sim.leaderChanges++;
      if (sim.noLeaderSince != null) {
        const d = sim.now - sim.noLeaderSince;
        sim.totalDowntimeMs += d;
        sim.noLeaderSince = null;
        ev('ok', '换主完成', `${n.name} 当选 term ${n.term} 主库，本次无主不可用 ${d}ms`);
      } else {
        ev('ok', '选主', `${n.name} 当选 term ${n.term} 主库`);
      }
      // Raft 惯例：新主先追加一条 no-op，以便提交旧 term 条目
      _appendEntries(n, [{ index: li + 1, term: n.term, writeId: null, noop: true, key: null, value: null, critical: false, session: null }]);
      _broadcastAppend(n);
      _resetHeartbeat(n);
      _classifyWritesAfterFailover(n);
    }

    function _classifyWritesAfterFailover(ldr) {
      for (const w of sim.writes.values()) {
        if (w.quorumAcked || (w.status !== 'PENDING' && w.status !== 'LOCAL_ACKED')) continue;
        const survived = ldr.log.some(e => e.writeId === w.writeId);
        if (survived) {
          ev('info', '换主核对', `写号 ${w.writeId} 的条目保留在新主日志中，继续走复制，不重复写入`);
        } else if (w.status === 'LOCAL_ACKED') {
          w.status = 'LOST';
          w.evidence = '换主后新主无该条目，仅本地回执的写入确认丢失';
          ev('danger', '写入丢失', `非关键写 ${w.writeId} 只拿到旧主本地回执、未进多数派，换主后确认丢失`);
        } else {
          w.evidence = '证据未到：新主日志无此条目，保持待确认';
          ev('warn', '证据缺失', `关键写 ${w.writeId} 尚无任何确认证据：保持待确认，可同号重试/继续查询，严禁换新号重写`);
        }
      }
    }

    // ---------- 日志复制 ----------
    function _appendEntries(ldr, entries) {
      for (const e of entries) {
        ldr.log.push(e);
        if (e.writeId) {
          const w = sim.writes.get(e.writeId);
          if (w) { w.index = e.index; w.term = e.term; }
        }
      }
    }

    function _broadcastAppend(ldr) {
      for (const f of sim.nodes) {
        if (f.id === ldr.id || !f.alive) continue;
        _sendAppend(ldr, f);
      }
    }

    function _sendAppend(ldr, f) {
      const next = ldr.nextIndex[f.id] || 1;
      const prev = ldr.log[next - 2];
      const entries = ldr.log.slice(next - 1).map(e => Object.assign({}, e));
      _send(ldr.id, f.id, 'AppendEntries', {
        term: ldr.term,
        prevLogIndex: prev ? prev.index : 0,
        prevLogTerm: prev ? prev.term : 0,
        entries,
        leaderCommit: ldr.commitIndex
      });
    }

    function _handleAppendEntries(m) {
      const n = sim.byId[m.to], ldr = sim.byId[m.from];
      if (m.payload.term < n.term) {
        _send(n.id, m.from, 'AppendResult', { term: n.term, success: false, match: lastLogIndex(n), reason: 'term 过期' });
        return;
      }
      if (m.payload.term > n.term) { n.term = m.payload.term; n.votedFor = null; }
      n.role = 'follower';
      n.leaderId = m.from;
      _resetElectionTimer(n);
      const p = m.payload;
      // 日志谱系一致性检查
      if (p.prevLogIndex > 0) {
        const cur = n.log[p.prevLogIndex - 1];
        if (!cur || cur.term !== p.prevLogTerm) {
          _send(n.id, m.from, 'AppendResult', { term: n.term, success: false, match: lastLogIndex(n), reason: '日志谱系不一致' });
          return;
        }
      }
      // 截断冲突后缀
      if (p.entries.length) {
        const base = p.prevLogIndex;
        for (const e of p.entries) {
          const pos = e.index - 1;
          const existing = n.log[pos];
          if (existing && existing.term !== e.term) {
            const removed = n.log.splice(pos).filter(x => x.writeId);
            for (const r of removed) {
              const w = sim.writes.get(r.writeId);
              if (w && !w.quorumAcked) ev('warn', '日志截断', `${n.name} 截断未达多数派的冲突条目（写号 ${r.writeId}，index ${r.index}）`);
            }
          }
          if (!n.log[pos]) n.log.push(e);
        }
      }
      if (p.leaderCommit > n.commitIndex) {
        n.commitIndex = Math.min(p.leaderCommit, lastLogIndex(n));
        _apply(n);
      }
      _send(n.id, m.from, 'AppendResult', { term: n.term, success: true, match: lastLogIndex(n) });
    }

    function _handleAppendResult(m) {
      const ldr = sim.byId[m.to];
      if (!ldr.alive || ldr.role !== 'leader') return;
      const p = m.payload, f = sim.byId[m.from];
      if (p.term > ldr.term) { _stepDown(ldr, p.term); return; }
      if (!p.success) {
        ldr.nextIndex[f.id] = Math.max(1, (ldr.nextIndex[f.id] || 1) - 1);
        ev('debug', '复制回退', `${ldr.name} 将 ${f.name} 的 nextIndex 回退至 ${ldr.nextIndex[f.id]}（${p.reason}）`);
        if (f.alive) _sendAppend(ldr, f);
        return;
      }
      ldr.matchIndex[f.id] = p.match;
      ldr.nextIndex[f.id] = p.match + 1;
      _advanceCommit(ldr);
    }

    function _advanceCommit(ldr) {
      for (let idx = ldr.log.length; idx > ldr.commitIndex; idx--) {
        const e = ldr.log[idx - 1];
        if (e.term !== ldr.term) continue; // Raft：只能直接提交当前 term 条目
        let votes = 1;
        for (const f of sim.nodes) if (f.id !== ldr.id && (ldr.matchIndex[f.id] || 0) >= idx) votes++;
        if (votes >= quorum()) {
          ldr.commitIndex = idx;
          _apply(ldr);
          _broadcastCommit(ldr);
          break;
        }
      }
    }
    function _broadcastCommit(ldr) {
      for (const f of sim.nodes) {
        if (f.id !== ldr.id && f.alive && (f.commitIndex < ldr.commitIndex)) _sendAppend(ldr, f);
      }
    }

    // ---------- 状态机应用（含三道幂等/冲突防线的最后一道） ----------
    function _apply(n) {
      while (n.appliedIndex < n.commitIndex) {
        const e = n.log[n.appliedIndex];
        n.appliedIndex++;
        if (!e.writeId) continue; // no-op
        const prior = n.appliedWrites[e.writeId];
        if (prior) {
          if (prior.key === e.key && prior.value === e.value) { ev('debug', '幂等应用', `${n.name} 跳过重复应用写号 ${e.writeId}`); continue; }
          ev('danger', '冲突拦截', `${n.name} 状态机拒绝同号换内容：写号 ${e.writeId}`);
          continue;
        }
        n.appliedWrites[e.writeId] = { index: e.index, key: e.key, value: e.value };
        n.kv[e.key] = e.value;
        const w = sim.writes.get(e.writeId);
        if (w && n.role === 'leader') _onLeaderCommit(w, e, n);
      }
    }

    // 集群事实（quorumAcked）与客户端证据（status）分离：
    // 回执丢失时，集群已提交但客户端台账仍停在“待确认”，直到查明或回执补达。
    function _onLeaderCommit(w, e, ldr) {
      if (w.quorumAcked) return;
      w.quorumAcked = true;
      w.commitAt = sim.now;
      w.evidence = `集群已多数派确认 term=${e.term} index=${e.index}，客户端回执在途`;
      if (w.status === 'LOCAL_ACKED') {
        ev('ok', '耐久性升级', `非关键写 ${w.writeId} 已进多数派（index=${e.index}），向客户端补发确定确认`);
      } else {
        ev('ok', '多数派确认', `写号 ${w.writeId} 达成 ${quorum()}/${sim.nodes.length} 多数派，index=${e.index}，确认回执发往客户端`);
      }
      // 客户端状态不在这里升级：只有 ClientAck 实际送达（_handleClientReply）才转 COMMITTED
      _send(ldr.id, CLIENT, 'ClientAck', { writeId: w.writeId, kind: 'COMMITTED', index: e.index, term: e.term });
    }

    // ---------- 客户端写入在主库的处理 ----------
    function _handleClientWrite(m) {
      const ldr = sim.byId[m.to], p = m.payload;
      if (!ldr.alive || ldr.role !== 'leader') {
        _send(ldr.id, CLIENT, 'ClientAck', { writeId: p.writeId, kind: 'NOT_LEADER' });
        return;
      }
      if (!_canReachQuorum(ldr)) {
        if (p.critical) {
          _send(ldr.id, CLIENT, 'ClientAck', { writeId: p.writeId, kind: 'NO_QUORUM' });
          return;
        }
        ev('warn', '降级写入', `${ldr.name} 在无多数派下接受非关键写 ${p.writeId}：仅本地落盘`);
      }
      // 第一道：日志与状态机查重
      const inLog = ldr.log.find(e => e.writeId === p.writeId);
      const applied = ldr.appliedWrites[p.writeId];
      if (inLog || applied) {
        const old = inLog || { key: applied.key, value: applied.value, index: applied.index };
        if (old.key !== p.key || old.value !== p.value) {
          const w = sim.writes.get(p.writeId);
          if (w) { w.status = 'CONFLICT_REJECTED'; w.evidence = '主库拒绝同号换内容'; }
          ev('danger', '冲突拒绝', `主库拒绝写号 ${p.writeId} 换内容（原值 ${old.key}=${old.value}），写号一次绑定内容`);
          _send(ldr.id, CLIENT, 'ClientAck', { writeId: p.writeId, kind: 'CONFLICT' });
          return;
        }
        ev('ok', '幂等命中', `写号 ${p.writeId} 的重复请求命中已有条目 index=${old.index}，不重复追加`);
        if (ldr.commitIndex >= old.index) {
          _send(ldr.id, CLIENT, 'ClientAck', { writeId: p.writeId, kind: 'COMMITTED', index: old.index, term: inLog ? inLog.term : ldr.term });
        } else {
          _broadcastAppend(ldr);
          _send(ldr.id, CLIENT, 'ClientAck', { writeId: p.writeId, kind: 'REPLICATING', index: old.index });
        }
        return;
      }
      const idx = lastLogIndex(ldr) + 1;
      const entry = { index: idx, term: ldr.term, writeId: p.writeId, key: p.key, value: p.value, critical: p.critical, session: p.session, noop: false };
      _appendEntries(ldr, [entry]);
      const w = sim.writes.get(p.writeId);
      ev('info', '日志追加', `${ldr.name} 追加 index=${idx}（写号 ${p.writeId}）并向副本复制`);
      _broadcastAppend(ldr);
      if (!p.critical) {
        // 非关键写：本地落盘即给回执（代价：换主可能丢）
        if (w) { w.status = 'LOCAL_ACKED'; w.localAcked = true; w.ackedAt = sim.now; w.evidence = '仅主库本地回执，未达多数派'; }
        ev('warn', '本地回执', `非关键写 ${p.writeId} 仅主库本地确认即回执（副本未追齐也可继续，但不经久耐摔）`);
        _send(ldr.id, CLIENT, 'ClientAck', { writeId: p.writeId, kind: 'LOCAL', index: idx });
      } else {
        ev('info', '等待回执', `关键写 ${p.writeId} 必须等够 ${quorum()} 票多数派，客户端进入等待…`);
      }
    }

    // ---------- 查询与读取 ----------
    function _handleInquiry(m) {
      const ldr = sim.byId[m.to], writeId = m.payload.writeId;
      const applied = ldr.appliedWrites[writeId];
      const inLog = ldr.log.find(e => e.writeId === writeId);
      let result;
      if (applied) {
        result = { writeId, verdict: 'COMMITTED', index: applied.index, key: applied.key, value: applied.value, evidence: '状态机已应用' };
      } else if (inLog) {
        let acks = 1;
        for (const f of sim.nodes) if (f.id !== ldr.id && (ldr.matchIndex[f.id] || 0) >= inLog.index) acks++;
        result = { writeId, verdict: 'REPLICATING', index: inLog.index, acks, quorum: quorum(), evidence: `复制中 ${acks}/${quorum()}` };
      } else {
        result = { writeId, verdict: 'NOT_FOUND', evidence: '本主日志与状态机均无该写号' };
      }
      _send(ldr.id, CLIENT, 'InquiryResult', result);
    }

    function _handleRead(m) {
      const n = sim.byId[m.to], p = m.payload;
      const ldr = leader();
      const stale = ldr && n.role === 'follower' && n.appliedIndex < ldr.commitIndex;
      // 同会话读己之写：已确认写号的 index 是否超过该节点 appliedIndex
      const missing = [];
      for (const w of sim.writes.values()) {
        if (w.session === p.session && w.quorumAcked && w.index > n.appliedIndex) missing.push(w.writeId);
      }
      _send(n.id, CLIENT, 'ReadResult', {
        session: p.session, key: p.key, value: Object.prototype.hasOwnProperty.call(n.kv, p.key) ? n.kv[p.key] : null,
        nodeId: n.id, appliedIndex: n.appliedIndex,
        leaderCommit: ldr ? ldr.commitIndex : null,
        stale, missingDurableWrites: missing, viaLeader: p.viaLeader
      });
    }

    function _handleClientReply(m) {
      const p = m.payload;
      const w = sim.writes.get(p.writeId);
      if (m.type === 'ClientAck') {
        if (p.kind === 'CONFLICT') {
          if (w) w.status = 'CONFLICT_REJECTED';
          ev('danger', '冲突拒绝', `客户端收到拒绝：写号 ${p.writeId} 同号换内容被驳回，原写入不受影响`);
        } else if (p.kind === 'NOT_LEADER') {
          ev('warn', '重试提示', `写号 ${p.writeId} 的接收方已不是主库，请向新主同号重试`);
        } else if (p.kind === 'NO_QUORUM') {
          ev('danger', '失去多数派', `写号 ${p.writeId} 被主库挡回：当前联系不到多数派，无法确认，保持待确认`);
        } else if (p.kind === 'COMMITTED') {
          if (w && w.status !== 'COMMITTED') {
            w.status = 'COMMITTED'; w.quorumAcked = true; w.ackedAt = sim.now;
            w.evidence = `客户端已收到多数派确认回执 index=${p.index}`;
            ev('ok', '收到回执', `写号 ${p.writeId} 收到多数派确认回执，等待结束（index=${p.index}）`);
          } else if (w) {
            ev('ok', '幂等回执', `写号 ${p.writeId} 已确认过，回执重复到达不产生副作用`);
          }
        } else if (p.kind === 'LOCAL') {
          ev('info', '收到回执', `写号 ${p.writeId} 收到本地回执（非关键写，可继续读己之写，但换主有丢失风险）`);
        } else if (p.kind === 'REPLICATING') {
          ev('info', '查明中', `写号 ${p.writeId} 仍在复制，主库未重复追加`);
        }
      } else if (m.type === 'InquiryResult') {
        if (!w) return;
        w.evidence = p.evidence;
        if (p.verdict === 'COMMITTED') {
          const before = w.status;
          w.status = 'COMMITTED'; w.quorumAcked = true;
          if (w.ackedAt == null) w.ackedAt = sim.now;
          ev('ok', '查明结果', `写号 ${p.writeId} 查明为【已确认】index=${p.index}，值=${p.value}${before === 'PENDING' ? '，待确认注销' : ''}`);
        } else if (p.verdict === 'REPLICATING') {
          ev('info', '查明结果', `写号 ${p.writeId} 查明为【复制中】${p.acks}/${p.quorum} 票，继续等待，勿重复写`);
        } else {
          if (w.status === 'LOCAL_ACKED') {
            w.status = 'LOST';
            ev('danger', '查明结果', `写号 ${p.writeId} 查明为【查无此写】：本地回执写入确认丢失`);
          } else {
            ev('warn', '查明结果', `写号 ${p.writeId} 查明为【证据未到】：保持待确认，只能同号重试或继续查询，严禁换新号重写`);
          }
        }
      } else if (m.type === 'ReadResult') {
        const n = sim.byId[p.nodeId];
        let tail = '';
        if (p.value === null) tail = '，键不存在（null）';
        if (p.stale) tail += `；副本滞后：applied=${p.appliedIndex} 而主库已提交=${p.leaderCommit}`;
        if (p.missingDurableWrites.length) tail += `；读己之写不满足：本会话已确认写号 ${p.missingDurableWrites.join('、')} 尚未在此节点应用`;
        else if (p.viaLeader) {
          const pend = [...sim.writes.values()].filter(x => x.session === p.session && (x.status === 'PENDING' || x.status === 'LOCAL_ACKED'));
          if (pend.length) tail += `；注意本会话仍有待确认写入 ${pend.map(x => x.writeId).join('、')}，本次读取不含其保证`;
        }
        ev(p.missingDurableWrites.length ? 'warn' : 'info', '读取结果',
          `会话 ${p.session} 在 ${n.name} 读到 ${p.key}=${p.value === null ? '∅' : p.value}${tail}`);
      }
    }

    function _stepDown(n, newTerm) {
      n.term = newTerm;
      n.role = 'follower';
      n.leaderId = null;
      n.votedFor = null;
      ev('warn', '退位', `${n.name} 发现更高 term ${newTerm}，自行退位为 follower`);
      _resetElectionTimer(n);
    }

    // ---------- 主循环 ----------
    function step() {
      const item = sim.queue.shift();
      if (!item) return null;
      sim.now = item.at;
      if (item.kind === 'timer') {
        if (item.ttype === 'election') {
          const n = sim.byId[item.node];
          if (n && n.alive && n.role !== 'leader' && n.electionAt === item.at) _startElection(n);
        } else if (item.ttype === 'heartbeat') {
          const n = sim.byId[item.node];
          if (n && n.alive && n.role === 'leader' && n.hbAt === item.at) {
            // Raft 主库自检：心跳已无法送达多数派（分区/宕机），主动退位，避免脑裂自认主库
            if (!_canReachQuorum(n)) {
              ev('warn', '主库自检', `${n.name} 心跳已联系不到多数派，主动退位为 follower，等待新主`);
              n.role = 'follower';
              n.leaderId = null;
              if (sim.leaderId === n.id) { sim.leaderId = null; sim.noLeaderSince = sim.now; }
              // 给完整选举超时窗口：分区恢复后新主心跳(heartbeatMs)应先于本定时器到达
              _resetElectionTimer(n);
            } else {
              _broadcastAppend(n);
              _resetHeartbeat(n);
            }
          }
        } else if (item.ttype === 'clientTimeout') {
          const w = sim.writes.get(item.writeId);
          if (w && w.status === 'PENDING' && cfg.autoRetry && item.attempt === w.attempts) {
            ev('warn', '超时重试', `写号 ${w.writeId} 等待 ${cfg.clientTimeoutMs}ms 无回执，自动同号重试`);
            _dispatchWrite(w, true);
          }
        }
        return item;
      }
      // 消息到达
      if (item.to !== CLIENT && !sim.byId[item.to].alive) {
        ev('debug', '网络', `到达 ${sim.byId[item.to].name} 的 ${item.type} 因节点下线被丢弃`);
        sim.droppedCount++;
        return item;
      }
      switch (item.type) {
        case 'ClientWrite': _handleClientWrite(item); break;
        case 'AppendEntries': _handleAppendEntries(item); break;
        case 'AppendResult': _handleAppendResult(item); break;
        case 'RequestVote': _handleRequestVote(item); break;
        case 'VoteResult': _handleVoteResult(item); break;
        case 'Inquiry': _handleInquiry(item); break;
        case 'Read': _handleRead(item); break;
        default:
          if (CLIENT_REPLY_TYPES.has(item.type)) _handleClientReply(item);
      }
      return item;
    }

    function _handleRequestVote(m) {
      const n = sim.byId[m.to], p = m.payload;
      if (p.term < n.term) {
        _send(n.id, m.from, 'VoteResult', { term: n.term, candidateId: p.candidateId, granted: false, reason: 'term 落后' });
        return;
      }
      if (p.term > n.term) { n.term = p.term; n.role = 'follower'; n.votedFor = null; n.leaderId = null; }
      const upToDate = p.lastLogTerm > lastLogTerm(n) ||
        (p.lastLogTerm === lastLogTerm(n) && p.lastLogIndex >= lastLogIndex(n));
      const canVote = (n.votedFor == null || n.votedFor === p.candidateId) && upToDate;
      const reason = !upToDate ? '候选日志落后，未追齐已确认数据' : '本 term 已投他人';
      if (canVote) {
        n.votedFor = p.candidateId;
        _resetElectionTimer(n);
        ev('ok', '投票', `${n.name} 投票给 ${sim.byId[p.candidateId].name}（term ${p.term}，日志够新）`);
      } else {
        ev('warn', '拒票', `${n.name} 拒绝投票给 ${sim.byId[p.candidateId].name}：${reason}`);
      }
      _send(n.id, m.from, 'VoteResult', { term: n.term, candidateId: p.candidateId, granted: canVote, reason: canVote ? null : reason });
    }

    function _handleVoteResult(m) {
      const c = sim.byId[m.to], p = m.payload;
      if (!c.alive || c.role !== 'candidate' || p.term !== c.term) return;
      if (p.term > c.term) { _stepDown(c, p.term); return; }
      if (!p.granted) return;
      c._grants = (c._grants || 1) + 1;
      if (c._grants >= quorum() && c.role === 'candidate') _becomeLeader(c);
    }

    function _hasUnresolved() {
      for (const w of sim.writes.values()) if (w.status === 'PENDING' || w.status === 'LOCAL_ACKED') return true;
      return false;
    }
    function _hasCandidate() { return sim.nodes.some(n => n.alive && n.role === 'candidate'); }

    function _runUntil(ms, stopWhenSettled) {
      const horizon = sim.now + ms;
      let guard = 0;
      while (sim.queue.length && guard++ < 100000) {
        const next = sim.queue[0];
        if (next.at > horizon) { sim.now = horizon; break; }
        if (stopWhenSettled) {
          const onlyTimers = sim.queue.every(q => q.kind === 'timer');
          // 收束需同时满足：在途消息排空、无待确认写入、主库掌握多数派、没有候选者仍在竞选
          if (onlyTimers && !_hasUnresolved() && !_hasCandidate() && _availableLeader()) break;
        }
        step();
      }
      if (sim.queue.length && sim.now < horizon && !stopWhenSettled) sim.now = horizon;
    }

    function runUntilIdle() { _record('idle', {}); _runUntil(cfg.idleHorizonMs, true); }
    function flush() {
      // “立即同步”：只把当前在途消息全部投递完，不推进选举/心跳等定时器
      _record('flush', {});
      let guard = 0;
      while (sim.queue.some(q => q.kind === 'msg') && guard++ < 100000) step();
    }

    // ---------- 状态快照 / 校验 ----------
    function getState() {
      return {
        now: sim.now,
        leaderId: sim.leaderId,
        quorum: quorum(),
        noLeaderSince: sim.noLeaderSince,
        totalDowntimeMs: sim.totalDowntimeMs,
        leaderChanges: sim.leaderChanges,
        droppedCount: sim.droppedCount,
        blockedWriteCount: sim.blockedWriteCount,
        dropArmed: sim.dropNextClientReply,
        autoRetry: cfg.autoRetry,
        nodes: sim.nodes.map(n => ({
          id: n.id, name: n.name, alive: n.alive, role: n.role, term: n.term,
          logLen: n.log.length,
          log: n.log.map(e => ({ index: e.index, term: e.term, writeId: e.writeId, noop: !!e.noop })),
          commitIndex: n.commitIndex, appliedIndex: n.appliedIndex,
          kv: Object.assign({}, n.kv), votedFor: n.votedFor,
          match: n.role === 'leader' ? Object.assign({}, n.matchIndex) : null
        })),
        links: sim.nodes.map(a => sim.nodes.filter(b => a.id !== b.id).map(b => ({
          from: a.id, to: b.id, latency: sim.links[a.id][b.id].latency,
          blocked: sim.links[a.id][b.id].blocked, dropNext: sim.links[a.id][b.id].dropNext
        }))).flat(),
        writes: [...sim.writes.values()].map(w => ({ ...w })),
        events: sim.events.slice(-300),
        queueLen: sim.queue.length,
        actions: sim.actions.length
      };
    }

    function checksum() {
      const snap = getState();
      // 仅取决定一致性结果的稳态部分，不含事件措辞
      const canon = {
        now: snap.now,
        nodes: snap.nodes.map(n => ({ id: n.id, role: n.role, term: n.term, log: n.log, c: n.commitIndex, a: n.appliedIndex, kv: n.kv })),
        writes: snap.writes.map(w => ({ id: w.writeId, st: w.status, idx: w.index, q: w.quorumAcked }))
      };
      return fnv1a(stableStringify(canon));
    }

    // 各动作的位置参数顺序，重放时按此从 args 对象展开
    const ACTION_ARGS = {
      submitWrite: ['writeId', 'session', 'key', 'value', 'critical'],
      retryWrite: ['writeId'], resubmitDifferent: ['writeId', 'newValue'],
      inquire: ['writeId'],
      read: ['session', 'key', 'nodeId', 'viaLeader'],
      killNode: ['id'], recoverNode: ['id'],
      blockLink: ['a', 'b'], unblockLink: ['a', 'b'], healAllLinks: [],
      setLatency: ['a', 'b', 'ms'], dropNextOnLink: ['a', 'b'],
      armDropAck: [], triggerElection: ['id'], wait: ['ms'], flush: [], idle: []
    };

    function applyAction(action) {
      // 记录名 idle 对应 runUntilIdle
      const type = action.type === 'idle' ? 'runUntilIdle' : action.type;
      const fn = actions[type];
      if (!fn) throw new Error('未知动作 ' + action.type);
      const names = ACTION_ARGS[action.type];
      if (!names) throw new Error('动作缺少重放参数声明：' + action.type);
      let args;
      if (action.type === 'submitWrite' || action.type === 'read') {
        args = [action.args]; // 这两个动作接收单个 opts 对象，记录的就是其字段
      } else {
        args = names.map(k => action.args[k]);
      }
      _replaying = true;
      try { fn(...args); } finally { _replaying = false; }
    }

    const actions = {
      submitWrite, retryWrite, resubmitDifferent, inquire, read,
      killNode, recoverNode, blockLink, unblockLink, healAllLinks,
      setLatency, dropNextOnLink, armDropAck, triggerElection, wait,
      flush, runUntilIdle
    };

    return Object.assign(sim, {
      quorum,
      submitWrite, retryWrite, resubmitDifferent, inquire, read,
      killNode, recoverNode, blockLink, unblockLink, healAllLinks,
      setLatency, dropNextOnLink, armDropAck, triggerElection, wait,
      step, runUntilIdle, flush, applyAction, getState, checksum
    });
  }

  // 写入状态中文展示元数据
  const WRITE_STATUS = {
    PENDING: { label: '待确认', cls: 'warn' },
    LOCAL_ACKED: { label: '本地已收·未确认', cls: 'warn' },
    COMMITTED: { label: '多数派已确认', cls: 'ok' },
    CONFLICT_REJECTED: { label: '冲突拒绝', cls: 'danger' },
    LOST: { label: '确认丢失', cls: 'danger' }
  };

  return { createSim, WRITE_STATUS, stableStringify, fnv1a };
});

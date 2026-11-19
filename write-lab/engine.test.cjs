const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createSim } = require('./engine.js');

const W = (id, critical = true, value = 'v1', session = 's1') =>
  ({ writeId: id, key: 'k1', value, critical, session });

function settled(sim) {
  sim.runUntilIdle();
  return sim.getState();
}
function findWrite(st, id) { return st.writes.find(w => w.writeId === id); }
function leaderNode(st) { return st.nodes.find(n => n.id === st.leaderId); }

// 1. 关键写：等多数派回执，同号重试幂等不重复
test('关键写达成多数派后确认；同号同内容重试不产生重复条目', () => {
  const sim = createSim({});
  sim.submitWrite(W('w1'));
  let st = settled(sim);
  const w1 = findWrite(st, 'w1');
  assert.equal(w1.status, 'COMMITTED');
  assert.ok(w1.ackedAt >= w1.submittedAt);
  assert.ok(w1.evidence.includes('多数派'));
  const logLen = leaderNode(st).logLen;

  sim.retryWrite('w1');
  st = settled(sim);
  assert.equal(findWrite(st, 'w1').status, 'COMMITTED');
  assert.equal(leaderNode(st).logLen, logLen, '重试不得追加新日志条目');
});

// 2. 同号换内容：主库拒绝，原数据不变
test('同号换内容在主库侧被冲突拒绝，原值保留', () => {
  const sim = createSim({});
  sim.submitWrite(W('w2'));
  settled(sim);
  sim.resubmitDifferent('w2', 'HACKED');
  const st = settled(sim);
  assert.equal(findWrite(st, 'w2').status, 'CONFLICT_REJECTED');
  assert.equal(leaderNode(st).kv.k1, 'v1');
});

// 3. 丢回执 + 查询：已确认数据可查明，且不重复生效
test('回执丢失后：集群已提交但客户端保持待确认，查明后才补回证据', () => {
  const sim = createSim({});
  sim.submitWrite(W('w3'));
  sim.armDropAck();
  sim.flush();
  const st2 = sim.getState();
  const w3mid = findWrite(st2, 'w3');
  assert.equal(w3mid.quorumAcked, true, '集群侧必须已多数派落盘');
  assert.equal(w3mid.status, 'PENDING', '回执没到，客户端证据必须停在待确认');
  assert.equal(leaderNode(st2).kv.k1, 'v1');
  const lenBefore = leaderNode(st2).logLen;

  sim.inquire('w3');
  sim.flush();
  const st3 = sim.getState();
  assert.equal(findWrite(st3, 'w3').status, 'COMMITTED', '查明补回客户端证据');
  assert.equal(leaderNode(st3).logLen, lenBefore, '查明结果不重复写入');
});

// 4. 换主：已确认写入不丢，新主含相同状态机数据
test('多数派确认的写入在主库下线、换主后不丢失', () => {
  const sim = createSim({});
  sim.submitWrite(W('w4'));
  settled(sim);
  const committedAt = findWrite(sim.getState(), 'w4').index;

  sim.killNode('n1');
  const st = settled(sim);
  assert.ok(st.leaderId && st.leaderId !== 'n1');
  const nl = leaderNode(st);
  assert.equal(nl.kv.k1, 'v1', '新主状态机包含已确认数据');
  assert.ok(nl.appliedIndex >= committedAt);
  assert.equal(findWrite(st, 'w4').status, 'COMMITTED');
  assert.ok(st.totalDowntimeMs > 0, '换主存在可度量的不可用时间');
});

// 5. 非关键写（本地回执）在换主时可能丢
test('非关键写仅本地回执时换主可确认丢失；关键写存活', () => {
  const sim = createSim({});
  sim.blockLink('n1', 'n2');
  sim.blockLink('n1', 'n3');
  sim.submitWrite(W('be2', false, 'loose2'));
  sim.flush();
  const before = sim.getState();
  assert.equal(findWrite(before, 'be2').status, 'LOCAL_ACKED');

  sim.killNode('n1');
  const st = settled(sim);
  assert.ok(st.leaderId !== 'n1');
  assert.equal(findWrite(st, 'be2').status, 'LOST', '未进多数派的本地回执写入确认丢失');
  assert.equal(leaderNode(st).kv.k1, undefined, '新主没有 loose2');
});

// 6. 无主期间关键写保持待确认，恢复后同号重试可成功
test('无主期间关键写保持待确认，恢复后同号重试可成功', () => {
  const sim = createSim({});
  sim.killNode('n1');
  sim.killNode('n2');
  const r = sim.submitWrite(W('w6'));
  assert.equal(r.noLeader, true);
  let st = sim.getState();
  assert.equal(findWrite(st, 'w6').status, 'PENDING');
  assert.equal(st.blockedWriteCount, 1);

  sim.recoverNode('n2');
  st = settled(sim);
  assert.ok(st.leaderId, '恢复多数派后重新选出主库');
  sim.retryWrite('w6');
  st = settled(sim);
  assert.equal(findWrite(st, 'w6').status, 'COMMITTED');
});

// 7. 日志落后的副本不能当选
test('日志落后节点参选被拒票，追齐后才能当选', () => {
  const sim = createSim({});
  sim.submitWrite(W('w7'));
  settled(sim);
  sim.blockLink('n1', 'n3');
  sim.blockLink('n3', 'n1');
  sim.submitWrite(W('w7b'));
  settled(sim);
  sim.killNode('n1');
  sim.triggerElection('n3');
  let st = settled(sim);
  assert.equal(st.leaderId, 'n2', '只有日志够新的 n2 能当选');
  assert.equal(leaderNode(st).kv.k1, 'v1');
});

// 8. 同会话读己之写
test('同会话读己之写：滞后副本不满足，主库满足', () => {
  const sim = createSim({});
  sim.submitWrite(W('w8', true, 'v8'));
  settled(sim);
  sim.blockLink('n1', 'n3');
  sim.submitWrite({ writeId: 'w9', key: 'k2', value: 'v9', critical: true, session: 's1' });
  settled(sim);

  sim.read({ session: 's1', key: 'k2', nodeId: 'n3' });
  settled(sim);
  const evStale = sim.getState().events.find(e => e.text.includes('读己之写不满足'));
  assert.ok(evStale, '滞后副本读必须提示本会话已确认写入未应用');

  sim.read({ session: 's1', key: 'k2' });
  settled(sim);
  const evOk = sim.getState().events.find(e => e.text.includes('读到 k2=v9') && e.text.includes('主站'));
  assert.ok(evOk, '主库读满足读己之写');
});

// 9. 确定性重放
test('同一 seed 与动作序列重放得到相同校验和', () => {
  const script = () => {
    const s = createSim({ seed: 42 });
    s.submitWrite(W('d1'));
    s.armDropAck();
    s.flush();
    s.killNode('n1');
    s.runUntilIdle();
    s.recoverNode('n1');
    s.inquire('d1');
    s.runUntilIdle();
    return s.checksum();
  };
  const a = script();
  const b = script();
  assert.equal(a, b);
  assert.equal(a.length, 8);
});

// 10. 五节点容忍两故障
test('5 节点：两副本故障时多数派(3)仍可确认写入', () => {
  const sim = createSim({ nodeCount: 5 });
  sim.killNode('n4');
  sim.killNode('n5');
  sim.submitWrite(W('w10'));
  const st = settled(sim);
  assert.equal(st.quorum, 3);
  assert.equal(findWrite(st, 'w10').status, 'COMMITTED');
  sim.killNode('n2');
  const r = sim.submitWrite(W('w11'));
  assert.equal(r.noLeader, true);
});

// 11. 自动重试开关：回执超时后按同号自动重发，仍只生效一次
test('autoRetry：超时后自动同号重试，最终确认且无重复条目', () => {
  const sim = createSim({ autoRetry: true, clientTimeoutMs: 120 });
  sim.armDropAck(); // 第一次确认回执被丢
  sim.submitWrite(W('w12'));
  sim.runUntilIdle();
  const st = sim.getState();
  const w = findWrite(st, 'w12');
  assert.equal(w.status, 'COMMITTED');
  assert.ok(w.attempts >= 2, '应发生过自动重试');
  for (const n of st.nodes) {
    const hits = n.log.filter(e => e.writeId === 'w12').length;
    assert.ok(hits <= 1, `${n.id} 上同号条目不得超过 1 条`);
  }
});

// 12. 客户端侧同号换内容直接拦截（不发请求）
test('客户端幂等护栏：同号不同内容在提交处即被拒绝', () => {
  const sim = createSim({});
  sim.submitWrite(W('w13', true, 'orig'));
  sim.flush();
  const r = sim.submitWrite({ writeId: 'w13', key: 'k1', value: 'changed', critical: true, session: 's1' });
  assert.equal(r.rejected, true);
  const st = sim.getState();
  assert.equal(findWrite(st, 'w13').value, 'orig');
  assert.equal(leaderNode(st).kv.k1, 'orig');
});

// 13. 跨实例重放：动作序列序列化后在全新实例逐动作回放，校验和/状态/时钟完全一致（历史回看的基础）
test('动作日志跨实例重放：校验和、写入状态、虚拟时钟逐项一致', () => {
  function build() {
    const s = createSim({ nodeCount: 3, seed: 99 });
    s.submitWrite({ writeId: 'h1', key: 'a', value: '1', critical: true, session: 'z' });
    s.armDropAck(); s.flush();
    s.inquire('h1'); s.flush();
    s.blockLink('n1', 'n3');
    s.submitWrite({ writeId: 'h2', key: 'b', value: '2', critical: false, session: 'z' }); s.flush();
    s.killNode('n1'); s.runUntilIdle();
    s.recoverNode('n1'); s.healAllLinks(); s.runUntilIdle();
    s.resubmitDifferent('h1', 'X'); s.flush();
    s.read({ session: 'z', key: 'a' }); s.flush();
    return s;
  }
  const original = build();
  const actions = JSON.parse(JSON.stringify(original.actions));
  const replayed = createSim({ nodeCount: 3, seed: 99 });
  for (const a of actions) replayed.applyAction(a);
  assert.equal(replayed.checksum(), original.checksum());
  assert.equal(replayed.getState().now, original.getState().now);
  const wo = original.getState().writes.map(w => [w.writeId, w.status, w.index]);
  const wr = replayed.getState().writes.map(w => [w.writeId, w.status, w.index]);
  assert.deepEqual(wr, wo);
});

// 14. 脑裂：旧主被隔离后多数派侧重新选主，关键写不会在少数派旧主上确认
test('脑裂分区：多数派侧选出新主，旧主停在旧值，新写只在新主确认', () => {
  const sim = createSim({ seed: 3 });
  sim.submitWrite({ writeId: 'sb1', key: 'k', value: '1', critical: true, session: 'x' });
  sim.flush();
  sim.blockLink('n1', 'n2'); sim.blockLink('n2', 'n1');
  sim.blockLink('n1', 'n3'); sim.blockLink('n3', 'n1');
  sim.runUntilIdle();
  const st1 = sim.getState();
  assert.ok(['n2', 'n3'].includes(st1.leaderId), '多数派一侧必须产生新主');
  const oldN1 = st1.nodes.find(n => n.id === 'n1');
  assert.equal(oldN1.kv.k, '1', '旧主状态机停在分区前的值');

  sim.submitWrite({ writeId: 'sb2', key: 'k', value: '2', critical: true, session: 'x' });
  sim.flush();
  const st2 = sim.getState();
  assert.equal(findWrite(st2, 'sb2').status, 'COMMITTED');
  assert.equal(st2.nodes.find(n => n.id === 'n1').kv.k, '1', '旧主拿不到新确认');
  assert.equal(leaderNode(st2).kv.k, '2', '新主推进到新值');

  // 分区恢复后 term 收敛，旧主追平日志
  sim.healAllLinks();
  sim.runUntilIdle();
  const st3 = sim.getState();
  assert.equal(new Set(st3.nodes.map(n => n.term)).size, 1, 'term 重新一致');
  assert.equal(st3.nodes.find(n => n.id === 'n1').kv.k, '2', '旧主恢复后追平');
});

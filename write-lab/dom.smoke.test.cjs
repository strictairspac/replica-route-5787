const { test } = require('node:test');
const assert = require('node:assert/strict');
const { JSDOM } = require('jsdom');
const fs = require('node:fs');
const path = require('node:path');

const root = __dirname;
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');

function boot() {
  const dom = new JSDOM(html, {
    url: 'http://localhost/write-lab/',
    runScripts: 'outside-only',
    pretendToBeVisual: true
  });
  const { window } = dom;
  // 先注入引擎 UMD（它会挂 window.WCEngine）
  window.eval(fs.readFileSync(path.join(root, 'engine.js'), 'utf8'));
  // app.js 是 ES module，jsdom 对 module 支持有限：改写为普通脚本执行
  let app = fs.readFileSync(path.join(root, 'app.js'), 'utf8');
  window.eval(app);
  return dom;
}

function $ (dom, sel) { return dom.window.document.querySelector(sel); }
function $$(dom, sel) { return [...dom.window.document.querySelectorAll(sel)]; }
function click(el) { el.dispatchEvent(new dom.window.Event('click', { bubbles: true })); }
let dom;

test('页面启动：渲染 3 节点、空台账、预置剧本 6 个', () => {
  dom = boot();
  dom.window.document.dispatchEvent(new dom.window.Event('DOMContentLoaded'));
  assert.equal($$(dom, '.node').length, 3);
  assert.equal($$(dom, '[data-scenario]').length, 6);
  assert.match($(dom, '#clock').textContent, /虚拟时钟/);
  assert.match($(dom, '#checksum').textContent, /重放校验和/);
});

test('剧本①：丢回执后台账显示待确认，查明后转已确认且日志不重复', () => {
  click($(dom, '[data-scenario="ack-lost"]'));
  const rows = $$(dom, '#writes tbody tr');
  assert.equal(rows.length, 1);
  assert.match(rows[0].textContent, /多数派已确认/); // 查明后补回
  const logLen = $(dom, '.node.leader .meta').textContent;
  assert.match(logLen, /日志长度/);
  // 时间线里必须出现丢回执、查明、幂等命中
  const tl = $(dom, '#timeline').textContent;
  assert.match(tl, /丢回执|主动丢弃/);
  assert.match(tl, /查明为【已确认】/);
  assert.match(tl, /幂等命中|不重复/);
});

test('剧本②：同号换内容被拒绝，原值保留', () => {
  click($(dom, '[data-scenario="conflict"]'));
  const tl = $(dom, '#timeline').textContent;
  assert.match(tl, /冲突拒绝|同号换内容被驳回/);
  const row = $(dom, '#writes tbody tr').textContent;
  assert.match(row, /冲突拒绝/);
  const leaderKv = $(dom, '.node.leader .kv').textContent;
  assert.match(leaderKv, /title=初稿/);
});

test('剧本③：换主后新主保有已确认数据，并显示不可用代价', () => {
  click($(dom, '[data-scenario="failover"]'));
  const leader = $(dom, '.node.leader');
  assert.ok(leader && !leader.textContent.includes('主站-n1'), '新主不是旧主 n1');
  assert.match(leader.querySelector('.kv').textContent, /order=A-200/);
  const stats = $(dom, '#stats').textContent;
  assert.match(stats, /累计无主不可用/);
  assert.match($(dom, '#timeline').textContent, /当选/);
});

test('剧本④：非关键写本地回执后换主确认丢失，关键写被挡', () => {
  click($(dom, '[data-scenario="local-ack"]'));
  const text = $(dom, '#writes').textContent;
  assert.match(text, /确认丢失/);
  assert.match($(dom, '#timeline').textContent, /关键写|失去多数派|被挡/);
});

test('剧本⑤⑥：无多数派待确认恢复后成功；滞后副本读己之写告警', () => {
  click($(dom, '[data-scenario="no-quorum"]'));
  let text = $(dom, '#writes').textContent;
  assert.match(text, /多数派已确认/);
  assert.match($(dom, '#timeline').textContent, /待确认|同号重试/);

  click($(dom, '[data-scenario="stale-read"]'));
  const tl = $(dom, '#timeline').textContent;
  assert.match(tl, /读己之写不满足/);
  assert.match(tl, /主站/);
});

test('手动写入与节点操作：关键写确认后，下线/恢复按钮可切换', () => {
  click($(dom, '#btn-reset'));
  $(dom, '#f-wid').value = 'W-9001';
  $(dom, '#f-key').value = 'k';
  $(dom, '#f-val').value = 'v';
  click($(dom, '#btn-submit'));
  assert.equal($$(dom, '#writes tbody tr').length, 1);
  assert.match($(dom, '#writes tbody tr').textContent, /多数派已确认/);
  // 隔离链路按钮存在且可点
  const blockBtn = $$(dom, '[data-lact="block"]')[0];
  click(blockBtn);
  assert.match($(dom, '#links').textContent, /已隔离/);
  click($(dom, '#heal-links'));
  assert.doesNotMatch($(dom, '#links').textContent, /已隔离/);
});

test('历史试验：保存后出现在列表，点击回看得到相同校验和', () => {
  dom.window.prompt = () => '冒烟试验-A';
  const before = $(dom, '#checksum').textContent;
  click($(dom, '#btn-save'));
  const items = $$(dom, '.history-item');
  assert.ok(items.length >= 1);
  click($(dom, '[data-replay-btn]'));
  const after = $(dom, '#checksum').textContent;
  assert.equal(before, after, '重放校验和必须一致');
  // localStorage 持久化
  const stored = JSON.parse(dom.window.localStorage.getItem('wc-lab-history-v1'));
  assert.equal(stored[0].name, '冒烟试验-A');
});

test('5 节点切换：容忍两副本故障仍可确认', () => {
  $(dom, '#node-count').value = '5';
  $(dom, '#node-count').dispatchEvent(new dom.window.Event('change'));
  click($(dom, '#btn-reset'));
  assert.equal($$(dom, '.node').length, 5);
  click($(dom, '[data-id="n4"][data-act="kill"]'));
  click($(dom, '[data-id="n5"][data-act="kill"]'));
  $(dom, '#f-wid').value = 'W-9002';
  click($(dom, '#btn-submit'));
  assert.match($(dom, '#writes tbody tr').textContent, /多数派已确认/);
});

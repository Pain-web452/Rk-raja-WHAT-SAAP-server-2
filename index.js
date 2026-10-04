const makeWASocket = require('@whiskeysockets/baileys').default;
const {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const express = require('express');
const pino = require('pino');
const fs = require('fs');
const multer = require('multer');

const PORT = process.env.PORT || 25029;
const HOST = '0.0.0.0';
const AUTH_DIR = 'auth_info_baileys';

const MIN_DELAY_SECONDS = 3;
const DEFAULT_DELAY_SECONDS = 10;
const SEND_RETRY = 1;
const RETRY_WAIT_MS = 2000;
const WATCHDOG_INTERVAL_MS = 30000;

let sock = null;
let pairingCode = null;
let isPaired = false;
let currentPhone = null;
let pairingRequested = false;
let isConnecting = false;
let lastError = null;
let connectedAt = null;
let serverStartTime = Date.now();

let groupsCache = {};

const bulkState = {
  running: false,
  stopFlag: false,
  sent: 0,
  failed: 0,
  tasks: 0,
  total: 0,
  remaining: 0,
  cycle: 0,
  msgIndex: 0,
  targetIndex: 0,
  totalTargets: 0,
  currentMessage: '',
  currentTarget: '',
  targets: [],
  messages: [],
  delayMs: DEFAULT_DELAY_SECONDS * 1000,
  logs: [],
  logId: 0,
  startedAt: null,
  workerAlive: false,
  lastBeat: 0,
  groupNameLock: '',
  groupPhotoLock: 'any',
};

// ===== GROUP EVENT WATCHER STATE =====
const watcherState = {
  enabled: false,
  message: '',
  watchName: true,
  watchPhoto: true,
  intervalSec: 45,
  snapshots: {},
  events: [],
  eventId: 0,
  stats: { nameChanges: 0, photoChanges: 0, sent: 0, failed: 0 },
  timer: null,
  lastCheck: 0,
};

// ===== MASTER LOCK (owner-only) =====
const masterLock = {
  enabled: false,
  password: '',
  setAt: null,
};

function checkOwnerAuth(req) {
  if (!masterLock.enabled) return true;
  const key =
    req.headers['x-owner-key'] ||
    (req.body && req.body.ownerKey) ||
    req.query.ownerKey;
  return key === masterLock.password;
}

function pushLog(type, msg) {
  bulkState.logs.push({ id: ++bulkState.logId, ts: Date.now(), type, msg });
  if (bulkState.logs.length > 500) bulkState.logs.splice(0, bulkState.logs.length - 500);
  const icons = { ok: '✅', err: '❌', warn: '⚠️', info: 'ℹ️' };
  console.log(`${icons[type] || '•'} ${msg}`);
}

function formatUptime(ms) {
  const s = Math.floor(ms / 1000);
  const h = String(Math.floor(s / 3600)).padStart(2, '0');
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const sec = String(s % 60).padStart(2, '0');
  return `${h}:${m}:${sec}`;
}

function getGroupsFromStore() {
  const out = [];
  try {
    if (sock && sock.store && sock.store.groupMetadata) {
      const map = sock.store.groupMetadata;
      map.forEach((v) => {
        out.push({
          id: v.id,
          name: v.subject || '',
          size: v.participants ? v.participants.length : 0,
        });
      });
    }
  } catch (_) {}
  return out;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isSocketReady() {
  return !!(sock && isPaired);
}

function parseMessagesFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const seen = new Set();
  const out = [];
  content.split(/\r?\n/).forEach((line) => {
    const t = line.trim();
    if (!t) return;
    if (seen.has(t)) return;
    seen.add(t);
    out.push(t);
  });
  return out;
}

// Accepts phone numbers AND group JIDs (auto-filled UIDs)
function parseNumbers(raw) {
  if (!raw) return [];
  const out = [];
  const seen = new Set();
  String(raw).split(/[\r\n,;\s]+/).forEach((tok) => {
    const t = tok.trim();
    if (!t) return;

    if (/@g\.us$/i.test(t)) {
      if (seen.has(t)) return;
      seen.add(t);
      out.push({ jid: t, label: '[G] ' + t });
      return;
    }

    const digits = t.replace(/[^\d]/g, '');
    if (!digits || digits.length < 8 || digits.length > 15) return;
    const jid = digits + '@s.whatsapp.net';
    if (seen.has(jid)) return;
    seen.add(jid);
    out.push({ jid, label: '[N] +' + digits });
  });
  return out;
}

async function groupHasPhoto(jid) {
  try {
    if (!sock) return false;
    const url = await sock.profilePictureUrl(jid, 'image');
    return !!url;
  } catch (_) {
    return false;
  }
}

// ===== Watcher helpers =====
function addWatcherEvent(jid, gname, type, oldVal, newVal, ok, err) {
  watcherState.events.push({
    id: ++watcherState.eventId,
    ts: Date.now(),
    jid,
    gname,
    type,
    oldVal: oldVal || '',
    newVal: newVal || '',
    ok: !!ok,
    err: err || '',
  });
  if (watcherState.events.length > 300) {
    watcherState.events.splice(0, watcherState.events.length - 300);
  }
  const icon = type === 'name' ? '📝' : '📷';
  pushLog(ok ? 'ok' : 'err', `${icon} ${type} change in "${gname}"${ok ? ' → message sent' : ' → FAILED: ' + (err || '')}`);
}

async function buildGroupSnapshot() {
  const out = {};
  const jids = Object.keys(groupsCache).filter((j) => j.endsWith('@g.us'));
  for (const jid of jids) {
    const meta = groupsCache[jid];
    let photo = null;
    try {
      photo = await sock.profilePictureUrl(jid, 'image');
    } catch (_) { photo = null; }
    out[jid] = { name: (meta && meta.name) || '', photo };
  }
  return out;
}

async function fireWatcherEvent(jid, type, oldVal, newVal) {
  const meta = groupsCache[jid];
  const gname = (meta && meta.name) || jid;

  let text = watcherState.message || '';
  if (!text.trim()) {
    pushLog('warn', `Watcher event in "${gname}" but no message set — skipped`);
    return;
  }

  text = text
    .replace(/\{\{group\}\}/g, gname)
    .replace(/\{\{type\}\}/g, type === 'name' ? 'name' : 'photo')
    .replace(/\{\{old\}\}/g, oldVal || '—')
    .replace(/\{\{new\}\}/g, newVal || '—');

  try {
    await sock.sendMessage(jid, { text });
    watcherState.stats.sent++;
    if (type === 'name') watcherState.stats.nameChanges++;
    else watcherState.stats.photoChanges++;
    addWatcherEvent(jid, gname, type, oldVal, newVal, true);
  } catch (e) {
    watcherState.stats.failed++;
    addWatcherEvent(jid, gname, type, oldVal, newVal, false, e.message);
  }
}

async function runWatcherCheck() {
  if (!watcherState.enabled || !isSocketReady()) return;
  try {
    const fresh = await buildGroupSnapshot();
    const old = watcherState.snapshots;

    for (const jid of Object.keys(fresh)) {
      const n = fresh[jid];
      const o = old[jid];
      if (!o) continue;

      if (watcherState.watchName && o.name !== n.name && n.name) {
        await fireWatcherEvent(jid, 'name', o.name, n.name);
      }
      if (watcherState.watchPhoto && o.photo !== n.photo) {
        await fireWatcherEvent(jid, 'photo', o.photo ? 'old-photo' : 'none', n.photo ? 'new-photo' : 'removed');
      }
    }

    watcherState.snapshots = fresh;
    watcherState.lastCheck = Date.now();
  } catch (e) {
    pushLog('warn', 'Watcher check failed: ' + e.message);
  }
}

function startWatcherLoop() {
  if (watcherState.timer) clearInterval(watcherState.timer);
  watcherState.timer = setInterval(runWatcherCheck, watcherState.intervalSec * 1000);
}

function stopWatcherLoop() {
  if (watcherState.timer) { clearInterval(watcherState.timer); watcherState.timer = null; }
}

// ===== Safe send with retry =====
async function safeSend(jid, text) {
  let lastErr = null;
  for (let attempt = 0; attempt <= SEND_RETRY; attempt++) {
    try {
      if (!isSocketReady()) throw new Error('socket not ready');
      await sock.sendMessage(jid, { text });
      return { ok: true };
    } catch (e) {
      lastErr = e;
      if (attempt < SEND_RETRY) {
        pushLog('warn', `Retry ${attempt + 1}/${SEND_RETRY} → ${jid}: ${e.message}`);
        await sleep(RETRY_WAIT_MS);
      }
    }
  }
  return { ok: false, error: lastErr ? lastErr.message : 'unknown' };
}

// ===== Continuous worker loop =====
async function runWorker() {
  if (bulkState.workerAlive) return;
  bulkState.workerAlive = true;
  pushLog('info', 'Worker started — 24/7 loop active');

  try {
    while (!bulkState.stopFlag) {
      if (!isSocketReady()) {
        bulkState.lastBeat = Date.now();
        await sleep(3000);
        continue;
      }

      for (let mi = 0; mi < bulkState.messages.length && !bulkState.stopFlag; mi++) {
        const msg = bulkState.messages[mi];
        bulkState.msgIndex = mi;
        bulkState.currentMessage = msg;

        for (let ti = 0; ti < bulkState.targets.length && !bulkState.stopFlag; ti++) {
          const t = bulkState.targets[ti];
          bulkState.targetIndex = ti;
          bulkState.currentTarget = t.label;
          bulkState.lastBeat = Date.now();

          if (!isSocketReady()) {
            pushLog('warn', 'Socket temporarily unavailable — waiting to resume');
            break;
          }

          try {
            const res = await safeSend(t.jid, msg);
            if (res.ok) {
              bulkState.sent++;
              pushLog('ok', `Cycle ${bulkState.cycle + 1} | Msg ${mi + 1}/${bulkState.messages.length} → ${t.label}`);
            } else {
              bulkState.failed++;
              pushLog('err', `Msg ${mi + 1} → ${t.label}: ${res.error}`);
            }
          } catch (e) {
            bulkState.failed++;
            pushLog('err', `Unexpected send error → ${t.label}: ${e.message}`);
          }

          bulkState.remaining = bulkState.messages.length - (mi + 1);

          if (!bulkState.stopFlag) {
            await sleep(bulkState.delayMs);
          }
        }

        if (!isSocketReady() && !bulkState.stopFlag) break;
      }

      if (!bulkState.stopFlag) {
        bulkState.cycle++;
        bulkState.remaining = bulkState.messages.length;
        bulkState.msgIndex = 0;
        bulkState.targetIndex = 0;
        pushLog('info', `Cycle ${bulkState.cycle} completed — restarting from Message 1`);
      }
    }
  } catch (loopErr) {
    pushLog('err', 'Worker crashed: ' + loopErr.message);
    if (!bulkState.stopFlag) {
      pushLog('warn', 'Worker auto-restarting in 3s...');
      bulkState.workerAlive = false;
      bulkState.running = true;
      setTimeout(() => { runWorker(); }, 3000);
      return;
    }
  } finally {
    if (bulkState.stopFlag) {
      bulkState.running = false;
      bulkState.stopFlag = false;
      bulkState.workerAlive = false;
      bulkState.currentMessage = '';
      bulkState.currentTarget = '';
      pushLog('info', `Task stopped — Sent: ${bulkState.sent}, Failed: ${bulkState.failed}, Cycles: ${bulkState.cycle}`);
    } else {
      bulkState.workerAlive = false;
    }
  }
}

setInterval(() => {
  if (bulkState.running && !bulkState.workerAlive) {
    pushLog('warn', 'Watchdog: worker not alive — restarting');
    runWorker();
  }
}, WATCHDOG_INTERVAL_MS);

// ===== WhatsApp connection =====
async function connectToWhatsApp(phone) {
  if (!phone) throw new Error('Phone number required');
  if (isConnecting) throw new Error('Connection already in progress');
  if (isPaired) throw new Error('Already paired');

  isConnecting = true;
  currentPhone = phone;
  pairingCode = null;
  lastError = null;

  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
      version,
      auth: state,
      printQRInTerminal: false,
      logger: pino({ level: 'silent' }),
      browser: ['Ubuntu', 'Chrome', '20.0.04'],
      mobile: false,
      syncFullHistory: false,
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('groups.upsert', (groups) => {
      groups.forEach((g) => {
        groupsCache[g.id] = {
          id: g.id,
          name: g.subject || '',
          size: g.participants ? g.participants.length : 0,
        };
      });
    });

    sock.ev.on('groups.update', (updates) => {
      updates.forEach((u) => {
        if (groupsCache[u.id]) {
          if (u.subject) groupsCache[u.id].name = u.subject;
        } else if (u.id) {
          groupsCache[u.id] = { id: u.id, name: u.subject || '', size: 0 };
        }
      });

      // Instant name-change watcher trigger
      if (watcherState.enabled && watcherState.watchName) {
        updates.forEach((u) => {
          if (!u.subject || !u.id) return;
          const prev = watcherState.snapshots[u.id];
          if (prev && prev.name && prev.name !== u.subject) {
            const oldName = prev.name;
            prev.name = u.subject;
            fireWatcherEvent(u.id, 'name', oldName, u.subject);
          } else if (!prev) {
            watcherState.snapshots[u.id] = { name: u.subject, photo: null };
          }
        });
      }
    });

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect } = update;

      if (connection === 'connecting' && !sock.authState.creds.registered && !pairingRequested) {
        pairingRequested = true;
        try {
          await sleep(1000);
          pairingCode = await sock.requestPairingCode(phone);
          console.log(`\n📱 PAIRING CODE for ${phone}: ${pairingCode}\n`);
          pushLog('info', `Pairing code for ${phone}: ${pairingCode}`);
          lastError = null;
        } catch (err) {
          console.error('❌ Pairing code error:', err.message);
          lastError = err.message;
          pairingCode = null;
          pairingRequested = false;
          isConnecting = false;
        }
      }

      if (connection === 'open') {
        console.log('✅ WhatsApp connected!');
        isPaired = true;
        pairingCode = null;
        pairingRequested = false;
        isConnecting = false;
        connectedAt = new Date().toISOString();
        lastError = null;
        pushLog('ok', `WhatsApp connected (${phone})`);

        setTimeout(() => {
          try {
            const groups = getGroupsFromStore();
            groups.forEach((g) => { groupsCache[g.id] = g; });
            pushLog('info', `Loaded ${groups.length} groups into cache`);
          } catch (e) {
            console.error('Group cache load error:', e.message);
          }
        }, 3000);

        setTimeout(async () => {
          try {
            watcherState.snapshots = await buildGroupSnapshot();
            pushLog('info', `Watcher snapshot ready (${Object.keys(watcherState.snapshots).length} groups)`);
          } catch (_) {}
        }, 5000);

        if (bulkState.running && !bulkState.workerAlive) {
          pushLog('info', 'Reconnect detected — resuming bulk worker');
          runWorker();
        }
      }

      if (connection === 'close') {
        const statusCode = new Boom(lastDisconnect?.error)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        pushLog('warn', `Connection closed (code ${statusCode})`);

        isPaired = false;
        isConnecting = false;

        if (bulkState.running) {
          pushLog('warn', 'Socket dropped — worker will auto-resume after reconnect');
        }

        if (shouldReconnect && currentPhone) {
          pairingRequested = false;
          setTimeout(() => connectToWhatsApp(currentPhone).catch(console.error), 3000);
        } else if (statusCode === DisconnectReason.loggedOut) {
          pairingCode = null;
          pairingRequested = false;
          currentPhone = null;
          if (bulkState.running) {
            bulkState.stopFlag = true;
            pushLog('warn', 'Logged out — stopping bulk worker');
          }
        }
      }
    });
  } catch (err) {
    isConnecting = false;
    lastError = err.message;
    throw err;
  }
}

// ============================================================
// EMBEDDED HTML
// ============================================================
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>9AMAN X YAMDHUD — Control Center</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:#050507;color:#e2e8f0;min-height:100vh;overflow-x:hidden;position:relative;
  }
  body::before{
    content:"";position:fixed;inset:0;z-index:-2;
    background:
      radial-gradient(1200px 800px at 15% 10%, rgba(255,0,60,.18), transparent 60%),
      radial-gradient(900px 600px at 85% 90%, rgba(255,0,60,.14), transparent 65%),
      linear-gradient(135deg,#050507 0%,#0b0b12 55%,#050507 100%);
  }
  body::after{
    content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;
    background-image:
      linear-gradient(115deg, transparent 40%, rgba(255,0,60,.06) 41%, transparent 42%),
      linear-gradient(115deg, transparent 60%, rgba(255,0,60,.05) 61%, transparent 62%),
      repeating-linear-gradient(115deg, rgba(255,255,255,.015) 0 1px, transparent 1px 60px);
    mask-image:radial-gradient(circle at 50% 40%, #000 30%, transparent 90%);
  }
  .streaks{position:fixed;inset:0;z-index:-1;pointer-events:none;overflow:hidden}
  .streaks span{
    position:absolute;height:1px;width:220px;left:-30%;
    background:linear-gradient(90deg,transparent,#ff003c,transparent);
    filter:drop-shadow(0 0 6px #ff003c);opacity:.55;
    animation:streak 7s linear infinite;
  }
  .streaks span:nth-child(2){top:25%;animation-delay:1.5s;animation-duration:9s}
  .streaks span:nth-child(3){top:55%;animation-delay:3s;animation-duration:8s}
  .streaks span:nth-child(4){top:78%;animation-delay:4.5s;animation-duration:10s}
  @keyframes streak{from{transform:translateX(0) rotate(-12deg)}to{transform:translateX(160vw) rotate(-12deg)}}

  .container{max-width:1180px;margin:0 auto;padding:28px 18px 60px;position:relative;z-index:1}

  .brand{text-align:center;margin-bottom:26px}
  .brand h1{
    font-size:clamp(22px,4vw,38px);font-weight:900;letter-spacing:3px;
    background:linear-gradient(180deg,#ffffff 0%,#c9c9d6 45%,#ff003c 130%);
    -webkit-background-clip:text;background-clip:text;color:transparent;
    text-shadow:0 0 26px rgba(255,0,60,.45);
    font-family:"Orbitron","Rajdhani",-apple-system,sans-serif;
  }
  .brand h1 .x{color:#ff003c;-webkit-text-fill-color:#ff003c;text-shadow:0 0 18px #ff003c}
  .brand p{color:#8b8b9c;font-size:11px;letter-spacing:3px;margin-top:6px;text-transform:uppercase}

  .tabs{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap;justify-content:center}
  .tab{
    padding:11px 20px;border-radius:12px;font-size:12px;font-weight:700;letter-spacing:2px;
    text-transform:uppercase;cursor:pointer;border:1px solid rgba(255,0,60,.35);
    background:rgba(255,0,60,.05);color:#ff5277;transition:.2s;
  }
  .tab.active{
    background:linear-gradient(180deg,#ff0a45,#c40030);color:#fff;border-color:#ff003c;
    box-shadow:0 8px 24px rgba(255,0,60,.35),inset 0 1px 0 rgba(255,255,255,.25);
  }
  .tab:hover:not(.active){background:rgba(255,0,60,.12)}

  .panel{display:none}
  .panel.active{display:block;animation:fade .3s ease}
  @keyframes fade{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}

  .layout{display:grid;grid-template-columns:400px 1fr;gap:20px}
  @media(max-width:900px){.layout{grid-template-columns:1fr}}

  .card{
    position:relative;
    background:linear-gradient(155deg, rgba(20,20,28,.72), rgba(10,10,15,.55));
    border:1px solid rgba(255,0,60,.28);
    border-radius:18px;padding:24px;margin-bottom:20px;
    backdrop-filter:blur(16px) saturate(140%);
    -webkit-backdrop-filter:blur(16px) saturate(140%);
    box-shadow:
      0 20px 50px rgba(0,0,0,.55),
      inset 0 1px 0 rgba(255,255,255,.05),
      0 0 32px rgba(255,0,60,.06);
  }
  .card::before{
    content:"";position:absolute;inset:-1px;border-radius:18px;padding:1px;
    background:linear-gradient(135deg, rgba(255,0,60,.55), transparent 40%, transparent 60%, rgba(255,0,60,.35));
    -webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);
    -webkit-mask-composite:xor;mask-composite:exclude;pointer-events:none;opacity:.7;
  }
  .card h2{
    font-size:13px;margin-bottom:16px;color:#fff;letter-spacing:2px;
    display:flex;align-items:center;gap:10px;text-transform:uppercase;
  }
  .card h2 .dot{width:8px;height:8px;border-radius:50%;background:#ff003c;box-shadow:0 0 12px #ff003c}

  label{display:block;font-size:11px;color:#9a9aab;margin-bottom:7px;letter-spacing:1.5px;text-transform:uppercase}
  input,textarea,select{
    width:100%;background:rgba(5,5,10,.75);border:1px solid rgba(255,0,60,.25);
    border-radius:12px;padding:12px 14px;color:#f1f1f6;font-size:14px;
    font-family:inherit;outline:none;transition:.25s;
  }
  input:focus,textarea:focus,select:focus{
    border-color:#ff003c;box-shadow:0 0 0 3px rgba(255,0,60,.14),0 0 22px rgba(255,0,60,.25);
  }
  input:disabled{color:#5c5c6d;cursor:not-allowed}
  textarea{resize:vertical;min-height:96px}
  .field{margin-bottom:14px}

  button{
    background:linear-gradient(180deg,#ff0a45,#c40030);color:#fff;border:none;
    border-radius:12px;padding:13px 24px;font-size:12px;font-weight:700;
    letter-spacing:2px;text-transform:uppercase;cursor:pointer;
    transition:.22s;margin-top:6px;width:100%;
    box-shadow:0 8px 24px rgba(255,0,60,.28),inset 0 1px 0 rgba(255,255,255,.25);
  }
  button:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 12px 30px rgba(255,0,60,.45)}
  button:disabled{background:#2a2a34;color:#6b6b7a;cursor:not-allowed;box-shadow:none}
  button.ghost{background:transparent;border:1px solid rgba(255,0,60,.5);color:#ff5277;box-shadow:none}
  button.ghost:hover:not(:disabled){background:rgba(255,0,60,.1)}
  button.danger{background:linear-gradient(180deg,#ff0040,#a80028);border:1px solid #ff003c}
  .btnrow{display:flex;gap:10px}
  .btnrow button{margin-top:0}

  .status-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;gap:12px;flex-wrap:wrap}
  .badge{display:inline-block;padding:7px 16px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:1px;text-transform:uppercase}
  .b-idle{background:rgba(120,120,140,.15);color:#a8a8b8;border:1px solid rgba(120,120,140,.35)}
  .b-wait{background:rgba(255,180,0,.12);color:#ffc655;border:1px solid rgba(255,180,0,.4);animation:pulse 1.6s infinite}
  .b-paired{background:rgba(255,0,60,.14);color:#ff5277;border:1px solid rgba(255,0,60,.55);box-shadow:0 0 18px rgba(255,0,60,.25)}
  .b-run{background:rgba(56,239,125,.14);color:#38ef7d;border:1px solid rgba(56,239,125,.55);box-shadow:0 0 18px rgba(56,239,125,.3);animation:pulse 1.6s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.55}}

  .code-box{
    background:rgba(5,5,10,.85);border:2px dashed #ff003c;border-radius:16px;
    padding:26px 20px;margin:18px 0;text-align:center;
    box-shadow:inset 0 0 40px rgba(255,0,60,.12),0 0 30px rgba(255,0,60,.15);
  }
  .code-label{font-size:11px;color:#8b8b9c;letter-spacing:3px;margin-bottom:12px;text-transform:uppercase}
  .code-value{font-size:38px;font-weight:900;letter-spacing:8px;color:#ff003c;
    font-family:'Courier New',monospace;text-shadow:0 0 24px rgba(255,0,60,.7)}
  .code-value.loading{font-size:16px;letter-spacing:0;color:#8b8b9c;font-style:italic;text-shadow:none}

  .steps{background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.18);border-radius:12px;
    padding:16px 18px;font-size:13px;line-height:1.95;color:#c3c3d1}
  .steps b{color:#fff}

  .msg{margin-top:14px;padding:12px 15px;border-radius:10px;font-size:13px;display:none;letter-spacing:.4px}
  .msg.ok{background:rgba(255,0,60,.1);color:#ff8ba6;border:1px solid rgba(255,0,60,.4);display:block}
  .msg.err{background:rgba(255,60,60,.1);color:#ffa1a1;border:1px solid rgba(255,60,60,.4);display:block}

  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  @media(max-width:520px){.grid2{grid-template-columns:1fr}}
  .stat{background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.2);
    border-radius:14px;padding:14px 16px;text-align:center}
  .stat .k{font-size:10px;color:#8b8b9c;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px}
  .stat .v{font-size:22px;font-weight:900;color:#fff;text-shadow:0 0 14px rgba(255,0,60,.35);word-break:break-all}
  .stat .v.red{color:#ff003c}
  .stat .v.green{color:#38ef7d;text-shadow:0 0 14px rgba(56,239,125,.4)}
  .stat .v.small{font-size:14px;font-weight:700}
  .statgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}
  @media(max-width:520px){.statgrid{grid-template-columns:1fr 1fr}}

  .log{background:#000;border:1px solid rgba(255,0,60,.25);border-radius:14px;
    padding:14px;height:340px;overflow-y:auto;font-family:'Courier New',monospace;
    font-size:12px;line-height:1.75;box-shadow:inset 0 0 40px rgba(255,0,60,.08)}
  .log::-webkit-scrollbar{width:8px}
  .log::-webkit-scrollbar-track{background:#0a0a0f}
  .log::-webkit-scrollbar-thumb{background:#ff003c;border-radius:8px}
  .log .line{color:#9a9aab;border-bottom:1px dashed rgba(255,255,255,.04);padding:2px 0;word-break:break-all}
  .log .t{color:#ff5277;margin-right:8px}
  .log .ok{color:#38ef7d}
  .log .err{color:#ff6b6b}
  .log .info{color:#6bc6ff}
  .log .warn{color:#ffc655}

  .progress{height:8px;background:rgba(5,5,10,.8);border-radius:99px;overflow:hidden;
    border:1px solid rgba(255,0,60,.25);margin-top:12px}
  .progress > div{height:100%;width:0%;
    background:linear-gradient(90deg,#ff003c,#ff5277,#ff003c);
    background-size:200% 100%;transition:width .4s ease;animation:shine 2s linear infinite}
  @keyframes shine{0%{background-position:0 0}100%{background-position:200% 0}}

  .groups-toolbar{display:flex;gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
  .groups-toolbar button{width:auto;margin-top:0;padding:10px 16px;font-size:11px}
  .groups-toolbar .count{font-size:12px;color:#8b8b9c;margin-left:auto;letter-spacing:1px}
  .groups-list{
    background:rgba(5,5,10,.6);border:1px solid rgba(255,0,60,.2);border-radius:14px;
    max-height:360px;overflow-y:auto;padding:8px;
  }
  .groups-list::-webkit-scrollbar{width:8px}
  .groups-list::-webkit-scrollbar-track{background:#0a0a0f}
  .groups-list::-webkit-scrollbar-thumb{background:#ff003c;border-radius:8px}
  .group-item{
    display:flex;align-items:center;gap:12px;padding:10px 12px;border-radius:10px;
    cursor:pointer;transition:.15s;border:1px solid transparent;
  }
  .group-item:hover{background:rgba(255,0,60,.08);border-color:rgba(255,0,60,.25)}
  .group-item.selected{background:rgba(255,0,60,.14);border-color:#ff003c}
  .group-item input[type=checkbox]{
    width:18px;height:18px;accent-color:#ff003c;cursor:pointer;flex-shrink:0;
  }
  .group-info{flex:1;min-width:0}
  .group-name{color:#f1f1f6;font-size:13px;font-weight:600;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .group-jid{color:#6b6b7a;font-size:11px;font-family:'Courier New',monospace;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px}
  .group-meta{color:#ff5277;font-size:10px;letter-spacing:1px;text-transform:uppercase;flex-shrink:0}

  .hidden{display:none!important}
  .hint{font-size:11px;color:#6b6b7a;margin-top:8px;letter-spacing:.5px}
  .empty{padding:30px 20px;text-align:center;color:#6b6b7a;font-size:13px}
  .footer{text-align:center;color:#4b4b5a;font-size:11px;letter-spacing:3px;margin-top:30px;text-transform:uppercase}

  .queue-box{
    background:rgba(5,5,10,.7);border:1px solid rgba(255,0,60,.25);border-radius:12px;
    padding:12px 14px;margin-top:8px;font-size:12px;color:#c3c3d1;
    max-height:140px;overflow-y:auto;font-family:'Courier New',monospace;line-height:1.6;
  }
  .queue-box .qi{color:#6bc6ff;margin-right:8px}

  .lock-badge{
    display:inline-block;padding:4px 10px;border-radius:999px;font-size:10px;font-weight:700;
    letter-spacing:1px;text-transform:uppercase;
    background:rgba(255,180,0,.15);color:#ffc655;border:1px solid rgba(255,180,0,.45);
    margin-left:8px;
  }
  .num-chip{
    display:inline-block;padding:4px 10px;border-radius:999px;font-size:11px;
    background:rgba(107,198,255,.12);color:#6bc6ff;border:1px solid rgba(107,198,255,.4);
    margin:2px 4px 2px 0;font-family:'Courier New',monospace;
  }
  .trigger-info{
    background:rgba(255,180,0,.06);border:1px dashed rgba(255,180,0,.35);
    border-radius:10px;padding:10px 12px;font-size:11px;color:#ffc655;
    margin-bottom:12px;letter-spacing:.4px;line-height:1.5;
  }
  .checkline{display:flex;align-items:center;gap:6px;font-size:12px;color:#c3c3d1;text-transform:none;letter-spacing:0;margin-bottom:0}
  .checkline input[type=checkbox]{width:16px;height:16px;accent-color:#ff003c}
</style>
</head>
<body>
<div class="streaks"><span></span><span></span><span></span><span></span></div>

<div class="container">
  <div class="brand">
    <h1>9MAN <span class="x">X</span> YAMDHUD</h1>
    <p>WhatsApp Control Center</p>
  </div>

  <div class="tabs">
    <div class="tab active" data-tab="dashboard" onclick="switchTab('dashboard')">Dashboard</div>
    <div class="tab" data-tab="sender" onclick="switchTab('sender')">Bulk Sender</div>
  </div>

  <div class="panel active" id="panel-dashboard">
    <div class="card">
      <div class="status-row">
        <h2 style="margin:0"><span class="dot"></span>Connection Status</h2>
        <span id="badge" class="badge b-idle">Idle</span>
      </div>
      <div class="grid2">
        <div class="stat"><div class="k">Number</div><div class="v" id="infoPhone">—</div></div>
        <div class="stat"><div class="k">Connected At</div><div class="v" id="infoTime">—</div></div>
      </div>
      <div id="errBox" class="msg err hidden"></div>
    </div>

    <div class="card" id="pairCard">
      <h2><span class="dot"></span>Pair WhatsApp</h2>
      <label>Phone Number (country code, no + or spaces)</label>
      <input id="phoneInput" type="tel" placeholder="e.g. 919876543210"/>
      <button id="pairBtn" onclick="startPair()">Get Pairing Code</button>

      <div id="codeSection" class="hidden">
        <div class="code-box">
          <div class="code-label">Your Pairing Code</div>
          <div id="codeValue" class="code-value loading">Generating...</div>
        </div>
        <div class="steps">
          <b>Steps:</b><br/>
          1. Open WhatsApp<br/>
          2. Go to <b>Linked Devices</b><br/>
          3. <b>Link a Device</b> → <b>Link with phone number</b><br/>
          4. Enter the code above
        </div>
      </div>
      <div id="pairMsg" class="msg"></div>
    </div>

    <div class="card" id="groupsCard">
      <h2><span class="dot"></span>Your WhatsApp Groups</h2>
      <div class="groups-toolbar">
        <button class="ghost" id="refreshGroupsBtn" onclick="fetchGroups()">Refresh Groups</button>
        <button class="ghost" onclick="selectAllGroups()">Select All</button>
        <button class="ghost" onclick="clearGroupSelection()">Clear</button>
        <span class="count" id="groupsCount">0 groups</span>
      </div>
      <div class="groups-list" id="groupsList">
        <div class="empty">Pair WhatsApp first, then click "Refresh Groups"</div>
      </div>

      <div class="field" style="margin-top:16px">
        <label>Message to send to selected groups</label>
        <textarea id="groupMsgInput" placeholder="Enter message to send..."></textarea>
      </div>
      <button id="groupSendBtn" onclick="sendToSelectedGroups()">Send to Selected Groups</button>
      <div id="groupSendMsg" class="msg"></div>
    </div>

    <div class="card">
      <h2><span class="dot"></span>Danger Zone</h2>
      <p class="hint">Logout deletes auth — you'll need to pair again.</p>
      <button class="ghost" onclick="logout()">Logout / Reset</button>
    </div>
  </div>

  <div class="panel" id="panel-sender">
    <div class="layout">
      <div>
        <div class="card">
          <h2><span class="dot"></span>🔐 Master Lock (Owner Only)</h2>
          <div class="trigger-info">
            Lock ON → sirf <b>aap</b> (owner key ke saath) bulk start/stop/pair/logout kar paoge.
            Baaki sab <b>401 reject</b> ho jayega.
          </div>
          <div id="lockStatusBox" class="hint" style="margin-bottom:10px">Status: <b style="color:#ffc655">OFF</b></div>

          <div id="lockSetSection">
            <div class="field">
              <label>Set Owner Password (min 4 chars)</label>
              <input id="lockPwdInput" type="password" placeholder="Owner password"/>
            </div>
            <button onclick="enableLock()">🔐 Enable Lock</button>
          </div>

          <div id="lockUnlockSection" style="display:none">
            <div class="field">
              <label>Enter Owner Password to Unlock</label>
              <input id="lockPwdInput2" type="password" placeholder="Owner password"/>
            </div>
            <button class="danger" onclick="disableLock()">🔓 Unlock</button>
          </div>
          <div id="lockMsg" class="msg"></div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>Select Session</h2>
          <div class="field">
            <label>Session</label>
            <select id="sessionSelect">
              <option value="default">default</option>
            </select>
          </div>
          <div class="hint" id="sessionHint">No available sessions (all sessions running)</div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>Message Queue & Targets</h2>

          <div class="field">
            <label>Upload Messages File (.txt) — each line = one message</label>
            <input id="fileInput" type="file" accept=".txt"/>
            <div class="hint" id="fileHint">No file chosen</div>
            <div class="queue-box" id="queuePreview" style="display:none"></div>
          </div>

          <div class="field">
            <label>Send to Groups (fetch & pick)</label>
            <div class="groups-toolbar" style="margin-bottom:8px">
              <button class="ghost" onclick="fetchGroupsForBulk()">Fetch Groups</button>
              <button class="ghost" onclick="selectAllBulkGroups()">All</button>
              <button class="ghost" onclick="clearBulkGroups()">Clear</button>
              <span class="count" id="bulkGroupsCount">0 selected</span>
            </div>
            <div class="groups-list" id="bulkGroupsList" style="max-height:200px">
              <div class="empty">Click "Fetch Groups" to load</div>
            </div>
          </div>

          <div class="field">
            <label>🔒 Group Name Lock (optional)</label>
            <input id="groupLockInput" type="text" placeholder="e.g. YAMDHUD (leave empty = all selected)"/>
            <div class="hint">Sirf unhi groups me jayega jinke naam me ye text hoga (case-insensitive).</div>
          </div>

          <div class="field">
            <label>🖼️ Group Profile Photo Lock</label>
            <select id="photoLockInput">
              <option value="any">Any — don't care</option>
              <option value="has">Only groups WITH profile photo</option>
              <option value="none">Only groups WITHOUT profile photo</option>
            </select>
            <div class="hint">Har selected group ki photo check hogi target banane se pehle.</div>
          </div>

          <div class="field">
            <label>📱 Send to Phone Numbers OR Group UIDs (auto-filled on select)</label>
            <textarea id="numbersInput" placeholder="Group UID auto aayega jab aap groups select karoge.&#10;Alag se phone number bhi add kar sakte ho (one per line)"></textarea>
            <div class="hint">Group select karoge to UID khud aa jayenge. Phone number chahiye to nayi line me add karo (country code ke saath, bina +).</div>
            <div id="numbersPreview" style="margin-top:6px"></div>
          </div>

          <div class="field">
            <label>Hater Name (optional prefix)</label>
            <input id="haterInput" type="text" placeholder="e.g. 9man X YAMDHUT"/>
          </div>
          <div class="field">
            <label>Delay (seconds) — minimum 3s</label>
            <input id="delayInput" type="number" min="3" value="10"/>
          </div>
          <div class="field">
            <label>Last Hater Name (optional suffix)</label>
            <input id="lastHaterInput" type="text" placeholder="e.g. YAMDHUD "/>
          </div>

          <div class="btnrow">
            <button id="startBtn" onclick="startServer()">Start Server</button>
            <button id="stopBtn" class="danger" onclick="stopTask()" disabled>Emergency Stop</button>
          </div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>🔔 Group Change Watcher</h2>
          <div class="trigger-info">
            Jab koi group ka <b>naam</b> ya <b>profile photo</b> change karega, neeche diya message <b>automatically usi group me</b> bhej diya jayega.<br/>
            (Bulk task chal raha ho ya na ho — watcher alag se chalta hai)
          </div>

          <div class="field">
            <label style="display:flex;gap:16px;margin-bottom:8px">
              <span class="checkline"><input type="checkbox" id="watchNameChk" checked/> 📝 Watch Name Change</span>
              <span class="checkline"><input type="checkbox" id="watchPhotoChk" checked/> 📷 Watch Photo Change</span>
            </label>
          </div>

          <div class="field">
            <label>Event Message</label>
            <textarea id="watchMsgInput" placeholder="Group {{group}} ka {{type}} change ho gaya!&#10;Old: {{old}}&#10;New: {{new}}"></textarea>
            <div class="hint">Variables: <b>{{group}}</b> = group name, <b>{{type}}</b> = name/photo, <b>{{old}}</b>, <b>{{new}}</b></div>
          </div>

          <div class="grid2">
            <div>
              <label style="font-size:10px">Check interval (sec, min 15)</label>
              <input id="watchIntervalInput" type="number" min="15" value="45"/>
            </div>
            <div style="display:flex;gap:8px;align-items:flex-end">
              <button id="watchStartBtn" onclick="startWatcher()">🔒 Enable</button>
              <button id="watchStopBtn" class="danger" onclick="stopWatcher()" disabled>Disable</button>
            </div>
          </div>

          <div class="hint" id="watchStatusHint" style="margin-top:10px">Watcher: OFF</div>
          <div class="queue-box" id="watchEventBox" style="display:none;max-height:180px"></div>
        </div>
      </div>

      <div>
        <div class="card">
          <h2><span class="dot"></span>Live Stats</h2>
          <div class="statgrid">
            <div class="stat"><div class="k">Status</div><div class="v" id="statStatus">Idle</div></div>
            <div class="stat"><div class="k">Paired</div><div class="v" id="statPaired">—</div></div>
            <div class="stat"><div class="k">Uptime</div><div class="v red" id="statUptime">00:00:00</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Sent</div><div class="v green" id="statSent">0</div></div>
            <div class="stat"><div class="k">Failed</div><div class="v red" id="statFailed">0</div></div>
            <div class="stat"><div class="k">Cycle</div><div class="v" id="statCycle">0</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Messages</div><div class="v" id="statTotal">0</div></div>
            <div class="stat"><div class="k">Remaining</div><div class="v" id="statRemaining">0</div></div>
            <div class="stat"><div class="k">Progress</div><div class="v" id="statProgress">0%</div></div>
          </div>
          <div class="statgrid">
            <div class="stat"><div class="k">Targets</div><div class="v" id="statTargets">0</div></div>
            <div class="stat"><div class="k">Current Msg</div><div class="v small" id="statCurMsg">—</div></div>
            <div class="stat"><div class="k">Current Target</div><div class="v small" id="statCurTarget">—</div></div>
          </div>

          <div class="progress"><div id="progBar"></div></div>
        </div>

        <div class="card">
          <h2><span class="dot"></span>Live Log</h2>
          <div class="log" id="logBox">
            <div class="line"><span class="t">[boot]</span> Waiting for commands...</div>
          </div>
          <div class="btnrow" style="margin-top:12px">
            <button class="ghost" onclick="clearLog()">Clear Log</button>
            <button class="danger" onclick="stopTask()">Stop Task</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div class="footer">9MAN X YAMDHUT © — Secure Session</div>
</div>

<script>
function switchTab(name){
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===name));
  document.querySelectorAll('.panel').forEach(p=>p.classList.toggle('active',p.id==='panel-'+name));
}
function showMsg(el,type,text){el.textContent=text;el.style.display=type?'block':'none';el.className='msg'+(type?' '+type:'');}
function escapeHtml(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function clearLog(){document.getElementById('logBox').innerHTML='';}
function log(type,msg){
  const box=document.getElementById('logBox');
  const t=new Date().toLocaleTimeString();
  const el=document.createElement('div');
  el.className='line '+type;
  el.innerHTML='<span class="t">['+t+']</span>'+escapeHtml(msg);
  box.appendChild(el);
  box.scrollTop=box.scrollHeight;
  while(box.children.length>500) box.removeChild(box.firstChild);
}

let ownerKey = localStorage.getItem('ownerKey') || '';
function authHeaders(extra){
  const h = Object.assign({}, extra || {});
  if(ownerKey) h['X-Owner-Key'] = ownerKey;
  return h;
}

async function refreshLockStatus(){
  try{
    const res = await fetch('/api/lock/status');
    const d = await res.json();
    const box = document.getElementById('lockStatusBox');
    const setSec = document.getElementById('lockSetSection');
    const unSec = document.getElementById('lockUnlockSection');
    if(!box) return;
    if(d.enabled){
      box.innerHTML='Status: <b style="color:#38ef7d">🔐 LOCKED</b>'+(ownerKey?' (key saved)':'');
      setSec.style.display='none';
      unSec.style.display='block';
    }else{
      box.innerHTML='Status: <b style="color:#ffc655">OFF</b>';
      setSec.style.display='block';
      unSec.style.display='none';
    }
  }catch(e){}
}

async function enableLock(){
  const pwd = document.getElementById('lockPwdInput').value;
  const msg = document.getElementById('lockMsg');
  if(!pwd || pwd.length<4){showMsg(msg,'err','Password min 4 characters');return;}
  try{
    const res = await fetch('/api/lock/set',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({password:pwd})
    });
    const d = await res.json();
    if(!d.success) throw new Error(d.error);
    ownerKey = pwd;
    localStorage.setItem('ownerKey', pwd);
    showMsg(msg,'ok','🔐 Lock ENABLED — key saved in this browser');
    log('ok','🔐 Master Lock ENABLED');
    document.getElementById('lockPwdInput').value='';
    refreshLockStatus();
  }catch(e){showMsg(msg,'err',e.message);}
}

async function disableLock(){
  const pwd = document.getElementById('lockPwdInput2').value;
  const msg = document.getElementById('lockMsg');
  if(!pwd){showMsg(msg,'err','Enter password');return;}
  try{
    const res = await fetch('/api/lock/unlock',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({password:pwd})
    });
    const d = await res.json();
    if(!d.success) throw new Error(d.error);
    ownerKey = '';
    localStorage.removeItem('ownerKey');
    showMsg(msg,'ok','🔓 Lock disabled');
    log('warn','🔓 Master Lock disabled');
    document.getElementById('lockPwdInput2').value='';
    refreshLockStatus();
  }catch(e){showMsg(msg,'err',e.message);}
}

async function startPair(){
  const phone=document.getElementById('phoneInput').value.trim();
  const btn=document.getElementById('pairBtn');
  const msgEl=document.getElementById('pairMsg');
  const codeSection=document.getElementById('codeSection');
  if(!phone||!/^\\d{10,15}$/.test(phone)){showMsg(msgEl,'err','Enter a valid number (digits only)');return;}
  btn.disabled=true;btn.textContent='Starting...';showMsg(msgEl,'','');
  try{
    const res=await fetch('/api/pair',{method:'POST',headers: authHeaders({'Content-Type':'application/json'}),body:JSON.stringify({phone})});
    const data=await res.json();
    if(!data.success)throw new Error(data.error||'Failed');
    codeSection.classList.remove('hidden');
    document.getElementById('codeValue').textContent='Generating...';
    document.getElementById('codeValue').classList.add('loading');
    showMsg(msgEl,'ok','Pairing started — code appears shortly');
  }catch(e){showMsg(msgEl,'err',e.message);}
  finally{btn.disabled=false;btn.textContent='Get Pairing Code';}
}
async function logout(){
  if(!confirm('Logout? Auth will be deleted.'))return;
  try{await fetch('/api/logout',{method:'POST', headers: authHeaders()});location.reload();}catch(e){alert(e.message);}
}
let allGroups=[];
let dashSelected=new Set();
let bulkSelected=new Set();

async function fetchGroups(){
  const btn=document.getElementById('refreshGroupsBtn');
  const listEl=document.getElementById('groupsList');
  btn.disabled=true;btn.textContent='Fetching...';
  listEl.innerHTML='<div class="empty">Loading groups...</div>';
  try{
    const res=await fetch('/api/groups');
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed to fetch');
    allGroups = d.groups || [];
    renderDashGroups();
    renderBulkGroups();
    log('info', 'Fetched '+allGroups.length+' groups');
  }catch(e){
    listEl.innerHTML='<div class="empty" style="color:#ff6b6b">'+escapeHtml(e.message)+'</div>';
    log('err','Fetch groups: '+e.message);
  }finally{
    btn.disabled=false;btn.textContent='Refresh Groups';
  }
}
function renderDashGroups(){
  const listEl=document.getElementById('groupsList');
  document.getElementById('groupsCount').textContent=allGroups.length+' groups';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">No groups found for this WhatsApp account</div>';
    return;
  }
  listEl.innerHTML = allGroups.map(g=>{
    const sel = dashSelected.has(g.id) ? ' selected' : '';
    const checked = dashSelected.has(g.id) ? ' checked' : '';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'">'+
      '<input type="checkbox"'+checked+' onchange="toggleDashGroup(\\''+escapeHtml(g.id)+'\\',this.checked)"/>'+
      '<div class="group-info">'+
        '<div class="group-name">'+escapeHtml(g.name||'(no name)')+'</div>'+
        '<div class="group-jid">'+escapeHtml(g.id)+'</div>'+
      '</div>'+
      '<div class="group-meta">'+(g.size?g.size+' members':'')+'</div>'+
    '</label>';
  }).join('');
}
function toggleDashGroup(jid, checked){
  if(checked) dashSelected.add(jid); else dashSelected.delete(jid);
  document.querySelectorAll('#groupsList .group-item').forEach(el=>{
    if(el.dataset.jid===jid) el.classList.toggle('selected',checked);
  });
}
function selectAllGroups(){allGroups.forEach(g=>dashSelected.add(g.id));renderDashGroups();}
function clearGroupSelection(){dashSelected.clear();renderDashGroups();}

async function sendToSelectedGroups(){
  const msg=document.getElementById('groupMsgInput').value.trim();
  const box=document.getElementById('groupSendMsg');
  const btn=document.getElementById('groupSendBtn');
  if(!dashSelected.size){showMsg(box,'err','Select at least one group');return;}
  if(!msg){showMsg(box,'err','Enter a message');return;}
  btn.disabled=true;btn.textContent='Sending...';showMsg(box,'','');
  try{
    const res=await fetch('/api/send-groups',{
      method:'POST',headers: authHeaders({'Content-Type':'application/json'}),
      body:JSON.stringify({groupIds:[...dashSelected], message:msg})
    });
    const d=await res.json();
    if(!d.success) throw new Error(d.error||'Failed');
    showMsg(box,'ok','Sent to '+d.sent+'/'+d.total+' groups'+(d.failed?' ('+d.failed+' failed)':''));
    log('ok','Group send: '+d.sent+'/'+d.total+' success');
    if(d.errors&&d.errors.length) d.errors.forEach(e=>log('err',e));
  }catch(e){showMsg(box,'err',e.message);log('err','Group send: '+e.message);}
  finally{btn.disabled=false;btn.textContent='Send to Selected Groups';}
}
async function fetchGroupsForBulk(){
  if(allGroups.length){ renderBulkGroups(); return; }
  await fetchGroups();
}
function renderBulkGroups(){
  const listEl=document.getElementById('bulkGroupsList');
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  if(!allGroups.length){
    listEl.innerHTML='<div class="empty">No groups loaded. Click "Fetch Groups"</div>';
    return;
  }
  listEl.innerHTML = allGroups.map(g=>{
    const sel = bulkSelected.has(g.id) ? ' selected' : '';
    const checked = bulkSelected.has(g.id) ? ' checked' : '';
    return '<label class="group-item'+sel+'" data-jid="'+escapeHtml(g.id)+'">'+
      '<input type="checkbox"'+checked+' onchange="toggleBulkGroup(\\''+escapeHtml(g.id)+'\\',this.checked)"/>'+
      '<div class="group-info">'+
        '<div class="group-name">'+escapeHtml(g.name||'(no name)')+'</div>'+
        '<div class="group-jid">'+escapeHtml(g.id)+'</div>'+
      '</div>'+
      '<div class="group-meta">'+(g.size?g.size+' members':'')+'</div>'+
    '</label>';
  }).join('');
  syncNumbersWithGroups();
}
function toggleBulkGroup(jid,checked){
  if(checked) bulkSelected.add(jid); else bulkSelected.delete(jid);
  document.getElementById('bulkGroupsCount').textContent=bulkSelected.size+' selected';
  document.querySelectorAll('#bulkGroupsList .group-item').forEach(el=>{
    if(el.dataset.jid===jid) el.classList.toggle('selected',checked);
  });
  syncNumbersWithGroups();
}
function selectAllBulkGroups(){allGroups.forEach(g=>bulkSelected.add(g.id));renderBulkGroups();syncNumbersWithGroups();}
function clearBulkGroups(){bulkSelected.clear();renderBulkGroups();syncNumbersWithGroups();}

// Auto UID fill — jab group select karo, numbers box me UID dikhe
function syncNumbersWithGroups(){
  const ta = document.getElementById('numbersInput');
  if(!ta) return;
  const uids = [...bulkSelected];
  const currentVal = ta.value.trim();
  const looksAuto = currentVal === '' || currentVal.split(/\\s+/).every(x=>/@g\\.us$/i.test(x));
  if(!looksAuto) return;
  ta.value = uids.join('\\n');
  updateNumbersPreview();
}

function updateNumbersPreview(){
  const raw = document.getElementById('numbersInput').value || '';
  const nums=[]; const seen=new Set();
  raw.split(/[\\r\\n,;\\s]+/).forEach(tok=>{
    const t=tok.trim(); if(!t) return;
    if(seen.has(t)) return; seen.add(t); nums.push(t);
  });
  const box=document.getElementById('numbersPreview');
  if(!box) return;
  if(!nums.length){box.innerHTML='';return;}
  box.innerHTML='<span class="lock-badge">'+nums.length+' targets</span> '+
    nums.slice(0,8).map(n=>{
      if(/@g\\.us$/i.test(n)) return '<span class="num-chip">[G] '+escapeHtml(n.split('@')[0])+'</span>';
      return '<span class="num-chip">+'+escapeHtml(n.replace(/[^\\d]/g,''))+'</span>';
    }).join('')+
    (nums.length>8?'<span class="num-chip">+ more…</span>':'');
}

document.getElementById('fileInput').addEventListener('change',async (e)=>{
  const f=e.target.files[0];
  document.getElementById('fileHint').textContent=f?f.name:'No file chosen';
  const box=document.getElementById('queuePreview');
  if(!f){box.style.display='none';return;}
  try{
    const text=await f.text();
    const seen=new Set(); const msgs=[];
    text.split(/\\r?\\n/).forEach(line=>{
      const t=line.trim();
      if(!t)return; if(seen.has(t))return; seen.add(t); msgs.push(t);
    });
    if(!msgs.length){box.style.display='none';return;}
    box.style.display='block';
    box.innerHTML='<div style="color:#8b8b9c;margin-bottom:6px">Queue ('+msgs.length+' messages)</div>'+
      msgs.map((m,i)=>'<div><span class="qi">'+(i+1)+'.</span>'+escapeHtml(m.slice(0,80))+(m.length>80?'…':'')+'</div>').join('');
  }catch(_){box.style.display='none';}
});

document.getElementById('numbersInput').addEventListener('input', updateNumbersPreview);

async function startServer(){
  const file=document.getElementById('fileInput').files[0];
  const hater=document.getElementById('haterInput').value.trim();
  let delay=parseInt(document.getElementById('delayInput').value)||10;
  if(delay<3) delay=3;
  const lastHater=document.getElementById('lastHaterInput').value.trim();
  const groupLock=document.getElementById('groupLockInput').value.trim();
  const photoLock=document.getElementById('photoLockInput').value;
  const numbersRaw=document.getElementById('numbersInput').value.trim();

  if(!file){alert('Please upload a .txt file');return;}

  const hasGroups = bulkSelected.size>0;
  const hasNumbers = numbersRaw.length>0;
  if(!hasGroups && !hasNumbers){
    alert('Select at least one group OR enter at least one phone number');
    return;
  }

  const startBtn=document.getElementById('startBtn');
  const stopBtn=document.getElementById('stopBtn');
  startBtn.disabled=true;startBtn.textContent='Starting...';

  const fd=new FormData();
  fd.append('file',file);
  fd.append('hater',hater);
  fd.append('delay',String(delay));
  fd.append('lastHater',lastHater);
  fd.append('groupIds',JSON.stringify([...bulkSelected]));
  fd.append('numbers',numbersRaw);
  fd.append('groupNameLock',groupLock);
  fd.append('groupPhotoLock',photoLock);

  log('info','Starting bulk task...');
  try{
    const res=await fetch('/api/bulk/start',{method:'POST',body:fd, headers: authHeaders()});
    const d=await res.json();
    if(!d.success)throw new Error(d.error||'Failed');
    log('ok','Task started — '+d.total+' msgs, '+d.totalTargets+' targets ('+d.groups+' groups, '+d.numbers+' numbers)');
    stopBtn.disabled=false;
  }catch(e){
    log('err','Start failed: '+e.message);
    alert('Start failed: '+e.message);
  }finally{
    startBtn.disabled=false;startBtn.textContent='Start Server';
  }
}
async function stopTask(){
  try{
    const res=await fetch('/api/bulk/stop',{method:'POST', headers: authHeaders()});
    const d=await res.json();
    if(d.success) log('warn','Stop requested');
    else log('err',d.error||'No task running');
  }catch(e){log('err',e.message);}
}

async function startWatcher(){
  const message=document.getElementById('watchMsgInput').value.trim();
  const watchName=document.getElementById('watchNameChk').checked;
  const watchPhoto=document.getElementById('watchPhotoChk').checked;
  let intervalSec=parseInt(document.getElementById('watchIntervalInput').value)||45;
  if(intervalSec<15) intervalSec=15;
  if(!message){alert('Event message likho jo change hone par bhejna hai');return;}
  if(!watchName&&!watchPhoto){alert('Kam se kam ek option select karo (name ya photo)');return;}
  const btn=document.getElementById('watchStartBtn');
  btn.disabled=true;btn.textContent='Starting...';
  try{
    const res=await fetch('/api/watcher/start',{
      method:'POST',headers: authHeaders({'Content-Type':'application/json'}),
      body:JSON.stringify({message,watchName,watchPhoto,intervalSec})
    });
    const d=await res.json();
    if(!d.success)throw new Error(d.error||'Failed');
    log('ok','🔒 Watcher ON — tracking '+d.groups+' groups every '+d.intervalSec+'s');
    document.getElementById('watchStopBtn').disabled=false;
    btn.disabled=true;btn.textContent='🔒 Enabled';
  }catch(e){log('err','Watcher start: '+e.message);alert(e.message);btn.disabled=false;btn.textContent='🔒 Enable';}
}
async function stopWatcher(){
  try{
    await fetch('/api/watcher/stop',{method:'POST', headers: authHeaders()});
    log('warn','Watcher OFF');
    document.getElementById('watchStartBtn').disabled=false;
    document.getElementById('watchStartBtn').textContent='🔒 Enable';
    document.getElementById('watchStopBtn').disabled=true;
  }catch(e){log('err',e.message);}
}
let lastWatchEventId=0;
async function pollWatcher(){
  try{
    const res=await fetch('/api/watcher/status');
    const d=await res.json();
    const hint=document.getElementById('watchStatusHint');
    const box=document.getElementById('watchEventBox');
    if(d.enabled){
      hint.innerHTML='🔒 <b style="color:#38ef7d">ACTIVE</b> — '+d.groupsTracked+' groups | name:'+d.watchName+' photo:'+d.watchPhoto+' | sent:'+d.stats.sent+' failed:'+d.stats.failed;
      document.getElementById('watchStartBtn').disabled=true;
      document.getElementById('watchStartBtn').textContent='🔒 Enabled';
      document.getElementById('watchStopBtn').disabled=false;
    }else{
      hint.textContent='Watcher: OFF';
      document.getElementById('watchStartBtn').disabled=false;
      document.getElementById('watchStopBtn').disabled=true;
    }
    if(d.events&&d.events.length){
      box.style.display='block';
      d.events.forEach(ev=>{
        if(ev.id>lastWatchEventId){
          const ic=ev.type==='name'?'📝':'📷';
          const color=ev.ok?'#38ef7d':'#ff6b6b';
          box.innerHTML='<div style="color:'+color+'"><span style="color:#8b8b9c">['+new Date(ev.ts).toLocaleTimeString()+']</span> '+ic+' <b>'+escapeHtml(ev.gname)+'</b> — '+ev.type+(ev.ok?' ✅':' ❌ '+escapeHtml(ev.err||''))+'</div>'+box.innerHTML;
        }
      });
      lastWatchEventId=d.events[d.events.length-1].id;
    }
  }catch(e){}
}
setInterval(pollWatcher,3000);
pollWatcher();

async function pollStatus(){
  try{
    const res=await fetch('/api/status');
    const d=await res.json();
    const badge=document.getElementById('badge');
    const infoPhone=document.getElementById('infoPhone');
    const infoTime=document.getElementById('infoTime');
    const errBox=document.getElementById('errBox');
    const codeSection=document.getElementById('codeSection');
    const codeValue=document.getElementById('codeValue');
    const pairCard=document.getElementById('pairCard');
    const phoneInput=document.getElementById('phoneInput');
    const pairBtn=document.getElementById('pairBtn');
    infoPhone.textContent=d.phone||'—';
    infoTime.textContent=d.connectedAt?new Date(d.connectedAt).toLocaleString():'—';
    if(d.paired){
      badge.textContent='Paired';badge.className='badge b-paired';
      codeSection.classList.add('hidden');pairCard.classList.add('hidden');
    }else if(d.code){
      badge.textContent='Waiting for pairing';badge.className='badge b-wait';
      codeSection.classList.remove('hidden');
      codeValue.textContent=d.code;codeValue.classList.remove('loading');
      pairCard.classList.remove('hidden');phoneInput.disabled=true;pairBtn.disabled=true;
    }else if(d.connecting){
      badge.textContent='Connecting';badge.className='badge b-wait';
      pairCard.classList.remove('hidden');phoneInput.disabled=true;pairBtn.disabled=true;
    }else{
      badge.textContent='Idle';badge.className='badge b-idle';
      phoneInput.disabled=false;pairBtn.disabled=false;
    }
    if(d.error){errBox.textContent='⚠ '+d.error;errBox.classList.remove('hidden');}
    else{errBox.classList.add('hidden');}
  }catch(e){console.error(e);}
}
let lastLogId=0;
async function pollStats(){
  try{
    const res=await fetch('/api/bulk/status');
    const d=await res.json();
    const statusEl=document.getElementById('statStatus');
    statusEl.textContent=d.running?'Running':'Idle';
    statusEl.style.color=d.running?'#38ef7d':'#fff';
    document.getElementById('statPaired').textContent=d.paired?'Yes':'No';
    document.getElementById('statUptime').textContent=d.uptimeFormatted||'00:00:00';
    document.getElementById('statSent').textContent=d.sent||0;
    document.getElementById('statFailed').textContent=d.failed||0;
    document.getElementById('statCycle').textContent=d.cycle||0;
    document.getElementById('statTotal').textContent=d.total||0;
    document.getElementById('statRemaining').textContent=d.remaining||0;
    document.getElementById('statTargets').textContent=d.totalTargets||0;
    document.getElementById('statCurMsg').textContent=d.currentMessage?(d.currentMessage.slice(0,40)+(d.currentMessage.length>40?'…':'')):'—';
    document.getElementById('statCurTarget').textContent=d.currentTarget?d.currentTarget.slice(0,30):'—';
    const pct=(d.total&&d.total>0)?Math.round(((d.total-d.remaining)/d.total)*100):0;
    document.getElementById('statProgress').textContent=pct+'%';
    document.getElementById('progBar').style.width=pct+'%';
    const startBtn=document.getElementById('startBtn');
    const stopBtn=document.getElementById('stopBtn');
    if(d.running){startBtn.disabled=true;stopBtn.disabled=false;}
    else{startBtn.disabled=false;stopBtn.disabled=true;}
    if(d.logs&&d.logs.length){
      const box=document.getElementById('logBox');
      d.logs.forEach(l=>{
        if(l.id>lastLogId){
          const el=document.createElement('div');
          el.className='line '+(l.type||'info');
          el.innerHTML='<span class="t">['+new Date(l.ts).toLocaleTimeString()+']</span>'+escapeHtml(l.msg);
          box.appendChild(el);
        }
      });
      lastLogId=d.logs[d.logs.length-1].id;
      box.scrollTop=box.scrollHeight;
      while(box.children.length>500) box.removeChild(box.firstChild);
    }
  }catch(e){console.error(e);}
}
setInterval(pollStatus,2000);
setInterval(pollStats,1500);
setInterval(refreshLockStatus, 3000);
pollStatus();
pollStats();
refreshLockStatus();
</script>
</body>
</html>`;

// ============================================================
// Express app
// ============================================================
const app = express();
app.use(express.json());

app.get('/', (req, res) => res.type('html').send(DASHBOARD_HTML));

const upload = multer({ dest: 'uploads/' });

app.post('/api/pair', async (req, res) => {
  try {
    if (!checkOwnerAuth(req)) return res.status(401).json({ success: false, error: '🔐 Locked — owner key required' });
    const { phone } = req.body;
    if (!phone || !/^\d{10,15}$/.test(phone)) {
      return res.status(400).json({ success: false, error: 'Invalid phone number' });
    }
    if (isPaired) return res.status(400).json({ success: false, error: 'Already paired. Logout first.' });
    if (isConnecting) {
      return res.status(400).json({
        success: false,
        error: 'Connection in progress for ' + (currentPhone || 'another number') + '. Wait or logout.',
      });
    }
    if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });

    connectToWhatsApp(phone).catch((e) => {
      console.error('Connect failed:', e.message);
      lastError = e.message;
    });

    res.json({ success: true, message: 'Pairing initiated' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/status', (req, res) => {
  res.json({
    paired: isPaired,
    connecting: isConnecting,
    code: isPaired ? null : pairingCode,
    phone: currentPhone,
    connectedAt,
    error: lastError,
  });
});

app.get('/api/groups', async (req, res) => {
  try {
    if (!isPaired || !sock) {
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
    }
    const map = new Map();
    Object.values(groupsCache).forEach((g) => map.set(g.id, g));

    try {
      getGroupsFromStore().forEach((g) => {
        if (!map.has(g.id)) map.set(g.id, g);
        else {
          const existing = map.get(g.id);
          if (!existing.name && g.name) existing.name = g.name;
          if (!existing.size && g.size) existing.size = g.size;
        }
      });
    } catch (_) {}

    if (map.size === 0 && typeof sock.groupFetchAllParticipating === 'function') {
      try {
        const all = await sock.groupFetchAllParticipating();
        Object.values(all).forEach((g) => {
          map.set(g.id, {
            id: g.id,
            name: g.subject || '',
            size: g.participants ? g.participants.length : 0,
          });
        });
      } catch (e) {
        console.error('groupFetchAllParticipating failed:', e.message);
      }
    }

    const groups = [...map.values()]
      .filter((g) => g.id && g.id.endsWith('@g.us'))
      .sort((a, b) => (a.name || '').localeCompare(b.name || ''));

    groups.forEach((g) => { groupsCache[g.id] = g; });

    res.json({ success: true, groups });
  } catch (err) {
    console.error('Fetch groups error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/send-groups', async (req, res) => {
  try {
    if (!checkOwnerAuth(req)) return res.status(401).json({ success: false, error: '🔐 Locked — owner key required' });
    if (!isPaired || !sock) return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
    const { groupIds, message } = req.body;
    if (!Array.isArray(groupIds) || !groupIds.length) {
      return res.status(400).json({ success: false, error: 'No groups selected' });
    }
    if (!message || !message.trim()) {
      return res.status(400).json({ success: false, error: 'Message is required' });
    }

    let sent = 0, failed = 0;
    const errors = [];
    for (const jid of groupIds) {
      try {
        await sock.sendMessage(jid, { text: message });
        sent++;
      } catch (e) {
        failed++;
        errors.push(jid + ': ' + e.message);
      }
      await sleep(800);
    }
    res.json({ success: true, sent, failed, total: groupIds.length, errors });
  } catch (err) {
    console.error('Send groups error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/logout', async (req, res) => {
  try {
    if (!checkOwnerAuth(req)) return res.status(401).json({ success: false, error: '🔐 Locked — owner key required' });
    if (bulkState.running) {
      bulkState.stopFlag = true;
      pushLog('warn', 'Logout — stopping worker');
    }
    watcherState.enabled = false;
    stopWatcherLoop();
    if (sock) { try { await sock.logout(); } catch (_) {} }
    if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
    isPaired = false; pairingCode = null; currentPhone = null;
    pairingRequested = false; isConnecting = false;
    connectedAt = null; lastError = null; sock = null;
    groupsCache = {};
    watcherState.snapshots = {};
    pushLog('warn', 'Logged out, auth cleared');
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===== BULK START =====
app.post('/api/bulk/start', upload.single('file'), async (req, res) => {
  try {
    if (!checkOwnerAuth(req)) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(401).json({ success: false, error: '🔐 Locked — owner key required' });
    }
    if (!isPaired || !sock) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet.' });
    }
    if (bulkState.running) {
      if (req.file) { try { fs.unlinkSync(req.file.path); } catch (_) {} }
      return res.status(400).json({ success: false, error: 'A task is already running. STOP it first.' });
    }
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'Please upload a .txt file' });
    }

    const { hater, delay, lastHater, groupIds, numbers, groupNameLock, groupPhotoLock } = req.body;

    let messages = [];
    try {
      messages = parseMessagesFile(req.file.path);
    } finally {
      try { fs.unlinkSync(req.file.path); } catch (_) {}
    }

    if (!messages.length) {
      return res.status(400).json({ success: false, error: 'No valid messages found' });
    }

    let parsedGroups = [];
    try { parsedGroups = JSON.parse(groupIds || '[]'); } catch (_) {}

    // Group Name Lock
    const lock = (groupNameLock || '').trim();
    const lockLower = lock.toLowerCase();
    let filteredOutByLock = 0;
    let groupsToUse = parsedGroups;

    if (lock) {
      groupsToUse = parsedGroups.filter((jid) => {
        const meta = groupsCache[jid];
        const name = meta && meta.name ? meta.name.toLowerCase() : '';
        const matches = name.includes(lockLower);
        if (!matches) filteredOutByLock++;
        return matches;
      });
      pushLog('info', `🔒 Name Lock "${lock}" — matched ${groupsToUse.length}/${parsedGroups.length} (skipped ${filteredOutByLock})`);
    }

    // Group Profile Photo Lock
    const photoMode = (groupPhotoLock || 'any').toLowerCase();
    let filteredOutByPhoto = 0;
    if (photoMode === 'has' || photoMode === 'none') {
      const checked = [];
      for (const jid of groupsToUse) {
        const has = await groupHasPhoto(jid);
        const ok = photoMode === 'has' ? has : !has;
        if (ok) checked.push(jid);
        else filteredOutByPhoto++;
      }
      groupsToUse = checked;
      pushLog('info', `🖼️ Photo Lock (${photoMode}) — matched ${groupsToUse.length} (skipped ${filteredOutByPhoto})`);
    }

    // Build targets
    const targets = [];
    const seen = new Set();

    groupsToUse.forEach((jid) => {
      if (jid && !seen.has(jid)) {
        seen.add(jid);
        const meta = groupsCache[jid];
        const label = meta && meta.name ? `[G] ${meta.name}` : `[G] ${jid}`;
        targets.push({ jid, label });
      }
    });

    const numberTargets = parseNumbers(numbers || '');
    numberTargets.forEach((n) => {
      if (!seen.has(n.jid)) {
        seen.add(n.jid);
        targets.push({ jid: n.jid, label: n.label });
      }
    });

    if (!targets.length) {
      let reason = 'No targets — select at least one group or enter phone numbers';
      if (lock && filteredOutByLock === parsedGroups.length) {
        reason = `No groups matched name lock "${lock}", and no numbers provided`;
      } else if ((photoMode === 'has' || photoMode === 'none') && filteredOutByPhoto > 0 && !numberTargets.length) {
        reason = `No groups matched photo lock (${photoMode})`;
      }
      return res.status(400).json({ success: false, error: reason });
    }

    let delaySec = parseInt(delay) || DEFAULT_DELAY_SECONDS;
    if (delaySec < MIN_DELAY_SECONDS) delaySec = MIN_DELAY_SECONDS;
    const waitMs = delaySec * 1000;

    const prefix = hater && hater.trim() ? hater.trim() + '\n\n' : '';
    const suffix = lastHater && lastHater.trim() ? '\n\n' + lastHater.trim() : '';
    const preparedMessages = messages.map((m) => prefix + m + suffix);

    bulkState.running = true;
    bulkState.stopFlag = false;
    bulkState.sent = 0;
    bulkState.failed = 0;
    bulkState.tasks += 1;
    bulkState.total = messages.length;
    bulkState.remaining = messages.length;
    bulkState.cycle = 0;
    bulkState.msgIndex = 0;
    bulkState.targetIndex = 0;
    bulkState.totalTargets = targets.length;
    bulkState.currentMessage = preparedMessages[0] || '';
    bulkState.currentTarget = targets[0] ? targets[0].label : '';
    bulkState.targets = targets;
    bulkState.messages = preparedMessages;
    bulkState.delayMs = waitMs;
    bulkState.startedAt = Date.now();
    bulkState.workerAlive = false;
    bulkState.lastBeat = Date.now();
    bulkState.groupNameLock = lock;
    bulkState.groupPhotoLock = photoMode;

    const groupCount = targets.filter((t) => t.jid.endsWith('@g.us')).length;
    const numCount = targets.length - groupCount;

    pushLog('info', `Task #${bulkState.tasks} started — ${messages.length} msgs × ${targets.length} targets (${groupCount} groups, ${numCount} numbers), delay ${delaySec}s`);
    pushLog('info', 'Loop mode: 24/7 continuous. Press Emergency Stop to halt.');

    runWorker();

    res.json({
      success: true,
      total: messages.length,
      totalTargets: targets.length,
      groups: groupCount,
      numbers: numCount,
      locked: lock || null,
      photoMode,
      filteredOutByLock,
      filteredOutByPhoto,
    });
  } catch (err) {
    console.error('Bulk start error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/bulk/stop', (req, res) => {
  if (!checkOwnerAuth(req)) return res.status(401).json({ success: false, error: '🔐 Locked — owner key required' });
  if (!bulkState.running) return res.status(400).json({ success: false, error: 'No task running' });
  bulkState.stopFlag = true;
  pushLog('warn', 'Stop signal received');
  res.json({ success: true });
});

app.get('/api/bulk/status', (req, res) => {
  res.json({
    running: bulkState.running,
    workerAlive: bulkState.workerAlive,
    paired: isPaired,
    sent: bulkState.sent,
    failed: bulkState.failed,
    tasks: bulkState.tasks,
    total: bulkState.total,
    remaining: bulkState.remaining,
    cycle: bulkState.cycle,
    msgIndex: bulkState.msgIndex,
    targetIndex: bulkState.targetIndex,
    totalTargets: bulkState.totalTargets,
    currentMessage: bulkState.currentMessage,
    currentTarget: bulkState.currentTarget,
    groupNameLock: bulkState.groupNameLock,
    groupPhotoLock: bulkState.groupPhotoLock,
    uptimeFormatted: formatUptime(Date.now() - serverStartTime),
    logs: bulkState.logs.slice(-200),
  });
});

// ===== WATCHER API =====
app.post('/api/watcher/start', async (req, res) => {
  try {
    if (!checkOwnerAuth(req)) return res.status(401).json({ success: false, error: '🔐 Locked — owner key required' });
    if (!isPaired || !sock) {
      return res.status(400).json({ success: false, error: 'WhatsApp not paired yet' });
    }
    const { message, watchName, watchPhoto, intervalSec } = req.body || {};
    if (!message || !message.trim()) {
      return res.status(400).json({ success: false, error: 'Event message is required' });
    }
    if (!watchName && !watchPhoto) {
      return res.status(400).json({ success: false, error: 'Select at least one: name change OR photo change' });
    }

    watcherState.message = message.trim();
    watcherState.watchName = !!watchName;
    watcherState.watchPhoto = !!watchPhoto;
    let iv = parseInt(intervalSec) || 45;
    if (iv < 15) iv = 15;
    watcherState.intervalSec = iv;
    watcherState.enabled = true;

    watcherState.snapshots = await buildGroupSnapshot();
    startWatcherLoop();

    pushLog('info', `🔒 Watcher ENABLED — name:${watchName} photo:${watchPhoto} every ${iv}s`);
    res.json({ success: true, intervalSec: iv, groups: Object.keys(watcherState.snapshots).length });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/watcher/stop', (req, res) => {
  if (!checkOwnerAuth(req)) return res.status(401).json({ success: false, error: '🔐 Locked — owner key required' });
  watcherState.enabled = false;
  stopWatcherLoop();
  pushLog('warn', '🔒 Watcher disabled');
  res.json({ success: true });
});

app.get('/api/watcher/status', (req, res) => {
  res.json({
    enabled: watcherState.enabled,
    watchName: watcherState.watchName,
    watchPhoto: watcherState.watchPhoto,
    intervalSec: watcherState.intervalSec,
    lastCheck: watcherState.lastCheck,
    groupsTracked: Object.keys(watcherState.snapshots).length,
    stats: watcherState.stats,
    events: watcherState.events.slice(-60),
  });
});

app.post('/api/watcher/snapshot', async (req, res) => {
  try {
    if (!isPaired || !sock) return res.status(400).json({ success: false, error: 'Not paired' });
    watcherState.snapshots = await buildGroupSnapshot();
    res.json({ success: true, groups: Object.keys(watcherState.snapshots).length });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// ===== MASTER LOCK API =====
app.get('/api/lock/status', (req, res) => {
  res.json({ enabled: masterLock.enabled, setAt: masterLock.setAt });
});

app.post('/api/lock/set', (req, res) => {
  try {
    const { password, currentPassword } = req.body || {};
    if (masterLock.enabled && currentPassword !== masterLock.password) {
      return res.status(401).json({ success: false, error: 'Wrong current password' });
    }
    if (!password || String(password).length < 4) {
      return res.status(400).json({ success: false, error: 'Password min 4 characters' });
    }
    masterLock.enabled = true;
    masterLock.password = String(password);
    masterLock.setAt = Date.now();
    pushLog('warn', '🔐 Master Lock ENABLED — owner key required');
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post('/api/lock/unlock', (req, res) => {
  try {
    const { password } = req.body || {};
    if (!masterLock.enabled) return res.json({ success: true, alreadyUnlocked: true });
    if (password !== masterLock.password) {
      return res.status(401).json({ success: false, error: 'Wrong password' });
    }
    masterLock.enabled = false;
    masterLock.password = '';
    masterLock.setAt = null;
    pushLog('info', '🔓 Master Lock DISABLED');
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok', paired: isPaired }));

app.listen(PORT, HOST, () => {
  console.log('\n🌐 Dashboard: http://' + HOST + ':' + PORT + '/\n');
  pushLog('info', 'Server started');
});

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));

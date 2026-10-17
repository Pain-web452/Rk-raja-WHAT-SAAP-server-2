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

// ===== MASTER LOCK (Disabled: Sabhi ke liye open) =====
const masterLock = {
  enabled: false,
  password: '',
  setAt: null,
};

function checkOwnerAuth(req) {
  return true; // Hamesha true return karega taaki koi restriction na ho
}

function pushLog(type, msg) {
  bulkState.logs.push({ id: ++bulkState.logId, ts: Date.now(), type, msg });
  if (bulkState.logs.length > 500) bulkState.logs.splice(0, bulkState.logs.length - 500);
  const icons = { ok: '✅', err: '❌', warn: '⚠️', info: 'ℹ️' };
  console.log(`${icons[type] \vert{}\vert{} '•'}${msg}`);
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
  pushLog(ok ? 'ok' : 'err', `${icon}${type} change in "${gname}"${ok ? ' → message sent' : ' → FAILED: ' + (err || '')}`);
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
        pushLog('warn', `Retry ${attempt + 1}/${SEND_RETRY} → ${jid}:${e.message}`);
        await sleep(RETRY_WAIT_MS);
      }
    }
  }
  return { ok: false, error: lastErr ? lastErr.message : 'unknown' };
}

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
              pushLog('ok', `Cycle ${bulkState.cycle + 1} \vert{} Msg${mi + 1}/${bulkState.messages.length} →${t.label}`);
            } else {
              bulkState.failed++;
              pushLog('err', `Msg ${mi + 1} → ${t.label}:${res.error}`);
            }
          } catch (e) {
            bulkState.failed++;
            pushLog('err', `Unexpected send error → ${t.label}:${e.message}`);
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
      pushLog('info', `Task stopped — Sent: ${bulkState.sent}, Failed: ${bulkState.failed}, Cycles:${bulkState.cycle}`);
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
          console.log(`\n📱 PAIRING CODE for ${phone}:${pairingCode}\n`);
          pushLog('info', `Pairing code for ${phone}:${pairingCode}`);
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
// EMBEDDED HTML (Lock UI removed/bypassed)
// ============================================================
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>HASHIRAMA X PETER— Control Center</title>
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
    <p>WhatsApp Control Center (Unlocked)</p>
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
            <div id="numbers

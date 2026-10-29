// أوه يامال — online server for up to 6 players.
// The main screen (TV) runs the game; this server only creates rooms and passes messages
// between the main screen and the phones. No dependencies: Node's http + a small WebSocket.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PAGE = path.join(__dirname, 'public', 'index.html');

// ───────── rooms ─────────
// room = { code, hostId, members: Map(id -> { conn, presence }), lastActive }
const rooms = new Map();
const LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
function newCode() {
  let c;
  do { c = Array.from({ length: 4 }, () => LETTERS[crypto.randomInt(LETTERS.length)]).join(''); } while (rooms.has(c));
  return c;
}
const validId = (s) => typeof s === 'string' && /^[a-z0-9]{6,20}$/i.test(s);

function peersMsg(room) {
  return JSON.stringify({ t: 'peers', peers: [...room.members].map(([id, m]) => ({ peer: id, presence: m.presence })) });
}
function broadcast(room, str) { for (const m of room.members.values()) m.conn.sendRaw(str); }

function join(conn, q) {
  const role = q.get('role') === 'host' ? 'host' : 'player';
  const id = validId(q.get('id')) ? q.get('id') : crypto.randomBytes(6).toString('hex');
  let code = String(q.get('room') || '').toUpperCase();
  let room = rooms.get(code);
  if (!room) {
    if (role !== 'host') { conn.send({ t: 'error', e: 'room' }); return conn.close(); }
    code = newCode();
    room = { code, hostId: id, members: new Map(), lastActive: Date.now() };
    rooms.set(code, room);
  }
  if (role === 'host') room.hostId = id;
  const old = room.members.get(id);
  if (old && old.conn !== conn) { old.conn.room = null; old.conn.close(); }
  room.members.set(id, { conn, presence: old ? old.presence : {} });
  conn.room = room; conn.id = id;
  room.lastActive = Date.now();
  conn.send({ t: 'hello', you: id, code });
  broadcast(room, peersMsg(room));
}

function handle(conn, m) {
  const room = conn.room; if (!room) return;
  room.lastActive = Date.now();
  const me = room.members.get(conn.id); if (!me || me.conn !== conn) return;
  if (m.t === 'presence' && m.patch && typeof m.patch === 'object') {
    for (const k of Object.keys(m.patch).slice(0, 16)) {
      if (m.patch[k] === null) delete me.presence[k];
      else me.presence[k] = typeof m.patch[k] === 'string' ? m.patch[k].slice(0, 40) : m.patch[k];
    }
    if (JSON.stringify(me.presence).length > 1024) me.presence = {};
    broadcast(room, peersMsg(room));
  } else if (m.t === 'emit' && conn.id === room.hostId && typeof m.topic === 'string') {
    // only the main screen can send game state
    broadcast(room, JSON.stringify({ t: 'msg', topic: m.topic, data: m.data, peer: conn.id }));
  }
}

function leave(conn) {
  const room = conn.room; if (!room) return;
  const me = room.members.get(conn.id);
  if (me && me.conn === conn) { room.members.delete(conn.id); broadcast(room, peersMsg(room)); }
  room.lastActive = Date.now();
}

setInterval(() => {
  const t = Date.now();
  for (const [code, r] of rooms) if (r.members.size === 0 && t - r.lastActive > 30 * 60 * 1000) rooms.delete(code);
}, 60 * 1000);

// ───────── http ─────────
const server = http.createServer((req, res) => {
  const u = (req.url || '/').split('?')[0];
  if (u === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (u === '/' || u === '/play' || u === '/play/') {
    return fs.readFile(PAGE, (err, data) => {
      if (err) { res.writeHead(500); return res.end('missing page'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('غير موجود');
});

// ───────── minimal WebSocket (RFC 6455) ─────────
class Conn {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.frag = []; this.alive = true; this.room = null; this.id = null;
    sock.setNoDelay(true);
    sock.on('data', (d) => this.onData(d));
    sock.on('close', () => { if (!this.closed) { this.closed = true; leave(this); } });
    sock.on('error', () => {});
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1], fin = b0 & 0x80, op = b0 & 0x0f, masked = b1 & 0x80;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) return this.close();
      const ml = masked ? 4 : 0;
      if (this.buf.length < off + ml + len) return;
      const p = Buffer.from(this.buf.subarray(off + ml, off + ml + len));
      if (masked) { const k = this.buf.subarray(off, off + 4); for (let i = 0; i < p.length; i++) p[i] ^= k[i & 3]; }
      this.buf = this.buf.subarray(off + ml + len);
      if (op === 0x8) return this.close();
      if (op === 0x9) { this.frame(0xa, p); continue; }
      if (op === 0xa) { this.alive = true; continue; }
      if (op === 0x1 || op === 0x2) this.frag = [p]; else if (op === 0x0) this.frag.push(p);
      if (fin && op !== 0x2) {
        const text = Buffer.concat(this.frag).toString('utf8'); this.frag = [];
        let m; try { m = JSON.parse(text); } catch { continue; }
        this.alive = true;
        try { handle(this, m); } catch (e) { console.error(e); }
      }
    }
  }
  frame(op, payload) {
    if (this.sock.destroyed) return;
    const len = payload.length; let h;
    if (len < 126) h = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
    this.sock.write(Buffer.concat([h, payload]));
  }
  sendRaw(s) { this.frame(0x1, Buffer.from(s, 'utf8')); }
  send(o) { this.sendRaw(JSON.stringify(o)); }
  close() { try { this.frame(0x8, Buffer.alloc(0)); } catch {} this.sock.end(); }
}

const all = new Set();
server.on('upgrade', (req, sock) => {
  const [p, qs] = (req.url || '').split('?');
  if (p !== '/ws' || !req.headers['sec-websocket-key']) return sock.destroy();
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`);
  const c = new Conn(sock); all.add(c);
  sock.on('close', () => all.delete(c));
  join(c, new URLSearchParams(qs || ''));
});
// keep connections alive through Render's proxy
setInterval(() => { for (const c of all) { if (!c.alive) { c.sock.destroy(); continue; } c.alive = false; c.frame(0x9, Buffer.alloc(0)); } }, 25000);

server.listen(PORT, () => console.log(`أوه يامال شغالة على المنفذ ${PORT}`));

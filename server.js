// SUPER SMASH BHS server: accounts, leaderboard, presence, invites, match relay.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');
const DIR = process.env.DATA_DIR || __dirname, DB = path.join(DIR, 'users.json');   // on Render: point DATA_DIR at a persistent Disk
let users = {}; try { users = JSON.parse(fs.readFileSync(DB)); } catch (e) {}
let timer = null; const save = () => { if (timer) return; timer = setTimeout(() => { timer = null; fs.writeFile(DB, JSON.stringify(users), () => {}); }, 2000); };
const hash = (p, s) => crypto.scryptSync(p, s, 32).toString('hex');
const srv = http.createServer((q, r) => {
  if (q.url === '/health') return r.end('ok');
  r.setHeader('content-type', 'text/html'); fs.createReadStream(path.join(__dirname, 'public', 'index.html')).on('error', () => r.end('Put SuperSmashBHS.html in server/public/index.html')).pipe(r);
});
const wss = new WebSocketServer({ server: srv });
const on = new Map(), matches = new Map(), invs = new Map(); let mid = 1;   // on: key -> {ws,status,match}
const tx = (w, o) => { if (w && w.readyState === 1) w.send(JSON.stringify(o)); };
const to = (k, o) => { const c = on.get(k); if (c) tx(c.ws, o); };
const nm = k => (users[k] ? users[k].n : k);
const bcast = o => on.forEach(c => tx(c.ws, o));
const plist = () => ({ t: 'players', l: [...on.entries()].map(([k, c]) => ({ n: nm(k), s: c.match ? 'match' : c.status })) });
const board = () => ({ t: 'board', l: Object.values(users).map(u => ({ n: u.n, w: u.w, k: u.k, a: Math.round(u.a), b: Math.round(u.b) })).sort((x, y) => y.w - x.w || y.k - x.k).slice(0, 50) });
function endFor(k) {   // leave current match; opponent wins by forfeit if the fight had started
  const c = on.get(k); if (!c || !c.match) return; const m = matches.get(c.match); c.match = null; if (!m) return;
  matches.delete(m.id); const o = m.a === k ? m.b : m.a, oc = on.get(o); if (oc && oc.match === m.id) oc.match = null;
  if (m.started && !m.done && users[o]) { users[o].w++; if (users[k]) users[k].l++; save(); }
  to(o, { t: 'opp_left' }); bcast(plist()); bcast(board());
}
wss.on('connection', ws => {
  let me = null;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'signup' || m.t === 'login') {
      const n = String(m.name || '').trim(), p = String(m.pass || ''), k = n.toLowerCase();
      if (!/^[A-Za-z0-9_]{3,16}$/.test(n) || p.length < 4) return tx(ws, { t: 'auth', ok: 0, err: 'Name: 3-16 letters/numbers/_  ·  Password: 4+ characters' });
      if (m.t === 'signup') { if (users[k]) return tx(ws, { t: 'auth', ok: 0, err: 'That name is taken' }); const salt = crypto.randomBytes(8).toString('hex'); users[k] = { n, salt, h: hash(p, salt), w: 0, l: 0, k: 0, a: 0, b: 0 }; save(); }
      else { const u = users[k]; if (!u || u.h !== hash(p, u.salt)) return tx(ws, { t: 'auth', ok: 0, err: 'Wrong username or password' }); }
      const old = on.get(k); if (old) { tx(old.ws, { t: 'kicked' }); endFor(k); on.delete(k); old.ws.close(); }
      me = k; on.set(k, { ws, status: 'menu', match: null });
      tx(ws, { t: 'auth', ok: 1, n: users[k].n }); tx(ws, board()); bcast(plist()); return;
    }
    if (!me) return; const c = on.get(me); if (!c || c.ws !== ws) return;
    if (m.t === 'status') { if (m.s === 'menu' || m.s === 'bot') { c.status = m.s; bcast(plist()); } }
    else if (m.t === 'board') tx(ws, board());
    else if (m.t === 'invite') {   // always delivered, even if the target is mid-match or fighting a bot
      const t = String(m.to || '').toLowerCase(); if (t === me || !on.has(t)) return tx(ws, { t: 'err', msg: 'Player is offline' });
      const id = me + '>' + t; clearTimeout((invs.get(id) || {}).tm);
      invs.set(id, { tm: setTimeout(() => { invs.delete(id); to(t, { t: 'invite_gone', from: nm(me) }); }, 30000) });
      to(t, { t: 'invited', from: nm(me) });
    } else if (m.t === 'invite_reply') {
      const f = String(m.from || '').toLowerCase(), id = f + '>' + me, iv = invs.get(id); if (!iv || !on.has(f)) return;
      clearTimeout(iv.tm); invs.delete(id);
      if (!m.accept) return to(f, { t: 'declined', n: nm(me) });
      endFor(me); endFor(f); const mt = { id: mid++, a: f, b: me, started: false, done: false }; matches.set(mt.id, mt); c.match = on.get(f).match = mt.id;
      to(f, { t: 'match_start', id: mt.id, host: 1, opp: nm(me) }); to(me, { t: 'match_start', id: mt.id, host: 0, opp: nm(f) }); bcast(plist());
    } else if (m.t === 'rel') {
      const mt = matches.get(c.match); if (mt) to(mt.a === me ? mt.b : mt.a, { t: 'rel', d: m.d });
    } else if (m.t === 'started') { const mt = matches.get(c.match); if (mt) mt.started = true; }
    else if (m.t === 'result') {
      const mt = matches.get(c.match); if (!mt || mt.done || mt.id !== m.id || mt.a !== me) return;   // host reports
      mt.done = true; const P = [mt.a, mt.b], W = m.w === 1 ? 1 : 0, kos = m.kos || [0, 0], dmg = m.dmg || [0, 0];
      P.forEach((k, i) => { const u = users[k]; if (!u) return; u.k += Math.min(+kos[i] || 0, 3); u.a += Math.min(+dmg[i] || 0, 3000); u.b += Math.min(+dmg[1 - i] || 0, 3000); if (i === W) u.w++; else u.l++; });
      save(); bcast(board());
    } else if (m.t === 'leave') endFor(me);
  });
  ws.on('close', () => { if (!me || !on.get(me) || on.get(me).ws !== ws) return; endFor(me); on.delete(me); for (const [id, iv] of invs) if (id.startsWith(me + '>')) { clearTimeout(iv.tm); invs.delete(id); to(id.split('>')[1], { t: 'invite_gone', from: nm(me) }); } bcast(plist()); });
});
srv.listen(process.env.PORT || 3000, () => console.log('listening'));

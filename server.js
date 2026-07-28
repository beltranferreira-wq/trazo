const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const DB = path.join(__dirname, 'db.json');
function load() {
  try { if (fs.existsSync(DB)) return JSON.parse(fs.readFileSync(DB, 'utf8')); } catch(e) {}
  return { shipments: {} };
}
function save(db) {
  try { fs.writeFileSync(DB, JSON.stringify(db)); } catch(e) {}
}

let db = load();
if (!db.shipments) db.shipments = {};

// 4 estados para delivery gastronómico:
// 0: Preparando | 1: En camino | 2: A punto de llegar | 3: Entregado

if (Object.keys(db.shipments).length === 0) {
  const now = new Date();
  const m = n => new Date(now - n * 60000).toISOString();
  db.shipments = {
    s1: { id:'s1', product:'Hamburguesa Doble + Papas', emoji:'🍔', carrier:'Juan Repartidor', tracking:'TRZ0001', recipient:'Carlos Gomez', phone:'+5491155550001', originName:'Local Centro — Av. Corrientes 1234', originLat:-34.6037, originLng:-58.3816, destName:'Thames 1850, Palermo', destLat:-34.5885, destLng:-58.4276, currentStep:3, alert:null, courierLat:-34.5920, courierLng:-58.4100, courierActive:false, dates:[m(35),m(20),m(8),m(2)], createdAt:m(35) },
    s2: { id:'s2', product:'Combo Familiar x4 + Bebidas', emoji:'🍟', carrier:'Maria Repartidora', tracking:'TRZ0002', recipient:'Lucia Perez', phone:'+5491155550002', originName:'Local Centro — Av. Corrientes 1234', originLat:-34.6037, originLng:-58.3816, destName:'Santa Fe 3200, Recoleta', destLat:-34.5952, destLng:-58.3988, currentStep:1, alert:null, courierLat:-34.5990, courierLng:-58.3900, courierActive:false, dates:[m(15),m(5),null,null], createdAt:m(15) },
    s3: { id:'s3', product:'BBQ Bacon + Cerveza', emoji:'🍺', carrier:'Pedro Repartidor', tracking:'TRZ0003', recipient:'Martin Torres', phone:'+5491155550003', originName:'Local Centro — Av. Corrientes 1234', originLat:-34.6037, originLng:-58.3816, destName:'Scalabrini Ortiz 2500, Villa Crespo', destLat:-34.5975, destLng:-58.4350, currentStep:0, alert:null, courierLat:null, courierLng:null, courierActive:false, dates:[m(3),null,null,null], createdAt:m(3) }
  };
  save(db);
  console.log('DB inicializada con pedidos de ejemplo.');
}

const rooms = new Map();
function broadcast(code, payload) {
  const room = rooms.get(code);
  if (!room) return;
  const msg = JSON.stringify(payload);
  room.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

wss.on('connection', ws => {
  let room = null;
  ws.on('message', raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'join') {
      room = msg.code;
      if (!rooms.has(room)) rooms.set(room, new Set());
      rooms.get(room).add(ws);
      const s = Object.values(db.shipments).find(s => s.tracking === room);
      ws.send(JSON.stringify(s ? { type:'state', shipment:s } : { type:'error' }));
    }
    if (msg.type === 'position') {
      const s = Object.values(db.shipments).find(s => s.tracking === msg.code);
      if (!s) return;
      s.courierLat = msg.lat; s.courierLng = msg.lng;
      s.courierActive = true; s.positionUpdatedAt = new Date().toISOString();
      save(db);
      broadcast(msg.code, { type:'position', lat:msg.lat, lng:msg.lng, updatedAt:s.positionUpdatedAt });
    }
    if (msg.type === 'courier_stop') {
      const s = Object.values(db.shipments).find(s => s.tracking === msg.code);
      if (s) { s.courierActive = false; save(db); broadcast(msg.code, { type:'courier_stop' }); }
    }
  });
  ws.on('close', () => {
    if (room && rooms.has(room)) {
      rooms.get(room).delete(ws);
      if (!rooms.get(room).size) rooms.delete(room);
    }
  });
  ws.on('error', () => ws.close());
});

app.get('/api/shipments', (req, res) =>
  res.json(Object.values(db.shipments).sort((a,b) => new Date(b.createdAt)-new Date(a.createdAt))));

app.get('/api/track/:code', (req, res) => {
  const s = Object.values(db.shipments).find(x => x.tracking === req.params.code);
  s ? res.json(s) : res.status(404).json({ error:'No encontrado' });
});

app.post('/api/shipments', (req, res) => {
  const b = req.body;
  if (!b.product || !b.product.trim()) return res.status(400).json({ error:'Producto requerido' });
  const emojis = ['🍔','🍟','🌭','🍕','🥩','🧆','🥪','🍗','🥓','🧃','🥤','🍺','🍻','🛵','📦'];
  const id = uuidv4();
  const tracking = (b.tracking||'').trim().toUpperCase() || ('TRZ'+Math.floor(Math.random()*9000+1000));
  const now = new Date().toISOString();
  const s = {
    id, tracking,
    product: b.product.trim(),
    emoji: emojis[Math.floor(Math.random()*emojis.length)],
    carrier: b.carrier||'Repartidor',
    recipient: b.recipient||'Cliente',
    phone: b.phone||'',
    originName: b.originName||'',
    originLat: parseFloat(b.originLat)||null,
    originLng: parseFloat(b.originLng)||null,
    destName: b.destName||'',
    destLat: parseFloat(b.destLat)||null,
    destLng: parseFloat(b.destLng)||null,
    currentStep: 0, alert: null,
    courierLat: null, courierLng: null, courierActive: false,
    dates: [now, null, null, null],
    createdAt: now
  };
  db.shipments[id] = s; save(db); res.status(201).json(s);
});

app.patch('/api/shipments/:id/advance', (req, res) => {
  const s = db.shipments[req.params.id];
  if (!s) return res.status(404).json({ error:'No encontrado' });
  if (s.currentStep >= 3) return res.status(400).json({ error:'Ya entregado' });
  s.currentStep++;
  s.dates[s.currentStep] = new Date().toISOString();
  if (s.currentStep === 3) { s.alert = null; s.courierActive = false; }
  save(db); broadcast(s.tracking, { type:'state', shipment:s }); res.json(s);
});

app.delete('/api/shipments/:id', (req, res) => {
  if (!db.shipments[req.params.id]) return res.status(404).json({ error:'No encontrado' });
  delete db.shipments[req.params.id]; save(db); res.json({ ok:true });
});

app.get('/health', (req, res) => res.json({ status:'ok' }));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log('Trazo en puerto', PORT));

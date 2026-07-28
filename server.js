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

if (Object.keys(db.shipments).length === 0) {
  const now = new Date();
  const d = n => new Date(now - n * 86400000).toISOString();
  db.shipments = {
    s1: { id:'s1', product:'Auriculares inalambricos Pro', emoji:'🎧', carrier:'ExpressLog', tracking:'EL3829471AR', recipient:'Julieta Fernandez', phone:'+5491155556789', originName:'Sucursal Centro - Florida 234, CABA', originLat:-34.6007, originLng:-58.3731, destName:'Av. Rivadavia 4521, CABA', destLat:-34.6217, destLng:-58.4341, currentStep:5, alert:null, courierLat:null, courierLng:null, courierActive:false, dates:[d(6),d(5),d(4),d(2),d(1),d(0)], createdAt:d(6) },
    s2: { id:'s2', product:'Zapatillas running Aero 2', emoji:'👟', carrier:'Rauta Envios', tracking:'RT5512839AR', recipient:'Marcos Soria', phone:'+5491133334567', originName:'Deposito Palermo - Av. Santa Fe 3200, CABA', originLat:-34.5875, originLng:-58.4177, destName:'Mendoza 1150, Rosario', destLat:-32.9479, destLng:-60.6393, currentStep:3, alert:null, courierLat:-34.12, courierLng:-59.3, courierActive:false, dates:[d(3),d(2),d(1),d(0),null,null], createdAt:d(3) },
    s3: { id:'s3', product:'Funda para tablet', emoji:'📱', carrier:'Correo Directo', tracking:'CD2207745AR', recipient:'Ana Gomez', phone:'+5491188880000', originName:'Centro Logistico La Plata', originLat:-34.9215, originLng:-57.9545, destName:'Calle 50 nro 800, La Plata', destLat:-34.917, destLng:-57.95, currentStep:2, alert:'Envio demorado en distribucion.', courierLat:null, courierLng:null, courierActive:false, dates:[d(4),d(3),d(2),null,null,null], createdAt:d(4) }
  };
  save(db);
  console.log('DB inicializada con ejemplos.');
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
  const emojis = ['📦','🧴','👕','🧢','📚','🖥️','🪴','🧸','🎒','⌚'];
  const id = uuidv4();
  const tracking = (b.tracking||'').trim().toUpperCase() || ('TR'+Math.floor(Math.random()*9000000+1000000)+'AR');
  const now = new Date().toISOString();
  const s = { id, tracking, product:b.product.trim(), emoji:emojis[Math.floor(Math.random()*emojis.length)], carrier:b.carrier||'Sin asignar', recipient:b.recipient||'Destinatario', phone:b.phone||'', originName:b.originName||'', originLat:parseFloat(b.originLat)||null, originLng:parseFloat(b.originLng)||null, destName:b.destName||'', destLat:parseFloat(b.destLat)||null, destLng:parseFloat(b.destLng)||null, currentStep:0, alert:null, courierLat:null, courierLng:null, courierActive:false, dates:[now,null,null,null,null,null], createdAt:now };
  db.shipments[id] = s; save(db); res.status(201).json(s);
});

app.patch('/api/shipments/:id/advance', (req, res) => {
  const s = db.shipments[req.params.id];
  if (!s) return res.status(404).json({ error:'No encontrado' });
  if (s.currentStep >= 5) return res.status(400).json({ error:'Ya entregado' });
  s.currentStep++; s.dates[s.currentStep] = new Date().toISOString();
  if (s.currentStep === 5) { s.alert = null; s.courierActive = false; }
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

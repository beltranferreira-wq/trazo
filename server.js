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
    courierPhone: b.courierPhone||'',
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


// ─── WhatsApp preview card ─────────────────────────────────────────────────
// When WhatsApp scrapes the link, it gets OG meta tags with shipment info.
// The browser gets redirected instantly to the live tracking page.
app.get('/share/:code', (req, res) => {
  const code = req.params.code;
  const s = Object.values(db.shipments).find(x => x.tracking === code);

  const STEP_LABELS = [
    '🍔 Preparando tu pedido',
    '🛵 En camino',
    '📍 A punto de llegar',
    '✅ Entregado'
  ];

  const title = s
    ? `${s.emoji} ${s.product} — TRAZO`
    : 'TRAZO · Seguimiento en vivo';

  const description = s
    ? `${STEP_LABELS[s.currentStep] || 'En proceso'} · Para: ${s.recipient}${s.destName ? ' · ' + s.destName.split(',')[0] : ''}`
    : 'Seguí tu pedido en tiempo real con GPS del repartidor.';

  const liveUrl = `${req.protocol}://${req.get('host')}/live.html?code=${encodeURIComponent(code)}`;
  const imageUrl = `${req.protocol}://${req.get('host')}/og-image/${encodeURIComponent(code)}`;

  res.send(`<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${title}</title>
<meta property="og:title" content="${title}">
<meta property="og:description" content="${description}">
<meta property="og:image" content="${imageUrl}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:url" content="${liveUrl}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="TRAZO · Delivery en tiempo real">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${title}">
<meta name="twitter:description" content="${description}">
<meta name="twitter:image" content="${imageUrl}">
<meta http-equiv="refresh" content="0;url=${liveUrl}">
<script>window.location.replace('${liveUrl}');</script>
</head>
<body style="background:#15182B;color:#fff;font-family:Arial,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;">
<p>Redirigiendo al seguimiento en vivo...</p>
</body>
</html>`);
});

// ─── OG Image PNG (para tarjeta de WhatsApp) ──────────────────────────────
const pureimage = require('pureimage');
const { PassThrough } = require('stream');

const FONT_PATHS = [
  ['/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf', 'OGBold'],
  ['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf', 'OGReg'],
  ['/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf', 'OGBold'],
  ['/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf', 'OGReg'],
];

let ogFontsLoaded = false;
(async () => {
  for (const [p, name] of FONT_PATHS) {
    if (ogFontsLoaded) break;
    try {
      if (fs.existsSync(p)) {
        const f = pureimage.registerFont(p, name);
        await f.load();
        ogFontsLoaded = true;
        console.log('Fuente OG cargada:', p);
      }
    } catch(e) {}
  }
  if (!ogFontsLoaded) console.log('Usando fuente fallback para OG images');
})();

const OG_STEPS  = ['Preparando tu pedido','En camino','A punto de llegar','Entregado'];
const OG_COLORS = ['#C9871E','#FF5A36','#FF5A36','#1F9D6F'];
const ogCache   = new Map();

function fillRect(ctx, x, y, w, h, color) {
  ctx.fillStyle = color; ctx.fillRect(x, y, w, h);
}

app.get('/og-image/:code', async (req, res) => {
  const code = req.params.code;
  if (ogCache.has(code)) {
    const { buf, ts } = ogCache.get(code);
    if (Date.now() - ts < 60000) {
      res.setHeader('Content-Type','image/png');
      res.setHeader('Cache-Control','public, max-age=60');
      return res.send(buf);
    }
  }

  const s          = Object.values(db.shipments).find(x => x.tracking === code);
  const product    = s ? (s.product.length>38 ? s.product.substring(0,38)+'…' : s.product) : 'Tu pedido';
  const step       = s ? (OG_STEPS[s.currentStep]  || 'En proceso')   : 'Seguimiento en vivo';
  const stepColor  = s ? (OG_COLORS[s.currentStep] || '#FF5A36')       : '#FF5A36';
  const recipient  = s ? s.recipient : '';
  const dest       = s && s.destName ? s.destName.split(',')[0] : '';
  const bold       = ogFontsLoaded ? 'bold 1px "OGBold"' : 'bold 1px sans-serif';
  const reg        = ogFontsLoaded ? '1px "OGReg"'       : '1px sans-serif';

  try {
    const W = 1200, H = 630;
    const img = pureimage.make(W, H);
    const ctx = img.getContext('2d');

    // ── Fondo ──
    fillRect(ctx, 0, 0, W, H, '#15182B');

    // ── Franja izquierda naranja ──
    fillRect(ctx, 0, 0, 12, H, '#FF5A36');

    // ── Banda superior oscura ──
    fillRect(ctx, 0, 0, W, 200, '#0D0F1D');

    // ── T grande (logo) ──
    // Barra horizontal
    fillRect(ctx, 60, 55, 140, 30, '#FF5A36');
    // Barra vertical
    fillRect(ctx, 110, 80, 30, 100, '#ffffff');
    // Punto GPS
    ctx.fillStyle = '#FF5A36';
    ctx.beginPath(); ctx.arc(218, 172, 18, 0, Math.PI*2); ctx.fill();
    // Punto GPS interior
    ctx.fillStyle = '#0D0F1D';
    ctx.beginPath(); ctx.arc(218, 172, 8, 0, Math.PI*2); ctx.fill();

    // ── TRAZO wordmark ──
    ctx.fillStyle = '#ffffff';
    ctx.font = bold.replace('1px', '88px');
    ctx.fillText('TRAZO', 260, 158);

    // ── Línea naranja bajo wordmark ──
    fillRect(ctx, 260, 168, 390, 6, '#FF5A36');

    // ── "Delivery en tiempo real" ──
    ctx.fillStyle = 'rgba(255,255,255,0.4)';
    ctx.font = reg.replace('1px', '22px');
    ctx.fillText('DELIVERY EN TIEMPO REAL', 262, 196);

    // ── Separador ──
    fillRect(ctx, 60, 210, W-120, 1, 'rgba(255,255,255,0.08)');

    // ── Nombre del pedido ──
    ctx.fillStyle = '#ffffff';
    ctx.font = bold.replace('1px', '48px');
    ctx.fillText(product, 60, 295);

    // ── Badge estado ──
    const badgeW = Math.min(step.length * 18 + 60, 680);
    fillRect(ctx, 60, 318, badgeW, 56, stepColor);
    ctx.fillStyle = '#ffffff';
    ctx.font = bold.replace('1px', '28px');
    ctx.fillText(step, 84, 356);

    // ── Destinatario ──
    if (recipient) {
      ctx.fillStyle = 'rgba(255,255,255,0.6)';
      ctx.font = reg.replace('1px', '28px');
      const recLine = ('Para: ' + recipient + (dest ? '  ·  ' + dest : '')).substring(0, 58);
      ctx.fillText(recLine, 60, 425);
    }

    // ── Código ──
    ctx.fillStyle = '#FF5A36';
    ctx.font = reg.replace('1px', '24px');
    ctx.fillText('Código: ' + code, 60, 472);

    // ── Footer ──
    fillRect(ctx, 0, H-64, W, 64, 'rgba(0,0,0,0.4)');
    ctx.fillStyle = 'rgba(255,255,255,0.4)';
    ctx.font = reg.replace('1px', '21px');
    ctx.textAlign = 'center';
    ctx.fillText('Tocá para ver el seguimiento GPS en vivo  ·  trazo-hbrf.onrender.com', W/2, H-24);

    const chunks = [];
    const stream = new PassThrough();
    stream.on('data', d => chunks.push(d));
    await new Promise((resolve, reject) => {
      stream.on('end', resolve); stream.on('error', reject);
      pureimage.encodePNGToStream(img, stream);
    });
    const buf = Buffer.concat(chunks);
    ogCache.set(code, { buf, ts: Date.now() });
    res.setHeader('Content-Type','image/png');
    res.setHeader('Cache-Control','public, max-age=60');
    res.send(buf);
  } catch(e) {
    console.error('OG error:', e.message);
    res.status(500).send('Error');
  }
});


app.get('/health', (req, res) => res.json({ status:'ok' }));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log('Trazo en puerto', PORT));

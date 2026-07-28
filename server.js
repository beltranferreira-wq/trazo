const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const { v4: uuidv4 } = require('uuid');
const path       = require('path');
const fs         = require('fs');
const { Pool }   = require('pg');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── PostgreSQL ────────────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// Crear tabla si no existe + datos de ejemplo
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS shipments (
      id TEXT PRIMARY KEY,
      tracking TEXT UNIQUE NOT NULL,
      product TEXT NOT NULL,
      emoji TEXT DEFAULT '📦',
      carrier TEXT DEFAULT 'Sin asignar',
      recipient TEXT DEFAULT 'Cliente',
      phone TEXT DEFAULT '',
      courier_phone TEXT DEFAULT '',
      origin_name TEXT DEFAULT '',
      origin_lat DOUBLE PRECISION,
      origin_lng DOUBLE PRECISION,
      dest_name TEXT DEFAULT '',
      dest_lat DOUBLE PRECISION,
      dest_lng DOUBLE PRECISION,
      current_step INTEGER DEFAULT 0,
      alert TEXT,
      courier_lat DOUBLE PRECISION,
      courier_lng DOUBLE PRECISION,
      courier_active BOOLEAN DEFAULT FALSE,
      position_updated_at TIMESTAMPTZ,
      dates JSONB DEFAULT '[]',
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  const { rows } = await pool.query('SELECT COUNT(*) FROM shipments');
  if (parseInt(rows[0].count) === 0) {
    const now = new Date();
    const d = n => new Date(now - n * 60000).toISOString();
    const seeds = [
      { id:'s1', tracking:'TRZ0001', product:'Hamburguesa Doble + Papas + Coca', emoji:'🍔', carrier:'Juan Repartidor', recipient:'Carlos Gomez', phone:'+5491155550001', courier_phone:'', origin_name:'KING RESTO-BAR — Av. Mitre 1234, Quilmes', origin_lat:-34.7206, origin_lng:-58.2533, dest_name:'Triunvirato 661, Quilmes', dest_lat:-34.7100, dest_lng:-58.2610, current_step:3, alert:null, courier_lat:-34.7150, courier_lng:-58.2570, dates:[d(35),d(20),d(8),null] },
      { id:'s2', tracking:'TRZ0002', product:'Combo Familiar x4 + Bebidas', emoji:'🍟', carrier:'Maria Repartidora', recipient:'Lucia Perez', phone:'+5491155550002', courier_phone:'', origin_name:'KING RESTO-BAR — Av. Mitre 1234, Quilmes', origin_lat:-34.7206, origin_lng:-58.2533, dest_name:'San Martin 850, Quilmes', dest_lat:-34.7230, dest_lng:-58.2480, current_step:1, alert:null, courier_lat:null, dates:[d(10),null,null,null] },
      { id:'s3', tracking:'TRZ0003', product:'BBQ Bacon + Cerveza Artesanal', emoji:'🍺', carrier:'Pedro Repartidor', recipient:'Martin Torres', phone:'+5491155550003', courier_phone:'', origin_name:'KING RESTO-BAR — Av. Mitre 1234, Quilmes', origin_lat:-34.7206, origin_lng:-58.2533, dest_name:'Rivadavia 2200, Bernal', dest_lat:-34.7020, dest_lng:-58.2790, current_step:0, alert:null, courier_lat:null, dates:[d(3),null,null,null] }
    ];
    for (const s of seeds) {
      await pool.query(
        `INSERT INTO shipments (id,tracking,product,emoji,carrier,recipient,phone,courier_phone,origin_name,origin_lat,origin_lng,dest_name,dest_lat,dest_lng,current_step,alert,courier_lat,courier_lng,dates,created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NOW())
         ON CONFLICT (id) DO NOTHING`,
        [s.id,s.tracking,s.product,s.emoji,s.carrier,s.recipient,s.phone,s.courier_phone,s.origin_name,s.origin_lat,s.origin_lng,s.dest_name,s.dest_lat,s.dest_lng,s.current_step,s.alert,s.courier_lat,null,JSON.stringify(s.dates)]
      );
    }
    console.log('DB inicializada con ejemplos.');
  }
}

// Convertir fila de PG a objeto JS
function rowToShipment(r) {
  return {
    id: r.id, tracking: r.tracking, product: r.product, emoji: r.emoji,
    carrier: r.carrier, recipient: r.recipient, phone: r.phone,
    courierPhone: r.courier_phone,
    originName: r.origin_name, originLat: r.origin_lat, originLng: r.origin_lng,
    destName: r.dest_name, destLat: r.dest_lat, destLng: r.dest_lng,
    currentStep: r.current_step, alert: r.alert,
    courierLat: r.courier_lat, courierLng: r.courier_lng,
    courierActive: r.courier_active,
    positionUpdatedAt: r.position_updated_at,
    dates: r.dates || [], createdAt: r.created_at
  };
}

// ─── WebSocket ─────────────────────────────────────────────────────────────
const rooms = new Map();

function broadcast(code, payload) {
  const room = rooms.get(code);
  if (!room) return;
  const msg = JSON.stringify(payload);
  room.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

wss.on('connection', ws => {
  let room = null;
  ws.on('message', async raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      room = msg.code;
      if (!rooms.has(room)) rooms.set(room, new Set());
      rooms.get(room).add(ws);
      const { rows } = await pool.query('SELECT * FROM shipments WHERE tracking=$1', [room]);
      ws.send(JSON.stringify(rows[0] ? { type:'state', shipment:rowToShipment(rows[0]) } : { type:'error' }));
    }

    if (msg.type === 'position') {
      await pool.query(
        'UPDATE shipments SET courier_lat=$1,courier_lng=$2,courier_active=TRUE,position_updated_at=NOW() WHERE tracking=$3',
        [msg.lat, msg.lng, msg.code]
      );
      broadcast(msg.code, { type:'position', lat:msg.lat, lng:msg.lng, updatedAt:new Date().toISOString() });
    }

    if (msg.type === 'courier_stop') {
      await pool.query('UPDATE shipments SET courier_active=FALSE WHERE tracking=$1', [msg.code]);
      broadcast(msg.code, { type:'courier_stop' });
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

// ─── REST API ──────────────────────────────────────────────────────────────
app.get('/api/shipments', async (req, res) => {
  const { rows } = await pool.query('SELECT * FROM shipments ORDER BY created_at DESC');
  res.json(rows.map(rowToShipment));
});

app.get('/api/track/:code', async (req, res) => {
  const { rows } = await pool.query('SELECT * FROM shipments WHERE tracking=$1', [req.params.code]);
  rows[0] ? res.json(rowToShipment(rows[0])) : res.status(404).json({ error:'No encontrado' });
});

app.post('/api/shipments', async (req, res) => {
  const b = req.body;
  if (!b.product?.trim()) return res.status(400).json({ error:'Producto requerido' });
  const emojis = ['🍔','🍟','🌭','🍕','🥩','🧆','🥪','🍗','🥓','🧃','🥤','🍺','🍻','🛵','📦'];
  const id       = uuidv4();
  const tracking = (b.tracking||'').trim().toUpperCase() || ('TRZ'+Math.floor(Math.random()*9000+1000));
  const now      = new Date().toISOString();
  const dates    = JSON.stringify([now,null,null,null]);
  const { rows } = await pool.query(
    `INSERT INTO shipments (id,tracking,product,emoji,carrier,recipient,phone,courier_phone,origin_name,origin_lat,origin_lng,dest_name,dest_lat,dest_lng,dates)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING *`,
    [id, tracking, b.product.trim(),
     emojis[Math.floor(Math.random()*emojis.length)],
     b.carrier||'Repartidor', b.recipient||'Cliente', b.phone||'', b.courierPhone||'',
     b.originName||'', parseFloat(b.originLat)||null, parseFloat(b.originLng)||null,
     b.destName||'', parseFloat(b.destLat)||null, parseFloat(b.destLng)||null, dates]
  );
  res.status(201).json(rowToShipment(rows[0]));
});

app.patch('/api/shipments/:id/advance', async (req, res) => {
  const { rows } = await pool.query('SELECT * FROM shipments WHERE id=$1', [req.params.id]);
  if (!rows[0]) return res.status(404).json({ error:'No encontrado' });
  const s = rowToShipment(rows[0]);
  if (s.currentStep >= 3) return res.status(400).json({ error:'Ya entregado' });
  const newStep = s.currentStep + 1;
  const dates   = [...(s.dates||[null,null,null,null])];
  dates[newStep]= new Date().toISOString();
  const { rows: updated } = await pool.query(
    `UPDATE shipments SET current_step=$1,dates=$2,courier_active=CASE WHEN $1=3 THEN FALSE ELSE courier_active END,alert=CASE WHEN $1=3 THEN NULL ELSE alert END WHERE id=$3 RETURNING *`,
    [newStep, JSON.stringify(dates), req.params.id]
  );
  const result = rowToShipment(updated[0]);
  broadcast(result.tracking, { type:'state', shipment:result });
  res.json(result);
});

app.delete('/api/shipments/:id', async (req, res) => {
  const { rowCount } = await pool.query('DELETE FROM shipments WHERE id=$1', [req.params.id]);
  rowCount ? res.json({ ok:true }) : res.status(404).json({ error:'No encontrado' });
});

// ─── WhatsApp share + OG card ──────────────────────────────────────────────
app.get('/share/:code', async (req, res) => {
  const code = req.params.code;
  const { rows } = await pool.query('SELECT * FROM shipments WHERE tracking=$1', [code]);
  const s = rows[0] ? rowToShipment(rows[0]) : null;
  const STEPS = ['🍔 Preparando tu pedido','🛵 En camino','📍 A punto de llegar','✅ Entregado'];
  const title = s ? `${s.emoji} ${s.product} — TRAZO` : 'TRAZO · Delivery en tiempo real';
  const desc  = s ? `${STEPS[s.currentStep]||'En proceso'} · Para: ${s.recipient}${s.destName?' · '+s.destName.split(',')[0]:''}` : 'Seguí tu pedido en tiempo real con GPS del repartidor.';
  const liveUrl  = `${req.protocol}://${req.get('host')}/live.html?code=${encodeURIComponent(code)}`;
  const imageUrl = `${req.protocol}://${req.get('host')}/og-image/${encodeURIComponent(code)}`;
  res.send(`<!DOCTYPE html><html lang="es"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title}</title>
<meta property="og:title" content="${title}">
<meta property="og:description" content="${desc}">
<meta property="og:image" content="${imageUrl}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:url" content="${liveUrl}">
<meta property="og:type" content="website">
<meta property="og:site_name" content="TRAZO · Delivery en tiempo real">
<meta name="twitter:card" content="summary_large_image">
<meta http-equiv="refresh" content="0;url=${liveUrl}">
<script>window.location.replace('${liveUrl}');</script>
</head><body style="background:#15182B;color:#fff;font-family:Arial;display:flex;align-items:center;justify-content:center;height:100vh;margin:0">
<p>Redirigiendo al seguimiento...</p></body></html>`);
});

// ─── OG Image PNG ──────────────────────────────────────────────────────────
const { createCanvas, GlobalFonts } = require('@napi-rs/canvas');

const FONT_PATHS = [
  ['/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',         'TrazoBold'],
  ['/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf', 'TrazoBold'],
  ['/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',              'TrazoReg'],
  ['/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf','TrazoReg'],
];
FONT_PATHS.forEach(([p,name]) => { try { if(fs.existsSync(p)) { GlobalFonts.registerFromPath(p,name); console.log('Font:',name,p); } } catch(e){} });

const OG_STEPS  = ['Preparando tu pedido','En camino','A punto de llegar','Entregado'];
const OG_COLORS = ['#C9871E','#FF5A36','#FF5A36','#1F9D6F'];
const ogCache   = new Map();

app.get('/og-image/:code', async (req, res) => {
  const code = req.params.code;
  if (ogCache.has(code)) {
    const { buf, ts } = ogCache.get(code);
    if (Date.now()-ts < 60000) { res.setHeader('Content-Type','image/png'); res.setHeader('Cache-Control','public,max-age=60'); return res.send(buf); }
  }
  const { rows } = await pool.query('SELECT * FROM shipments WHERE tracking=$1',[code]);
  const s         = rows[0] ? rowToShipment(rows[0]) : null;
  const product   = s ? (s.product.length>38?s.product.substring(0,38)+'…':s.product) : 'Tu pedido';
  const step      = s ? (OG_STEPS[s.currentStep]||'En proceso')  : 'Seguimiento en vivo';
  const stepColor = s ? (OG_COLORS[s.currentStep]||'#FF5A36')    : '#FF5A36';
  const recipient = s ? s.recipient : '';
  const dest      = s&&s.destName ? s.destName.split(',')[0] : '';
  try {
    const W=1200, H=630, canvas=createCanvas(W,H), ctx=canvas.getContext('2d');
    const bold = n => `bold ${n}px TrazoBold`;
    const reg  = n => `${n}px TrazoReg`;
    ctx.fillStyle='#15182B'; ctx.fillRect(0,0,W,H);
    ctx.fillStyle='#0D0F1D'; ctx.fillRect(0,0,W,200);
    ctx.fillStyle='#FF5A36'; ctx.fillRect(0,0,12,H);
    ctx.fillStyle='#FF5A36'; ctx.fillRect(60,52,140,30);
    ctx.fillStyle='#ffffff'; ctx.fillRect(110,76,30,106);
    ctx.fillStyle='#FF5A36'; ctx.beginPath(); ctx.arc(220,172,18,0,Math.PI*2); ctx.fill();
    ctx.fillStyle='#0D0F1D'; ctx.beginPath(); ctx.arc(220,172,8,0,Math.PI*2);  ctx.fill();
    ctx.fillStyle='#ffffff'; ctx.font=bold(88); ctx.fillText('TRAZO',258,160);
    ctx.fillStyle='#FF5A36'; ctx.fillRect(258,170,390,6);
    ctx.fillStyle='rgba(255,255,255,0.4)'; ctx.font=reg(22); ctx.fillText('DELIVERY EN TIEMPO REAL',260,196);
    ctx.fillStyle='rgba(255,255,255,0.08)'; ctx.fillRect(60,210,W-120,1);
    ctx.fillStyle='#ffffff'; ctx.font=bold(48); ctx.fillText(product,60,295);
    ctx.fillStyle=stepColor; ctx.beginPath(); ctx.roundRect(60,316,Math.min(step.length*19+60,680),56,28); ctx.fill();
    ctx.fillStyle='#ffffff'; ctx.font=bold(28); ctx.fillText(step,88,354);
    if(recipient){ ctx.fillStyle='rgba(255,255,255,0.6)'; ctx.font=reg(28); ctx.fillText(('Para: '+recipient+(dest?' · '+dest:'')).substring(0,56),60,428); }
    ctx.fillStyle='#FF5A36'; ctx.font=reg(24); ctx.fillText('Código: '+code,60,474);
    ctx.fillStyle='rgba(0,0,0,0.4)'; ctx.fillRect(0,H-64,W,64);
    ctx.fillStyle='rgba(255,255,255,0.45)'; ctx.font=reg(21); ctx.textAlign='center';
    ctx.fillText('Tocá para ver el seguimiento GPS en vivo  ·  trazo-hbrf.onrender.com',W/2,H-22);
    const buf = canvas.toBuffer('image/png');
    ogCache.set(code,{buf,ts:Date.now()});
    res.setHeader('Content-Type','image/png'); res.setHeader('Cache-Control','public,max-age=60'); res.send(buf);
  } catch(e) { console.error('OG error:',e.message); res.status(500).send('Error'); }
});

app.get('/health', (req, res) => res.json({ status:'ok' }));
app.get('*', (req, res) => res.sendFile(path.join(__dirname,'public','index.html')));

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', async () => {
  console.log('Trazo en puerto', PORT);
  try { await initDB(); console.log('DB lista.'); }
  catch(e) { console.error('DB error:', e.message); }
});

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

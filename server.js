const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const { Client, LocalAuth, MessageMedia } = require("whatsapp-web.js");

// Evitar que promesas sin capturar tumben el proceso
process.on('unhandledRejection', (reason, promise) => {
  console.error('[unhandledRejection] Promesa sin capturar:', reason?.message || reason);
  // No re-lanzar — solo loguear
});
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err.message);
  // Solo loguear errores no fatales
});
const qrcode = require("qrcode");
const mysql = require("mysql2/promise");
const axios = require("axios");
const path = require("path");
const fs = require("fs");
const multer = require("multer");

// Multer config for media uploads
const mediaStorage = multer.diskStorage({
  destination: (req, file, cb) => {
    const dir = "/opt/whatsapp-crm/media";
    require("fs").mkdirSync(dir, { recursive: true });
    cb(null, dir);
  },
  filename: (req, file, cb) => {
    const ext = require("path").extname(file.originalname) || ".bin";
    cb(null, Date.now() + "_upload" + ext);
  }
});
const upload = multer({ storage: mediaStorage, limits: { fileSize: 16 * 1024 * 1024 } });


const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(cors());
app.use(express.json({ limit: "50mb" }));

// ═══════════════════════════════════════════════════════════════
// CONFIGURACIÓN
// ═══════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 3001;
const MAX_SESSIONS = 1;
const ALLOWED_SESSION = 'session_1'; // Solo este número está permitido

const dbConfig = {
  host: process.env.DB_HOST || "127.0.0.1",
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER || "rentallplus",
  password: process.env.DB_PASS || "Rp2026secure!",
  database: process.env.DB_NAME || "rentallplus"
};

// ═══════════════════════════════════════════════════════════════
// ESTADO DE SESIONES
// ═══════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════
// RATE LIMITER — Anti-ban inteligente
// Regla: máx 6 destinatarios distintos iniciados por nosotros en 20 min
// Excepción: si el cliente escribió primero → sin límite
// ═══════════════════════════════════════════════════════════════
const OUTBOUND_WINDOW_MS = 20 * 60 * 1000; // 20 minutos
const MAX_OUTBOUND_RECIPIENTS = 6;          // máx destinatarios distintos que iniciamos
// { sessionKey: [{ jid, ts }] }
const outboundLog = {};

// Comprueba si podemos enviar a este jid
// inboundFirst=true → el cliente escribió antes, sin límite
function checkRateLimit(sessionKey, jid, inboundFirst) {
  if (inboundFirst) return { allowed: true, inbound: true };

  const now = Date.now();
  if (!outboundLog[sessionKey]) outboundLog[sessionKey] = [];

  // Limpiar entradas fuera de la ventana
  outboundLog[sessionKey] = outboundLog[sessionKey].filter(e => now - e.ts < OUTBOUND_WINDOW_MS);

  // Contar destinatarios distintos en la ventana
  const uniqueJids = new Set(outboundLog[sessionKey].map(e => e.jid));

  if (uniqueJids.has(jid)) {
    // Ya enviamos a este destinatario en la ventana → permitir (misma conversación)
    return { allowed: true };
  }

  if (uniqueJids.size >= MAX_OUTBOUND_RECIPIENTS) {
    // Cuándo se libera el slot más antiguo
    const oldest = outboundLog[sessionKey].find(e => !uniqueJids.has(e.jid) || e === outboundLog[sessionKey][0]);
    const waitMs = oldest ? (OUTBOUND_WINDOW_MS - (now - oldest.ts)) : OUTBOUND_WINDOW_MS;
    const waitMinutes = Math.ceil(waitMs / 60000);
    return { allowed: false, waitMinutes, count: uniqueJids.size };
  }

  return { allowed: true, count: uniqueJids.size };
}

function logMessage(sessionKey, jid) {
  if (!outboundLog[sessionKey]) outboundLog[sessionKey] = [];
  outboundLog[sessionKey].push({ jid, ts: Date.now() });
}
// ═══════════════════════════════════════════════════════════════
// ANTI-BAN: Humanización de envíos
// ═══════════════════════════════════════════════════════════════

function humanDelay(text) {
  const chars = (text || '').length;
  const base  = Math.min(Math.max(chars * 50, 1500), 6000);
  const jitter = Math.floor(Math.random() * 1200);
  return base + jitter;
}

// isWithinSendHours eliminado — el horario ya no restringe envíos
// El anti-ban se gestiona por destinatarios únicos en ventana de tiempo

async function sendWithHumanBehavior(client, jid, text) {
  await new Promise(r => setTimeout(r, 300 + Math.floor(Math.random() * 700)));
  try { await client.sendPresenceAvailable(); } catch(e) {}
  try { await client.sendStateTyping(jid); } catch(e) {}
  const delay = humanDelay(text);
  await new Promise(r => setTimeout(r, delay));
  const result = await client.sendMessage(jid, text);
  try { await client.clearState(jid); } catch(e) {}
  return result;
}

// ═══════════════════════════════════════════════════════════════
const sessions = {};  // { "session_1": { client, status, phone, qr } }
const emittedOutbound = new Set(); // IDs de mensajes salientes ya emitidos al frontend
const sessionLocks = {};  // Mutex para evitar createSession() concurrentes

function acquireLock(key) {
  if (sessionLocks[key]) return false;
  sessionLocks[key] = true;
  return true;
}
function releaseLock(key) {
  delete sessionLocks[key];
}

// ═══════════════════════════════════════════════════════════════
// BASE DE DATOS
// ═══════════════════════════════════════════════════════════════
let db;

async function initDB() {
  db = await mysql.createPool(dbConfig);
  
  // Crear tablas si no existen
  await db.execute(`
    CREATE TABLE IF NOT EXISTS wa_sessions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      session_key VARCHAR(50) UNIQUE NOT NULL,
      phone VARCHAR(20),
      status ENUM('disconnected', 'connecting', 'connected') DEFAULT 'disconnected',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);
  
  await db.execute(`
    CREATE TABLE IF NOT EXISTS wa_conversations (
      id INT AUTO_INCREMENT PRIMARY KEY,
      session_key VARCHAR(50) NOT NULL,
      remote_jid VARCHAR(100) NOT NULL,
      phone VARCHAR(20),
      name VARCHAR(255),
      booking_id INT,
      last_message TEXT,
      last_message_at TIMESTAMP,
      unread_count INT DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY unique_conv (session_key, remote_jid),
      INDEX idx_booking (booking_id),
      INDEX idx_phone (phone)
    )
  `);
  
  await db.execute(`
    CREATE TABLE IF NOT EXISTS wa_messages (
      id INT AUTO_INCREMENT PRIMARY KEY,
      conversation_id INT NOT NULL,
      message_id VARCHAR(100),
      direction ENUM('in', 'out') NOT NULL,
      type VARCHAR(50) DEFAULT 'text',
      body TEXT,
      media_url VARCHAR(500),
      media_mime VARCHAR(100),
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_conv (conversation_id),
      INDEX idx_timestamp (timestamp)
    )
  `);
  
  await db.execute(`
    CREATE TABLE IF NOT EXISTS wa_config (
      id INT AUTO_INCREMENT PRIMARY KEY,
      config_key VARCHAR(100) UNIQUE NOT NULL,
      config_value TEXT,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS wa_templates (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(200) NOT NULL,
      body TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `);
  
  console.log("✅ Database initialized");
}

// ═══════════════════════════════════════════════════════════════
// CONFIGURACIÓN IA
// ═══════════════════════════════════════════════════════════════
async function getAIConfig() {
  const [rows] = await db.execute(
    "SELECT config_key, config_value FROM wa_config WHERE config_key IN (?, ?, ?, ?)",
    ["ai_provider", "ai_api_key", "ai_model", "ai_prompt"]
  );
  const config = {};
  rows.forEach(r => config[r.config_key] = r.config_value);
  return config;
}

async function callAI(messages, context) {
  const config = await getAIConfig();
  if (!config.ai_api_key) return null;
  
  const systemPrompt = (config.ai_prompt || "Eres un asistente amable.") + 
    (context ? "\n\nContexto del huésped:\n" + JSON.stringify(context) : "");
  
  const fullMessages = [
    { role: "system", content: systemPrompt },
    ...messages
  ];
  
  try {
    if (config.ai_provider === "minimax") {
      const res = await axios.post(
        "https://api.minimaxi.chat/v1/text/chatcompletion_v2",
        {
          model: config.ai_model || "abab6.5s-chat",
          messages: fullMessages
        },
        {
          headers: {
            "Authorization": "Bearer " + config.ai_api_key,
            "Content-Type": "application/json"
          }
        }
      );
      return res.data.choices?.[0]?.message?.content;
    } else {
      // OpenAI compatible
      const res = await axios.post(
        "https://api.openai.com/v1/chat/completions",
        {
          model: config.ai_model || "gpt-4o-mini",
          messages: fullMessages
        },
        {
          headers: {
            "Authorization": "Bearer " + config.ai_api_key,
            "Content-Type": "application/json"
          }
        }
      );
      return res.data.choices?.[0]?.message?.content;
    }
  } catch (err) {
    console.error("AI Error:", err.response?.data || err.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════
// IDENTIFICAR HUÉSPED POR TELÉFONO
// ═══════════════════════════════════════════════════════════════
async function findBookingByPhone(phone, name) {
  const fields = "b.*, a.name as accommodation_name, a.address, " +
    "JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.phone')) as client_phone, " +
    "JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.name')) as client_name, " +
    "JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.email')) as client_email ";
  const fromJoin = "FROM bookings b LEFT JOIN accommodations a ON b.accommodation_id = a.id ";

  // 1) Match por teléfono — últimos 9 dígitos (cubre variaciones internacionales)
  const digits = phone.replace(/\D/g, "");
  if (digits.length >= 7) {
    const suffix = digits.slice(-9);
    const [rows] = await db.execute(
      "SELECT " + fields + fromJoin +
      "WHERE REPLACE(REPLACE(REPLACE(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.phone')), ' ', ''), '+', ''), '-', ''), '.', '') LIKE ? " +
      "AND JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.phone')) NOT IN ('-', '', 'null') " +
      "AND b.status NOT IN ('canceled') " +
      "ORDER BY b.arrival_date DESC LIMIT 1",
      ["%" + suffix]
    );
    if (rows[0]) return rows[0];
  }

  // 2) Fallback: primer nombre + apellido (NUNCA solo primer nombre)
  if (name && name.length > 2 && name !== phone) {
    const parts = name.trim().split(/\s+/).filter(Boolean);
    const first = parts[0];
    const last  = parts[1] || null;
    if (last && first.length >= 3 && last.length >= 3) {
      const [r1] = await db.execute(
        "SELECT " + fields + fromJoin +
        "WHERE JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.name')) LIKE ? " +
        "AND JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.name')) LIKE ? " +
        "AND b.status NOT IN ('canceled') " +
        "ORDER BY b.arrival_date DESC LIMIT 1",
        ["%" + first + "%", "%" + last + "%"]
      );
      if (r1[0]) return r1[0];
    }
  }

  return null; // Sin match → asignación manual
}


async function getBookingTickets(bookingId) {
  const [rows] = await db.execute(`
    SELECT t.*, a.name as action_name, a.color as action_color
    FROM tickets t
    LEFT JOIN actions a ON t.action_id = a.id
    WHERE t.booking_id = ?
    ORDER BY t.date ASC
  `, [bookingId]);
  return rows;
}

// ═══════════════════════════════════════════════════════════════
// GESTIÓN DE SESIONES WHATSAPP
// ═══════════════════════════════════════════════════════════════
async function createSession(sessionKey, autoReconnect = false) {
  // Solo se permite session_1
  if (sessionKey !== ALLOWED_SESSION) {
    console.warn('[createSession] Sesión ' + sessionKey + ' bloqueada — solo ' + ALLOWED_SESSION + ' permitida');
    return { error: 'Only ' + ALLOWED_SESSION + ' is allowed' };
  }
  // Mutex: evitar múltiples llamadas concurrentes para la misma sesión
  if (!acquireLock(sessionKey)) {
    console.log('[createSession] Lock activo para ' + sessionKey + ' — ignorando llamada duplicada');
    return { error: 'Session creation already in progress' };
  }

  try {
  // Si existe pero está desconectado, destruir y recrear
  if (sessions[sessionKey]?.client) {
    const st = sessions[sessionKey].status;
    if (st === "connected" || st === "connecting") {
      return { error: "Session already exists" };
    }
    // Está disconnected en memoria — limpiar para recrear
    try { await sessions[sessionKey].client.destroy(); } catch(e) {}
    if (sessions[sessionKey]?.qrTimer) clearTimeout(sessions[sessionKey].qrTimer);
    delete sessions[sessionKey];
  }
  
  const client = new Client({
    authStrategy: new LocalAuth({ clientId: sessionKey }),
    webVersionCache: {
      type: 'local',
      path: '/opt/whatsapp-crm/.wwebjs_cache'
    },
    webVersion: '2.3000.1041453086',
    puppeteer: {
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
    }
  });
  
  sessions[sessionKey] = { client, status: "connecting", phone: null, qr: null, qrTimer: null };
  
  client.on("qr", async (qr) => {
    // Si es reconexión automática y el auth expiró → NO mostrar QR, desconectar
    if (autoReconnect) {
      console.log('[autoReconnect] Auth expirado para ' + sessionKey + ' — QR requerido. Marcando disconnected.');
      try { await sessions[sessionKey].client.destroy(); } catch(e) {}
      delete sessions[sessionKey];
      releaseLock(sessionKey);
      await updateSessionDB(sessionKey, 'disconnected', null);
      io.emit('session_disconnected', { sessionKey, reason: 'auth_expired' });
      return;
    }
    const qrDataUrl = await qrcode.toDataURL(qr);
    sessions[sessionKey].qr = qrDataUrl;
    sessions[sessionKey].status = "connecting";
    io.emit("session_qr", { sessionKey, qr: qrDataUrl });
    await updateSessionDB(sessionKey, "connecting", null);

    // Auto-reset si no se escanea en 2 minutos
    if (sessions[sessionKey].qrTimer) clearTimeout(sessions[sessionKey].qrTimer);
    sessions[sessionKey].qrTimer = setTimeout(async () => {
      if (sessions[sessionKey]?.status === "connecting") {
        console.log("⏱️ QR timeout para " + sessionKey + " — reseteando a disconnected");
        try { await sessions[sessionKey].client.destroy(); } catch (e) {}
        delete sessions[sessionKey];
        await updateSessionDB(sessionKey, "disconnected", null);
        io.emit("session_disconnected", { sessionKey, reason: "qr_timeout" });
      }
    }, 2 * 60 * 1000); // 2 minutos
  });
  
  // Flag para evitar que "ready" se dispare múltiples veces (bug whatsapp-web.js)
  let _readyFired = false;
  client.on("ready", async () => {
    if (_readyFired) {
      console.log('[ready] Evento duplicado ignorado para ' + sessionKey);
      return;
    }
    _readyFired = true;
    const phone = client.info?.wid?.user || "unknown";
    if (sessions[sessionKey]?.qrTimer) { clearTimeout(sessions[sessionKey].qrTimer); sessions[sessionKey].qrTimer = null; }
    sessions[sessionKey].status = "connected";
    sessions[sessionKey].phone = phone;
    sessions[sessionKey].qr = null;
    io.emit("session_ready", { sessionKey, phone });
    await updateSessionDB(sessionKey, "connected", phone);
    console.log("✅ Session " + sessionKey + " connected: " + phone);
    releaseLock(sessionKey);
    // Actualizar nombres de grupos existentes en BD
    setTimeout(async () => {
      try {
        const chats = await client.getChats();
        for (const chat of chats) {
          if (chat.isGroup && chat.name) {
            const jid = chat.id._serialized;
            await db.execute(
              'UPDATE wa_conversations SET name = ? WHERE session_key = ? AND remote_jid = ?',
              [chat.name, sessionKey, jid]
            ).catch(() => {});
          }
        }
        console.log('[ready] Nombres de grupos sincronizados');

        // Resolver LIDs: contactos con phone >13 dígitos
        const [lidRows] = await db.execute(
          "SELECT id, remote_jid FROM wa_conversations WHERE session_key = ? AND LENGTH(phone) > 13 AND remote_jid LIKE '%@lid'",
          [sessionKey]
        );
        let resolved = 0;
        for (const row of lidRows) {
          try {
            const contact = await client.getContactById(row.remote_jid);
            if (contact?.number && contact.number.length <= 15) {
              const name = contact.pushname || contact.name || null;
              await db.execute(
                'UPDATE wa_conversations SET phone = ?' + (name ? ', name = ?' : '') + ' WHERE id = ?',
                name ? [contact.number, name, row.id] : [contact.number, row.id]
              );
              resolved++;
            }
          } catch(e) { /* contacto no disponible */ }
        }
        if (lidRows.length) console.log('[ready] LIDs resueltos: ' + resolved + '/' + lidRows.length);
      } catch(e) {
        console.warn('[ready] Error sincronizando grupos:', e.message);
      }
    }, 5000);
  });
  
  client.on("disconnected", async (reason) => {
    if (sessions[sessionKey]?.qrTimer) { clearTimeout(sessions[sessionKey].qrTimer); sessions[sessionKey].qrTimer = null; }
    sessions[sessionKey].status = "disconnected";
    sessions[sessionKey].phone = null;
    io.emit("session_disconnected", { sessionKey });
    await updateSessionDB(sessionKey, "disconnected", null);
    console.log("⚠️ Session " + sessionKey + " disconnected, reason: " + reason);

    // Detectar posible ban y alertar por Telegram
    const isBan = reason && (
      String(reason).toUpperCase().includes('BANNED') ||
      String(reason).toUpperCase().includes('CONFLICT') ||
      String(reason).toUpperCase().includes('UNLAUNCHED')
    );
    if (isBan) {
      console.error('[ALERTA BAN] Sesión ' + sessionKey + ' posiblemente baneada. Reason: ' + reason);
      try {
        const axios = require('axios');
        await axios.post('https://api.telegram.org/bot8577028388:AAFRQPMNfuyUqjhAoVHjYCJ5EJCh2sqdoj0/sendMessage', {
          chat_id: '1479879640',
          text: '🚨 *WA CRM RentallPlus*\nSesión `' + sessionKey + '` posiblemente baneada.\nMotivo: `' + reason + '`\nRevisa en: https://wa.rentallplus.com',
          parse_mode: 'Markdown'
        });
      } catch(e) { console.error('[ALERTA BAN] No se pudo enviar Telegram:', e.message); }
    } else {
      // Desconexión normal — notificar igualmente si es fuera de horario o inesperada
      console.log('[disconnect] reason=' + reason + ' (reconexión automática si auth existe)');
    }

    // Auto-reconectar o pedir nuevo QR según el motivo
    const authDir = "/opt/whatsapp-crm/.wwebjs_auth/session-" + sessionKey;
    const _fs = require("fs");
    const isLogout = reason === 'LOGOUT';

    try { await client.destroy(); } catch(e) {}
    delete sessions[sessionKey];
    releaseLock(sessionKey);

    if (isLogout) {
      // LOGOUT = WA invalidó la sesión → borrar auth para no reconectar con credenciales viejas
      console.warn('[LOGOUT] WA anuló sesión ' + sessionKey + '. Borrando auth y esperando nuevo QR.');
      try {
        if (_fs.existsSync(authDir)) {
          _fs.rmSync(authDir, { recursive: true, force: true });
          console.log('[LOGOUT] Auth dir borrado: ' + authDir);
        }
      } catch(e) { console.error('[LOGOUT] Error borrando auth:', e.message); }
      await updateSessionDB(sessionKey, 'disconnected', null);
      io.emit('session_disconnected', { sessionKey, reason: 'logout_manual_qr_needed' });
      // Alerta Telegram
      try {
        const _axios = require('axios');
        await _axios.post('https://api.telegram.org/bot8577028388:AAFRQPMNfuyUqjhAoVHjYCJ5EJCh2sqdoj0/sendMessage', {
          chat_id: '1479879640',
          text: '⚠️ *WA CRM RentallPlus*\nSesión `' + sessionKey + '` cerrada por WhatsApp (LOGOUT).\nNecesita nuevo QR en: https://wa.rentallplus.com/whatsapp/config',
          parse_mode: 'Markdown'
        });
      } catch(e) {}
    } else if (_fs.existsSync(authDir)) {
      // Desconexión inesperada con auth válido → reconectar con delay largo
      const jitter = Math.floor(Math.random() * 60000); // 0-60s extra
      const delay = 120000 + jitter; // 2-3 min mínimo
      console.log("🔄 Auth encontrado, reconectando " + sessionKey + " en " + Math.round(delay/1000) + "s...");
      setTimeout(() => {
        createSession(sessionKey).catch(e => console.error("[autoReconnect] " + e.message));
      }, delay);
    }
  });
  
  client.on("message", async (msg) => {
    await handleIncomingMessage(sessionKey, msg);
  });

  // Capturar mensajes SALIENTES (enviados desde el móvil, WhatsApp Web, etc.)
  client.on("message_create", async (msg) => {
    if (!msg.fromMe) return; // solo salientes, los entrantes los maneja 'message'
    try {
      const chat = await msg.getChat();
      const remoteJid = chat.id._serialized;

      // Buscar conversación existente por jid
      const [convRows] = await db.execute(
        "SELECT * FROM wa_conversations WHERE session_key = ? AND remote_jid = ?",
        [sessionKey, remoteJid]
      );
      if (!convRows[0]) return; // si no hay conversación previa no la creamos

      const conversationId = convRows[0].id;

      // Evitar duplicados: si ya fue emitido desde el endpoint de envío, ignorar
      if (emittedOutbound.has(msg.id._serialized)) {
        emittedOutbound.delete(msg.id._serialized);
        return;
      }

      // También chequear BD por si acaso
      const [existing] = await db.execute(
        "SELECT id FROM wa_messages WHERE message_id = ?",
        [msg.id._serialized]
      );
      if (existing[0]) return;

      await db.execute(
        "INSERT INTO wa_messages (conversation_id, message_id, direction, type, body) VALUES (?, ?, 'out', ?, ?)",
        [conversationId, msg.id._serialized, msg.type, msg.body || '']
      );

      // Actualizar last_message
      await db.execute(
        "UPDATE wa_conversations SET last_message = ?, last_message_at = NOW() WHERE id = ?",
        [(msg.body || '[media]').substring(0, 500), conversationId]
      );

      io.emit("new_message", {
        sessionKey,
        conversationId,
        message: {
          id: msg.id._serialized,
          direction: "out",
          type: msg.type,
          body: msg.body,
          mediaUrl: null,
        },
        conversation: { id: conversationId }
      });
    } catch (e) {
      console.error('[message_create] Error:', e.message);
    }
  });
  
  await client.initialize();
  // Lock se libera en "ready" o en "disconnected"
  return { success: true, sessionKey };
  } catch(err) {
    releaseLock(sessionKey);
    throw err;
  }
}

async function updateSessionDB(sessionKey, status, phone) {
  await db.execute(`
    INSERT INTO wa_sessions (session_key, status, phone)
    VALUES (?, ?, ?)
    ON DUPLICATE KEY UPDATE status = VALUES(status), phone = VALUES(phone)
  `, [sessionKey, status, phone]);
}

// ═══════════════════════════════════════════════════════════════
// MANEJO DE MENSAJES ENTRANTES
// ═══════════════════════════════════════════════════════════════
async function handleIncomingMessage(sessionKey, msg) {
  const remoteJid = msg.from;

  // Ignorar mensajes de estado, broadcast y JIDs del sistema
  if (
    remoteJid === 'status@broadcast' ||
    remoteJid.includes('status@') ||
    remoteJid.includes('broadcast') ||
    remoteJid.startsWith('7000') // lista de estados de WA
  ) return;

  const isGroup = remoteJid.endsWith('@g.us');
  let phone = isGroup ? remoteJid : remoteJid.split('@')[0];
  let contactName = msg._data?.notifyName || phone;

  if (isGroup) {
    // Para grupos: obtener nombre del chat
    try {
      const chat = await msg.getChat();
      if (chat?.name) contactName = chat.name;
    } catch (e) {
      console.warn('[handleIncomingMessage] getChat falló para grupo', remoteJid, e.message);
    }
  } else {
    // Para JIDs @lid (privacidad WA) el split no da el teléfono real
    // Intentar obtenerlo del objeto contacto
    try {
      const contact = await msg.getContact();
      if (contact?.number) {
        phone = contact.number; // Número real sin +
      }
      if (contact?.pushname || contact?.name) {
        contactName = contact.pushname || contact.name || contactName;
      }
    } catch (e) {
      // Si falla, seguir con el fallback
      console.warn('[handleIncomingMessage] getContact falló para', remoteJid, e.message);
    }
  }
  
  // Buscar o crear conversación
  let [convRows] = await db.execute(
    "SELECT * FROM wa_conversations WHERE session_key = ? AND remote_jid = ?",
    [sessionKey, remoteJid]
  );
  
  let conversationId;
  let booking = null;
  
  if (convRows.length === 0) {
    // Buscar booking por teléfono
    booking = await findBookingByPhone(phone, contactName);
    
    try {
      const [result] = await db.execute(`
        INSERT INTO wa_conversations (session_key, remote_jid, phone, name, booking_id, last_message, last_message_at, unread_count)
        VALUES (?, ?, ?, ?, ?, ?, NOW(), 1)
      `, [sessionKey, remoteJid, phone, contactName, booking?.id || null, (msg.body || "[media]").substring(0, 500)]);
      conversationId = result.insertId;
    } catch (insertErr) {
      if (insertErr.code === 'ER_DUP_ENTRY') {
        // Race condition: otro mensaje insertó primero, obtener fila existente
        const [dupRows] = await db.execute(
          "SELECT * FROM wa_conversations WHERE session_key = ? AND remote_jid = ?",
          [sessionKey, remoteJid]
        );
        conversationId = dupRows[0].id;
        await db.execute(`
          UPDATE wa_conversations
          SET last_message = ?, last_message_at = NOW(), unread_count = unread_count + 1, name = ?
          WHERE id = ?
        `, [(msg.body || "[media]").substring(0, 500), contactName, conversationId]);
      } else {
        throw insertErr;
      }
    }
  } else {
    conversationId = convRows[0].id;
    // Si el teléfono guardado es un LID (>12 dígitos sin prefijo de país válido), actualizarlo
    const storedPhone = convRows[0].phone;
    const isLid = storedPhone && storedPhone.length > 13 && storedPhone !== phone;
    if (isLid && phone !== storedPhone) {
      await db.execute('UPDATE wa_conversations SET phone = ? WHERE id = ?', [phone, conversationId]);
      // Intentar vincular reserva ahora que tenemos el tel real
      if (!convRows[0].booking_id) {
        const b = await findBookingByPhone(phone, contactName);
        if (b) await db.execute('UPDATE wa_conversations SET booking_id = ? WHERE id = ?', [b.id, conversationId]);
      }
    }
    booking = convRows[0].booking_id ? await findBookingByPhone(phone, contactName) : null;
    
    await db.execute(`
      UPDATE wa_conversations 
      SET last_message = ?, last_message_at = NOW(), unread_count = unread_count + 1, name = ?
      WHERE id = ?
    `, [(msg.body || "[media]").substring(0, 500), contactName, conversationId]);
  }
  
  // Guardar mensaje
  let mediaUrl = null;
  let mediaMime = null;
  
  if (msg.hasMedia) {
    try {
      const media = await msg.downloadMedia();
      if (media) {
        const ext = media.mimetype.split("/")[1] || "bin";
        const filename = Date.now() + "_" + phone + "." + ext;
        const mediaPath = "/opt/whatsapp-crm/media/" + filename;
        fs.mkdirSync("/opt/whatsapp-crm/media", { recursive: true });
        fs.writeFileSync(mediaPath, media.data, "base64");
        mediaUrl = "/media/" + filename;
        mediaMime = media.mimetype;
      }
    } catch (e) {
      console.error("Media download error:", e.message);
    }
  }
  
  // Resolver @menciones LID → nombre real en el cuerpo del mensaje
  let bodyFinal = msg.body || '';
  try {
    const mentions = await msg.getMentions();
    for (const contact of mentions) {
      const mentionName = contact.pushname || contact.name || contact.number || '';
      if (mentionName) {
        // Reemplazar @LID o @número por @NombreReal
        const lidNum = contact.id?.user || '';
        const realNum = contact.number || lidNum;
        bodyFinal = bodyFinal.replace(new RegExp('@' + lidNum, 'g'), '@' + mentionName);
        if (realNum !== lidNum) bodyFinal = bodyFinal.replace(new RegExp('@' + realNum, 'g'), '@' + mentionName);
      }
    }
  } catch(e) { /* sin menciones o error */ }

  await db.execute(`
    INSERT INTO wa_messages (conversation_id, message_id, direction, type, body, media_url, media_mime)
    VALUES (?, ?, 'in', ?, ?, ?, ?)
  `, [conversationId, msg.id._serialized, msg.type, bodyFinal, mediaUrl, mediaMime]);
  
  // Auto-adjuntar media WA al ticket de check-in (action_id=3)
  if (mediaUrl && booking) {
    try {
      const [checkinTickets] = await db.execute(
        "SELECT id FROM tickets WHERE booking_id = ? AND action_id = 3 ORDER BY id DESC LIMIT 1",
        [booking.id]
      );
      if (checkinTickets[0]) {
        const comment = "[WA Media] " + (msg.body || mediaMime || "archivo");
        const meta = JSON.stringify({ type: "wa_media", url: mediaUrl, mime: mediaMime, from: phone });
        await db.execute(
          "INSERT INTO comments (ticket_id, message, metadata, created_at, updated_at) VALUES (?, ?, ?, NOW(), NOW())",
          [checkinTickets[0].id, comment, meta]
        );
        console.log("[auto-attach] Media adjuntada al ticket check-in", checkinTickets[0].id);
      }
    } catch (e) { console.error("[auto-attach] Error:", e.message); }
  }

  // Emitir a frontend
  const tickets = booking ? await getBookingTickets(booking.id) : [];
  
  io.emit("new_message", {
    sessionKey,
    conversationId,
    message: {
      id: msg.id._serialized,
      direction: "in",
      type: msg.type,
      body: msg.body,
      mediaUrl,
      mediaMime,
      timestamp: new Date()
    },
    conversation: {
      id: conversationId,
      phone,
      name: contactName,
      booking,
      tickets
    }
  });
}

// ═══════════════════════════════════════════════════════════════
// API ROUTES
// ═══════════════════════════════════════════════════════════════

// Estado de sesiones
app.get("/sessions", async (req, res) => {
  const [rows] = await db.execute("SELECT * FROM wa_sessions ORDER BY id");
  const result = rows.map(r => ({
    ...r,
    qr: sessions[r.session_key]?.qr || null
  }));
  res.json(result);
});

// Crear/conectar sesión — solo session_1 permitida
app.post("/sessions/:key/connect", async (req, res) => {
  const { key } = req.params;
  if (key !== ALLOWED_SESSION) {
    return res.status(403).json({ error: "Solo se permite " + ALLOWED_SESSION + ". No se pueden añadir más sesiones." });
  }
  const result = await createSession(key);
  res.json(result);
});

// Desconectar sesión
app.post("/sessions/:key/disconnect", async (req, res) => {
  const { key } = req.params;
  if (sessions[key]?.client) {
    await sessions[key].client.destroy();
    delete sessions[key];
    await updateSessionDB(key, "disconnected", null);
  }
  res.json({ success: true });
});

// Conversaciones
app.get("/conversations", async (req, res) => {
  try {
    const { session, search } = req.query;
    const limit = search ? 50 : Math.max(1, Math.min(100, parseInt(req.query.limit) || 20));
    const offset = search ? 0 : Math.max(0, parseInt(req.query.offset) || 0);

    let query = `
      SELECT c.*, b.arrival_date, b.departure_date, a.name as accommodation_name
      FROM wa_conversations c
      LEFT JOIN bookings b ON c.booking_id = b.id
      LEFT JOIN accommodations a ON b.accommodation_id = a.id
    `;
    const conditions = [];
    const params = [];

    if (session && session !== "all") {
      conditions.push("c.session_key = ?");
      params.push(session);
    }

    if (search && search.trim()) {
      const s = `%${search.trim()}%`;
      conditions.push("(c.name LIKE ? OR c.phone LIKE ? OR a.name LIKE ?)");
      params.push(s, s, s);
    }

    // Excluir siempre JIDs de estado/broadcast
    conditions.push("c.remote_jid NOT LIKE '%status%' AND c.remote_jid NOT LIKE '%broadcast%' AND c.phone NOT LIKE '7000%'");

    if (conditions.length) query += " WHERE " + conditions.join(" AND ");
    query += " ORDER BY c.last_message_at DESC LIMIT " + limit + " OFFSET " + offset;

    const [rows] = await db.execute(query, params);
    res.json(rows);
  } catch (err) {
    console.error('[conversations] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Mensajes de una conversación
app.get("/conversations/:id/messages", async (req, res) => {
  try {
    const id = parseInt(req.params.id) || 0;
    const limitVal = Math.max(1, Math.min(100, parseInt(req.query.limit) || 50));
    const before = req.query.before ? parseInt(req.query.before) : null;
    
    let query = "SELECT * FROM wa_messages WHERE conversation_id = " + id;
    if (before) {
      query += " AND id < " + before;
    }
    query += " ORDER BY id DESC LIMIT " + limitVal;
    
    const [rows] = await db.execute(query);
    
    // Marcar como leídos
    await db.execute("UPDATE wa_conversations SET unread_count = 0 WHERE id = " + id);
    
    res.json(rows.reverse());
  } catch (err) {
    console.error('[messages] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Info de reserva para una conversación
app.get("/conversations/:id/booking", async (req, res) => {
  const { id } = req.params;
  
  const [conv] = await db.execute("SELECT * FROM wa_conversations WHERE id = ?", [id]);
  if (!conv[0]) return res.status(404).json({ error: "Not found" });
  
  if (!conv[0].booking_id) {
    // Intentar buscar por teléfono y por nombre
    const booking = await findBookingByPhone(conv[0].phone, conv[0].name);
    if (booking) {
      await db.execute("UPDATE wa_conversations SET booking_id = ? WHERE id = ?", [booking.id, id]);
      const tickets = await getBookingTickets(booking.id);
      return res.json({ booking, tickets });
    }
    return res.json({ booking: null, tickets: [] });
  }
  
  const [bookings] = await db.execute(`
    SELECT b.*, a.name as accommodation_name, a.address
    FROM bookings b
    LEFT JOIN accommodations a ON b.accommodation_id = a.id
    WHERE b.id = ?
  `, [conv[0].booking_id]);
  
  const tickets = await getBookingTickets(conv[0].booking_id);
  res.json({ booking: bookings[0] || null, tickets });
});

// Enviar mensaje
app.post("/conversations/:id/send", async (req, res) => {
  const { id } = req.params;
  const { message, useAI } = req.body;
  
  const [conv] = await db.execute("SELECT * FROM wa_conversations WHERE id = ?", [id]);
  if (!conv[0]) return res.status(404).json({ error: "Conversation not found" });
  
  const session = sessions[conv[0].session_key];
  if (!session?.client) return res.status(400).json({ error: "Session not connected" });

  // Detectar si el cliente escribió antes que nosotros (inbound first)
  const [lastMsgs] = await db.execute(
    "SELECT direction FROM wa_messages WHERE conversation_id = ? ORDER BY id DESC LIMIT 1",
    [id]
  );
  const inboundFirst = lastMsgs[0]?.direction === "in";

  // Rate limit solo si somos nosotros quienes iniciamos
  const rateCheck = checkRateLimit(conv[0].session_key, conv[0].remote_jid, inboundFirst);
  if (!rateCheck.allowed) {
    console.log(`[send] RATE LIMITED session=${conv[0].session_key} wait=${rateCheck.waitMinutes}min`);
    return res.status(429).json({
      error: "Espera un momento",
      message: `Por favor espera ${rateCheck.waitMinutes} min antes de escribir a nuevos contactos. Límite: ${MAX_OUTBOUND_RECIPIENTS} conversaciones nuevas cada 20 min.`,
      waitMinutes: rateCheck.waitMinutes
    });
  }
  
  let finalMessage = message;
  
  // Si useAI, generar respuesta con IA
  if (useAI) {
    const [msgs] = await db.execute(
      "SELECT direction, body FROM wa_messages WHERE conversation_id = ? ORDER BY id DESC LIMIT 10",
      [id]
    );
    const history = msgs.reverse().map(m => ({
      role: m.direction === "in" ? "user" : "assistant",
      content: m.body
    }));
    
    let context = null;
    if (conv[0].booking_id) {
      const booking = await findBookingByPhone(conv[0].phone);
      const tickets = await getBookingTickets(conv[0].booking_id);
      context = { booking, tickets };
    }
    
    const aiResponse = await callAI(history, context);
    if (aiResponse) finalMessage = aiResponse;
  }
  
  // Enviar con comportamiento humano (anti-ban)
  if (!session?.client) {
    return res.status(400).json({ error: "Sesión no conectada" });
  }
  let sentMsg;
  try {
    sentMsg = await sendWithHumanBehavior(session.client, conv[0].remote_jid, finalMessage);
  } catch (sendErr) {
    console.error('[sendMessage] Error al enviar:', sendErr.message);
    return res.status(500).json({ error: "Error al enviar mensaje: " + sendErr.message });
  }
  logMessage(conv[0].session_key, conv[0].remote_jid);

  // Guardar en BD con message_id — message_create se encarga del socket emit
  const sentMsgId = sentMsg?.id?._serialized || null;
  await db.execute(`
    INSERT INTO wa_messages (conversation_id, message_id, direction, type, body)
    VALUES (?, ?, 'out', 'text', ?)
  `, [id, sentMsgId, finalMessage]);

  await db.execute(`
    UPDATE wa_conversations SET last_message = ?, last_message_at = NOW() WHERE id = ?
  `, [finalMessage.substring(0, 500), id]);

  // Emitir inmediatamente al frontend para respuesta instantánea
  io.emit("new_message", {
    sessionKey: conv[0].session_key,
    conversationId: parseInt(id),
    message: {
      id: sentMsgId,
      direction: "out",
      type: "text",
      body: finalMessage,
      timestamp: new Date()
    }
  });

  // Marcar como ya emitido para que message_create no lo duplique
  if (sentMsgId) {
    emittedOutbound.add(sentMsgId);
    setTimeout(() => emittedOutbound.delete(sentMsgId), 15000);
  }

  res.json({ success: true, message: finalMessage });
});

// Configuración IA
app.get("/config", async (req, res) => {
  const config = await getAIConfig();
  // No enviar API key completa
  if (config.ai_api_key) {
    config.ai_api_key = config.ai_api_key.substring(0, 8) + "..." + config.ai_api_key.slice(-4);
  }
  res.json(config);
});

app.post("/config", async (req, res) => {
  const { ai_provider, ai_api_key, ai_model, ai_prompt } = req.body;
  
  const updates = [];
  if (ai_provider) updates.push(["ai_provider", ai_provider]);
  if (ai_api_key && !ai_api_key.includes("...")) updates.push(["ai_api_key", ai_api_key]);
  if (ai_model) updates.push(["ai_model", ai_model]);
  if (ai_prompt !== undefined) updates.push(["ai_prompt", ai_prompt]);
  
  for (const [key, value] of updates) {
    await db.execute(`
      INSERT INTO wa_config (config_key, config_value) VALUES (?, ?)
      ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)
    `, [key, value]);
  }
  
  res.json({ success: true });
});

// Test IA
app.post("/config/test-ai", async (req, res) => {
  const response = await callAI([{ role: "user", content: "Hola, esto es una prueba" }], null);
  res.json({ success: !!response, response: response || "Error connecting to AI" });
});

// Crear ticket desde CRM
app.post("/conversations/:id/create-ticket", async (req, res) => {
  try {
    const { id } = req.params;
    const { action_id, notes } = req.body;

    const [conv] = await db.execute("SELECT * FROM wa_conversations WHERE id = ?", [id]);
    if (!conv[0]?.booking_id) return res.status(400).json({ error: "No booking linked" });

    const [booking] = await db.execute("SELECT * FROM bookings WHERE id = ?", [conv[0].booking_id]);
    if (!booking[0]) return res.status(404).json({ error: "Booking not found" });

    // Obtener nombre de la acción para el título
    const [actions] = await db.execute("SELECT name FROM actions WHERE id = ?", [action_id]);
    const actionName = actions[0]?.name || "Ticket";

    // Obtener nombre del alojamiento
    const [accs] = await db.execute("SELECT name FROM accommodations WHERE id = ?", [booking[0].accommodation_id]);
    const accName = accs[0]?.name || "";

    const title = `${actionName}: ${accName} (WhatsApp)`;
    const description = notes || "Creado desde WhatsApp CRM";

    const [result] = await db.execute(`
      INSERT INTO tickets (title, booking_id, accommodation_id, action_id, status, description, date, created_at, updated_at)
      VALUES (?, ?, ?, ?, 'pending', ?, NOW(), NOW(), NOW())
    `, [title, booking[0].id, booking[0].accommodation_id, action_id, description]);

    res.json({ success: true, ticket_id: result.insertId, title });
  } catch (err) {
    console.error('[create-ticket] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Asignar reserva manualmente por localizador o código
app.post("/conversations/:id/assign-booking", async (req, res) => {
  try {
    const { id } = req.params;
    const { localizator } = req.body; // localizador o código de reserva
    if (!localizator) return res.status(400).json({ error: "Localizator requerido" });

    const [conv] = await db.execute("SELECT * FROM wa_conversations WHERE id = ?", [id]);
    if (!conv[0]) return res.status(404).json({ error: "Conversación no encontrada" });

    // Buscar por localizator o code
    const search = localizator.trim();
    const [bookings] = await db.execute(
      "SELECT b.*, a.name as accommodation_name, " +
      "JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.name')) as client_name, " +
      "JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.phone')) as client_phone " +
      "FROM bookings b LEFT JOIN accommodations a ON b.accommodation_id = a.id " +
      "WHERE b.localizator = ? OR b.code = ? LIMIT 1",
      [search, search]
    );

    if (!bookings[0]) return res.status(404).json({ error: "Reserva no encontrada con ese localizador" });

    const booking = bookings[0];
    await db.execute(
      "UPDATE wa_conversations SET booking_id = ? WHERE id = ?",
      [booking.id, id]
    );

    res.json({ success: true, booking });
  } catch (err) {
    console.error('[assign-booking] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Desvincular reserva de una conversación
app.post("/conversations/:id/unassign-booking", async (req, res) => {
  try {
    const { id } = req.params;
    await db.execute("UPDATE wa_conversations SET booking_id = NULL WHERE id = ?", [id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Enviar media (imagen, audio, documento)
app.post("/conversations/:id/send-media", upload.single("file"), async (req, res) => {
  try {
    const { id } = req.params;
    console.log(`[send-media] received: conv=${id} file=${req.file?.originalname} mime=${req.file?.mimetype} size=${req.file?.size}`);
    const [conv] = await db.execute("SELECT * FROM wa_conversations WHERE id = ?", [id]);
    if (!conv[0]) return res.status(404).json({ error: "Conversation not found" });

    const session = sessions[conv[0].session_key];
    if (!session?.client) return res.status(400).json({ error: "Session not connected" });

    if (!req.file) return res.status(400).json({ error: "No file provided" });

    // Rate limit — misma lógica: libre si el cliente escribió primero
    const [lastMsgsMedia] = await db.execute(
      "SELECT direction FROM wa_messages WHERE conversation_id = ? ORDER BY id DESC LIMIT 1",
      [conv[0].id]
    );
    const inboundFirstMedia = lastMsgsMedia[0]?.direction === "in";
    const rateCheck = checkRateLimit(conv[0].session_key, conv[0].remote_jid, inboundFirstMedia);
    if (!rateCheck.allowed) {
      fs.unlinkSync(req.file.path);
      return res.status(429).json({
        error: "Espera un momento",
        message: "Por favor espera " + rateCheck.waitMinutes + " min antes de escribir a nuevos contactos.",
        waitMinutes: rateCheck.waitMinutes
      });
    }

    let mimeType = req.file.mimetype || "application/octet-stream";
    const filename = req.file.originalname || req.file.filename;

    const fileData = fs.readFileSync(req.file.path);
    const base64Data = fileData.toString("base64");
    const media = new MessageMedia(mimeType, base64Data, filename);

    // Caption opcional
    const caption = req.body.caption || undefined;
    const sendOptions = {};
    if (caption) sendOptions.caption = caption;

    // Nota de voz: solo si es audio OGG real (no webm del browser)
    const isOggAudio = mimeType === "audio/ogg" || mimeType.startsWith("audio/ogg;");
    if (isOggAudio) sendOptions.sendAudioAsVoice = true;

    // Video como documento si no es mp4/3gp nativo
    if (mimeType.startsWith("video/") && !mimeType.includes("mp4") && !mimeType.includes("3gp")) {
      sendOptions.sendMediaAsDocument = true;
    }

    await session.client.sendMessage(conv[0].remote_jid, media, sendOptions);
    logMessage(conv[0].session_key, conv[0].remote_jid);

    // Guardar en BD
    const mediaUrl = "/media/" + req.file.filename;
    const [result] = await db.execute(`
      INSERT INTO wa_messages (conversation_id, direction, type, body, media_url, media_mime)
      VALUES (?, 'out', ?, ?, ?, ?)
    `, [id, mimeType.split("/")[0], caption || media.filename || "", mediaUrl, mimeType]);

    await db.execute(`
      UPDATE wa_conversations SET last_message = ?, last_message_at = NOW() WHERE id = ?
    `, [`[${mimeType.split("/")[0]}]`, conv[0].id]);

    const [rows] = await db.execute("SELECT * FROM wa_messages WHERE id = ?", [result.insertId]);
    const savedMsg = rows[0];

    io.emit("new_message", {
      conversationId: Number(id),
      message: savedMsg
    });

    res.json({ success: true, message: savedMsg });
  } catch (err) {
    console.error("[send-media] Error:", err.message || err);
    console.error("[send-media] File info:", req.file?.originalname, req.file?.mimetype, req.file?.size);
    if (req.file?.path) {
      try { fs.unlinkSync(req.file.path); } catch {}
    }
    res.status(500).json({ error: "Error enviando media: " + err.message });
  }
});

// Servir media
app.use("/media", express.static("/opt/whatsapp-crm/media"));

// ═══════════════════════════════════════════════════════════════
// TRADUCCIÓN
// ═══════════════════════════════════════════════════════════════
app.post("/translate", async (req, res) => {
  try {
    const { text, targetLang, detectOnly } = req.body;
    if (!text?.trim()) return res.json({ translated: text, detectedLang: null });

    const config = await getAIConfig();
    if (!config.ai_api_key) return res.status(400).json({ error: "IA no configurada" });

    const systemPrompt = detectOnly
      ? `Detect the language of the following text. Reply ONLY with the ISO 639-1 language code (e.g. en, fr, de, it, ar, zh, ja, pt, ru). Nothing else.`
      : `You are a professional translator. Translate the following text to ${targetLang || 'Spanish (es)'}. Reply ONLY with the translated text, no explanations, no quotes.`;

    const messages = [
      { role: "system", content: systemPrompt },
      { role: "user", content: text }
    ];

    let result = null;
    if (config.ai_provider === "minimax") {
      const r = await axios.post("https://api.minimaxi.chat/v1/text/chatcompletion_v2",
        { model: config.ai_model || "abab6.5s-chat", messages },
        { headers: { "Authorization": "Bearer " + config.ai_api_key, "Content-Type": "application/json" } }
      );
      result = r.data.choices?.[0]?.message?.content;
    } else {
      const r = await axios.post("https://api.openai.com/v1/chat/completions",
        { model: config.ai_model || "gpt-4o-mini", messages },
        { headers: { "Authorization": "Bearer " + config.ai_api_key, "Content-Type": "application/json" } }
      );
      result = r.data.choices?.[0]?.message?.content;
    }

    if (detectOnly) {
      return res.json({ detectedLang: result?.trim()?.toLowerCase()?.substring(0, 5) || null });
    }
    res.json({ translated: result?.trim() || text });
  } catch (err) {
    console.error("[translate]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Debug: ver info de contacto por JID
app.get("/debug/contact/:jid", async (req, res) => {
  try {
    const { jid } = req.params;
    const session = Object.values(sessions).find(s => s.client);
    if (!session?.client) return res.status(400).json({ error: "No hay sesión conectada" });
    const contact = await session.client.getContactById(jid + "@lid");
    res.json({
      id: contact?.id,
      number: contact?.number,
      pushname: contact?.pushname,
      name: contact?.name,
      shortName: contact?.shortName,
      isMyContact: contact?.isMyContact,
      serialized: contact?.id?._serialized,
      user: contact?.id?.user,
      server: contact?.id?.server,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Re-resolver teléfonos LID en conversaciones existentes
app.post("/conversations/resolve-phones", async (req, res) => {
  try {
    const [convs] = await db.execute("SELECT * FROM wa_conversations WHERE booking_id IS NULL");
    let resolved = 0;
    for (const conv of convs) {
      const session = sessions[conv.session_key];
      if (!session?.client) continue;
      try {
        const contact = await session.client.getContactById(conv.remote_jid);
        if (contact?.number && contact.number !== conv.phone) {
          const booking = await findBookingByPhone(contact.number);
          await db.execute(
            'UPDATE wa_conversations SET phone = ?, booking_id = ?, name = ? WHERE id = ?',
            [contact.number, booking?.id || null, contact.pushname || contact.name || conv.name, conv.id]
          );
          if (booking) resolved++;
        }
      } catch(e) { /* contacto no disponible */ }
    }
    res.json({ success: true, resolved });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Actualizar tag de conversación
app.patch("/conversations/:id/tag", async (req, res) => {
  try {
    const { id } = req.params;
    const { tag } = req.body; // 'pre-stay' | 'during-stay' | 'post-stay' | null
    const allowed = [null, 'pre-stay', 'during-stay', 'post-stay'];
    if (!allowed.includes(tag)) {
      return res.status(400).json({ error: "Tag inválido" });
    }
    await db.execute("UPDATE wa_conversations SET tag = ? WHERE id = ?", [tag, id]);
    const [rows] = await db.execute("SELECT * FROM wa_conversations WHERE id = ?", [id]);
    if (!rows[0]) return res.status(404).json({ error: "Conversación no encontrada" });
    io.emit("conversation_updated", rows[0]);
    res.json({ success: true, conversation: rows[0] });
  } catch (err) {
    console.error("[tag] Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// PLANTILLAS
// ═══════════════════════════════════════════════════════════════

// GET todas las plantillas
app.get("/templates", async (req, res) => {
  try {
    const [rows] = await pool.query("SELECT * FROM wa_templates ORDER BY name ASC");
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST crear plantilla
app.post("/templates", async (req, res) => {
  try {
    const { name, body } = req.body;
    if (!name || !body) return res.status(400).json({ error: "name y body requeridos" });
    const [result] = await pool.query("INSERT INTO wa_templates (name, body) VALUES (?, ?)", [name, body]);
    const [rows] = await pool.query("SELECT * FROM wa_templates WHERE id = ?", [result.insertId]);
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT actualizar plantilla
app.put("/templates/:id", async (req, res) => {
  try {
    const { name, body } = req.body;
    await pool.query("UPDATE wa_templates SET name = ?, body = ? WHERE id = ?", [name, body, req.params.id]);
    const [rows] = await pool.query("SELECT * FROM wa_templates WHERE id = ?", [req.params.id]);
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE plantilla
app.delete("/templates/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM wa_templates WHERE id = ?", [req.params.id]);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════
// INICIAR SERVIDOR
// ═══════════════════════════════════════════════════════════════
async function start() {
  await initDB();
  
  // Reconectar sesiones que tengan auth guardada
  const authDir = '/opt/whatsapp-crm/.wwebjs_auth';
  const fs = require('fs');
  if (fs.existsSync(authDir)) {
    const sessionDirs = fs.readdirSync(authDir).filter(d => d === 'session-' + ALLOWED_SESSION);
    for (const dir of sessionDirs) {
      const sessionKey = dir.replace('session-', '');
      console.log("🔄 Reconnecting " + sessionKey + " (auth found)...");
      createSession(sessionKey).catch(console.error);
    }
  }
  
  server.listen(PORT, () => {
    console.log("🚀 WhatsApp CRM running on port " + PORT);
  });
}

start().catch(console.error);


// ═══════════════════════════════════════════════════════════════
// TICKET COMMENTS
// ═══════════════════════════════════════════════════════════════
app.get('/tickets/:id/comments', async (req, res) => {
  try {
    const { id } = req.params;
    const [rows] = await db.execute(
      'SELECT * FROM comments WHERE ticket_id = ? AND deleted_at IS NULL ORDER BY created_at ASC',
      [id]
    );
    res.json(rows);
  } catch (err) {
    console.error('[comments] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/tickets/:id/comments', async (req, res) => {
  try {
    const { id } = req.params;
    const { message, metadata } = req.body;
    if (!message) return res.status(400).json({ error: 'message required' });
    await db.execute(
      'INSERT INTO comments (ticket_id, message, metadata, created_at, updated_at) VALUES (?, ?, ?, NOW(), NOW())',
      [id, message, metadata ? JSON.stringify(metadata) : null]
    );
    const [rows] = await db.execute(
      'SELECT * FROM comments WHERE ticket_id = ? AND deleted_at IS NULL ORDER BY created_at ASC',
      [id]
    );
    res.json({ success: true, comments: rows });
  } catch (err) {
    console.error('[comments post] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

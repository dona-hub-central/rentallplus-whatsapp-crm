const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const { Client, LocalAuth, MessageMedia } = require("whatsapp-web.js");
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
const MAX_SESSIONS = 3;

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
// RATE LIMITER - Max 8 mensajes por hora por sesión
// ═══════════════════════════════════════════════════════════════
const RATE_LIMIT = 8;
const RATE_WINDOW = 60 * 60 * 1000;
const messageLog = {};

function checkRateLimit(sessionKey) {
  const now = Date.now();
  if (!messageLog[sessionKey]) messageLog[sessionKey] = [];
  messageLog[sessionKey] = messageLog[sessionKey].filter(ts => now - ts < RATE_WINDOW);
  if (messageLog[sessionKey].length >= RATE_LIMIT) {
    const waitTime = Math.ceil((RATE_WINDOW - (now - messageLog[sessionKey][0])) / 60000);
    return { allowed: false, waitMinutes: waitTime, count: messageLog[sessionKey].length };
  }
  return { allowed: true, count: messageLog[sessionKey].length };
}

function logMessage(sessionKey) {
  if (!messageLog[sessionKey]) messageLog[sessionKey] = [];
  messageLog[sessionKey].push(Date.now());
}
// ═══════════════════════════════════════════════════════════════
const sessions = {};  // { "session_1": { client, status, phone, qr } }

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
async function findBookingByPhone(phone) {
  // Limpiar número - quedarnos con últimos 9 dígitos
  const cleanPhone = phone.replace(/\D/g, "").slice(-9);
  
  const [rows] = await db.execute(`
    SELECT b.*, a.name as accommodation_name, a.address,
           JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.phone')) as client_phone,
           JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.name')) as client_name,
           JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.email')) as client_email
    FROM bookings b
    LEFT JOIN accommodations a ON b.accommodation_id = a.id
    WHERE REPLACE(REPLACE(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(b.client, '$.phone')), ' ', ''), '+', ''), '-', '') LIKE ?
    ORDER BY b.arrival_date DESC
    LIMIT 1
  `, ["%" + cleanPhone]);
  
  return rows[0] || null;
}

async function getBookingTickets(bookingId) {
  const [rows] = await db.execute(`
    SELECT t.*, a.name as action_name, a.color as action_color
    FROM tickets t
    LEFT JOIN actions a ON t.action_id = a.id
    WHERE t.booking_id = ?
    ORDER BY t.scheduled_date ASC
  `, [bookingId]);
  return rows;
}

// ═══════════════════════════════════════════════════════════════
// GESTIÓN DE SESIONES WHATSAPP
// ═══════════════════════════════════════════════════════════════
async function createSession(sessionKey) {
  if (sessions[sessionKey]?.client) {
    return { error: "Session already exists" };
  }
  
  const client = new Client({
    authStrategy: new LocalAuth({ clientId: sessionKey }),
    puppeteer: {
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"]
    }
  });
  
  sessions[sessionKey] = { client, status: "connecting", phone: null, qr: null };
  
  client.on("qr", async (qr) => {
    const qrDataUrl = await qrcode.toDataURL(qr);
    sessions[sessionKey].qr = qrDataUrl;
    sessions[sessionKey].status = "connecting";
    io.emit("session_qr", { sessionKey, qr: qrDataUrl });
    await updateSessionDB(sessionKey, "connecting", null);
  });
  
  client.on("ready", async () => {
    const phone = client.info?.wid?.user || "unknown";
    sessions[sessionKey].status = "connected";
    sessions[sessionKey].phone = phone;
    sessions[sessionKey].qr = null;
    io.emit("session_ready", { sessionKey, phone });
    await updateSessionDB(sessionKey, "connected", phone);
    console.log("✅ Session " + sessionKey + " connected: " + phone);
  });
  
  client.on("disconnected", async () => {
    sessions[sessionKey].status = "disconnected";
    sessions[sessionKey].phone = null;
    io.emit("session_disconnected", { sessionKey });
    await updateSessionDB(sessionKey, "disconnected", null);
  });
  
  client.on("message", async (msg) => {
    await handleIncomingMessage(sessionKey, msg);
  });
  
  await client.initialize();
  return { success: true, sessionKey };
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
  const phone = remoteJid.split("@")[0];
  const contactName = msg._data?.notifyName || phone;
  
  // Buscar o crear conversación
  let [convRows] = await db.execute(
    "SELECT * FROM wa_conversations WHERE session_key = ? AND remote_jid = ?",
    [sessionKey, remoteJid]
  );
  
  let conversationId;
  let booking = null;
  
  if (convRows.length === 0) {
    // Buscar booking por teléfono
    booking = await findBookingByPhone(phone);
    
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
    booking = convRows[0].booking_id ? await findBookingByPhone(phone) : null;
    
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
  
  await db.execute(`
    INSERT INTO wa_messages (conversation_id, message_id, direction, type, body, media_url, media_mime)
    VALUES (?, ?, 'in', ?, ?, ?, ?)
  `, [conversationId, msg.id._serialized, msg.type, msg.body, mediaUrl, mediaMime]);
  
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

// Crear/conectar sesión
app.post("/sessions/:key/connect", async (req, res) => {
  const { key } = req.params;
  if (!key.match(/^session_[1-3]$/)) {
    return res.status(400).json({ error: "Invalid session key" });
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
    const { session } = req.query;
    const limit = Math.max(1, Math.min(100, parseInt(req.query.limit) || 20));
    const offset = Math.max(0, parseInt(req.query.offset) || 0);
    
    let query = `
      SELECT c.*, b.arrival_date, b.departure_date, a.name as accommodation_name
      FROM wa_conversations c
      LEFT JOIN bookings b ON c.booking_id = b.id
      LEFT JOIN accommodations a ON b.accommodation_id = a.id
    `;
    const params = [];
    
    if (session && session !== "all") {
      query += " WHERE c.session_key = ?";
      params.push(session);
    }
    
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
    // Intentar buscar por teléfono
    const booking = await findBookingByPhone(conv[0].phone);
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

  // Rate limit check
  const rateCheck = checkRateLimit(conv[0].session_key);
  if (!rateCheck.allowed) {
    console.log(`[send] RATE LIMITED session=${conv[0].session_key} wait=${rateCheck.waitMinutes}min`);
    return res.status(429).json({
      error: "Límite alcanzado",
      message: `Máximo ${RATE_LIMIT} mensajes/hora. Espera ${rateCheck.waitMinutes} min.`,
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
  
  // Enviar por WhatsApp
  await session.client.sendMessage(conv[0].remote_jid, finalMessage);
  logMessage(conv[0].session_key);
  
  // Guardar en BD
  await db.execute(`
    INSERT INTO wa_messages (conversation_id, direction, type, body)
    VALUES (?, 'out', 'text', ?)
  `, [id, finalMessage]);
  
  await db.execute(`
    UPDATE wa_conversations SET last_message = ?, last_message_at = NOW() WHERE id = ?
  `, [finalMessage.substring(0, 500), id]);
  
  io.emit("new_message", {
    sessionKey: conv[0].session_key,
    conversationId: parseInt(id),
    message: {
      direction: "out",
      type: "text",
      body: finalMessage,
      timestamp: new Date()
    }
  });
  
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
  const { id } = req.params;
  const { action_id, notes } = req.body;
  
  const [conv] = await db.execute("SELECT * FROM wa_conversations WHERE id = ?", [id]);
  if (!conv[0]?.booking_id) return res.status(400).json({ error: "No booking linked" });
  
  const [booking] = await db.execute("SELECT * FROM bookings WHERE id = ?", [conv[0].booking_id]);
  if (!booking[0]) return res.status(404).json({ error: "Booking not found" });
  
  await db.execute(`
    INSERT INTO tickets (booking_id, accommodation_id, action_id, status, notes, created_at, updated_at)
    VALUES (?, ?, ?, 'pending', ?, NOW(), NOW())
  `, [booking[0].id, booking[0].accommodation_id, action_id, notes || "Creado desde WhatsApp CRM"]);
  
  res.json({ success: true });
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

    // Rate limit check
    const rateCheck = checkRateLimit(conv[0].session_key);
    if (!rateCheck.allowed) {
      fs.unlinkSync(req.file.path);
      return res.status(429).json({
        error: "Límite alcanzado",
        message: `Máximo ${RATE_LIMIT} mensajes/hora. Espera ${rateCheck.waitMinutes} min.`,
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
    logMessage(conv[0].session_key);

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
// INICIAR SERVIDOR
// ═══════════════════════════════════════════════════════════════
async function start() {
  await initDB();
  
  // Reconectar sesiones que tengan auth guardada
  const authDir = '/opt/whatsapp-crm/.wwebjs_auth';
  const fs = require('fs');
  if (fs.existsSync(authDir)) {
    const sessionDirs = fs.readdirSync(authDir).filter(d => d.startsWith('session-'));
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


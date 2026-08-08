require("dotenv").config();
const express  = require("express");
const http     = require("http");
const { Server } = require("socket.io");
const { Pool } = require("pg");
const fs       = require("fs");

const app    = express();
const server = http.createServer(app);
const io     = new Server(server);

app.use(express.static("public"));

// ── PostgreSQL ───────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id       SERIAL PRIMARY KEY,
      username TEXT NOT NULL,
      message  TEXT NOT NULL,
      time     TEXT NOT NULL
    )
  `);
  console.log("DB初期化完了");
}

// ── 管理者ログ ───────────────────────────────────
const LOG_PATH = process.env.LOG_PATH || "admin.log";
function writeAdminLog(entry) {
  const line = `[${new Date().toISOString()}] ${entry}\n`;
  fs.appendFile(LOG_PATH, line, () => {});
  console.log("[ADMIN]", entry);
}

// ── 接続中ユーザー管理（メモリ） ─────────────────
// socketId → { username, isAdmin }
const activeUsers = new Map();

function takenNames() {
  const names = new Set();
  activeUsers.forEach(u => names.add(u.username));
  return names;
}

function broadcastUsers() {
  const list = Array.from(activeUsers.values()).map(u => u.username);
  io.emit("user list", list);
  io.emit("user count", list.length);
}

// ── レートリミット ─────────────────────────────
const rateLimitMap = new Map();
const RATE_LIMIT  = 3;
const RATE_WINDOW = 1000;

function checkRateLimit(socketId) {
  const now   = Date.now();
  const times = (rateLimitMap.get(socketId) || []).filter(t => now - t < RATE_WINDOW);
  if (times.length >= RATE_LIMIT) return false;
  times.push(now);
  rateLimitMap.set(socketId, times);
  return true;
}

// ── Typing indicator ──────────────────────────
const typingTimers = new Map();

function clearTyping(socket) {
  if (typingTimers.has(socket.id)) {
    clearTimeout(typingTimers.get(socket.id));
    typingTimers.delete(socket.id);
  }
  if (socket.username) {
    socket.broadcast.emit("typing stop", socket.username);
  }
}

// ── Socket.IO ─────────────────────────────────
io.on("connection", (socket) => {
  socket.emit("user list", Array.from(activeUsers.values()).map(u => u.username));
  socket.emit("user count", activeUsers.size);

  // ① 入室
  socket.on("join", async ({ username, password }) => {
    const name = (username || "").trim().slice(0, 20) || "名無し";

    if (takenNames().has(name)) {
      socket.emit("join result", { ok: false, reason: `「${name}」はすでに使われています` });
      return;
    }

    const isAdmin = !!process.env.ADMIN_PASSWORD && password === process.env.ADMIN_PASSWORD;
    socket.username = name;
    socket.isAdmin  = isAdmin;
    activeUsers.set(socket.id, { username: name, isAdmin });
    broadcastUsers();
    socket.emit("join result", { ok: true, isAdmin });

    // 過去ログ送信後に入室通知（→ 入室通知が一番下に来る）
    try {
      const { rows } = await pool.query(`SELECT * FROM messages ORDER BY id ASC`);
      rows.forEach(row => socket.emit("chat message", row));
      io.emit("system message", { message: `${name} が入室しました` });
    } catch (e) { console.error(e); }
  });

  // ② チャットメッセージ
  socket.on("chat message", async ({ message }) => {
    if (!socket.username) return;

    if (!checkRateLimit(socket.id)) {
      socket.emit("system message", { message: "送信が速すぎます。少し待ってください" });
      return;
    }

    const text = (message || "").trim().slice(0, 500);
    if (!text) return;

    clearTyping(socket);

    // /clear
    if (text === "/clear") {
      if (!socket.isAdmin) { socket.emit("system message", { message: "管理者のみ使用できます" }); return; }
      await pool.query(`DELETE FROM messages`);
      writeAdminLog(`${socket.username} が全メッセージを削除`);
      io.emit("clear messages");
      io.emit("system message", { message: "管理者がメッセージを全削除しました" });
      return;
    }

    // /name <新しい名前>
    if (text.startsWith("/name ")) {
      const newName = text.slice(6).trim().slice(0, 20);
      if (!newName) { socket.emit("system message", { message: "名前を入力してください" }); return; }
      if (takenNames().has(newName)) { socket.emit("system message", { message: `「${newName}」はすでに使われています` }); return; }
      const oldName = socket.username;
      socket.username = newName;
      activeUsers.set(socket.id, { username: newName, isAdmin: socket.isAdmin });
      broadcastUsers();
      io.emit("system message", { message: `${oldName} が名前を「${newName}」に変更しました` });
      return;
    }

    // /kick <ユーザー名>
    if (text.startsWith("/kick ")) {
      if (!socket.isAdmin) { socket.emit("system message", { message: "管理者のみ使用できます" }); return; }
      const targetName = text.slice(6).trim();
      let targetSocket = null;
      io.sockets.sockets.forEach(s => { if (s.username === targetName) targetSocket = s; });
      if (!targetSocket) { socket.emit("system message", { message: `「${targetName}」は見つかりません` }); return; }
      writeAdminLog(`${socket.username} が ${targetName} をキック`);
      targetSocket.emit("kicked");
      targetSocket.disconnect(true);
      io.emit("system message", { message: `${targetName} が管理者にキックされました` });
      return;
    }

    // 通常メッセージ
    const time = new Date().toTimeString().slice(0, 5);
    try {
      const { rows } = await pool.query(
        `INSERT INTO messages (username, message, time) VALUES ($1, $2, $3) RETURNING id`,
        [socket.username, text, time]
      );
      io.emit("chat message", { id: rows[0].id, username: socket.username, message: text, time });
    } catch (e) { console.error(e); }
  });

  // ③ Typing indicator
  socket.on("typing start", () => {
    if (!socket.username) return;
    socket.broadcast.emit("typing start", socket.username);
    if (typingTimers.has(socket.id)) clearTimeout(typingTimers.get(socket.id));
    typingTimers.set(socket.id, setTimeout(() => clearTyping(socket), 3000));
  });
  socket.on("typing stop", () => clearTyping(socket));

  // ④ 個別削除（管理者専用）
  socket.on("delete message", async (id) => {
    if (!socket.isAdmin) return;
    try {
      await pool.query(`DELETE FROM messages WHERE id = $1`, [id]);
      writeAdminLog(`${socket.username} がメッセージ ID:${id} を削除`);
      io.emit("delete message", id);
    } catch (e) { console.error(e); }
  });

  // ⑤ 退室ボタン
  socket.on("leave", () => {
    if (!socket.username) return;
    const name = socket.username;
    activeUsers.delete(socket.id);
    broadcastUsers();
    io.emit("system message", { message: `${name} が退室しました` });
    socket.username = null;
    socket.disconnect(true);
  });

  // ⑥ 切断
  socket.on("disconnect", () => {
    clearTyping(socket);
    rateLimitMap.delete(socket.id);
    if (socket.username) {
      activeUsers.delete(socket.id);
      broadcastUsers();
      io.emit("system message", { message: `${socket.username} が退出しました` });
    }
  });
});

// ── 起動 ─────────────────────────────────────
const PORT = process.env.PORT || 3000;

initDB().then(() => {
  server.listen(PORT, () => {
    console.log(`起動中: http://localhost:${PORT}`);
  });
}).catch(err => {
  console.error("DB接続失敗:", err);
  process.exit(1);
});

// ── Graceful shutdown ─────────────────────────
function shutdown() {
  console.log("\nシャットダウン中...");
  server.close(async () => {
    await pool.end();
    console.log("DB接続を閉じました");
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 5000);
}
process.on("SIGINT",  shutdown);
process.on("SIGTERM", shutdown);

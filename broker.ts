#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  AckMessagesRequest,
  Peer,
  PeerStatus,
  Message,
} from "./shared/types.ts";
import { computeSessionId } from "./shared/session.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;

// How long a polled-but-not-acked message stays "in flight" before being
// eligible for retry. Set conservatively — push notifications usually
// deliver within ms, so 60s leaves room for slow LLM consumption.
const POLL_LEASE_SECONDS = 60;

// How long after first poll a never-acked message is force-marked delivered.
// Bounds noise from MCP server clients that don't implement /ack-messages
// (e.g., older subprocess versions during a rollout). Without this, an old
// client's messages would re-poll every POLL_LEASE_SECONDS forever.
const FORCE_DELIVERED_SECONDS = 3600; // 1 hour

// Parse a required positive-integer env override, failing fast with a
// legible message on malformed input. Without this, parseInt("abc")
// returns NaN — which propagates into `new Date(NaN)` throwing
// RangeError inside reapStalePeers at startup (crashing the broker
// boot) and into NaN comparisons in peerStatus (silently marking every
// peer "disconnected"). A typo'd env var should not look like a broker
// bug; fail fast at boot instead.
function positiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(
      `${name} must be a positive integer (got ${JSON.stringify(raw)}); ` +
        `unsetting it falls back to the default ${fallback}.`,
    );
  }
  return n;
}

// A peer is "connected" if its last heartbeat is within this window. The
// MCP server heartbeats every 15s (HEARTBEAT_INTERVAL_MS in server.ts), so
// 45s tolerates 2 missed heartbeats before flipping to "disconnected".
const CONNECTED_WINDOW_SECONDS = positiveIntEnv(
  "CLAUDE_PEERS_CONNECTED_WINDOW_SECONDS",
  45,
);

// A peer is reaped (row + queued messages deleted) once last_seen is older
// than this. Decoupled from PID liveness so a session whose MCP subprocess
// is momentarily down (crash, terminal close, host sleep) stays in the
// roster as "disconnected" and keeps its session_id mapping and queued
// messages, delivering them on resume. A session that never resumes ages
// out after REAP_TTL. Default 10 min = ~40× heartbeat; tune via env.
const REAP_TTL_SECONDS = positiveIntEnv("CLAUDE_PEERS_REAP_TTL_SECONDS", 600);

// Reject an inverted window where the connected window exceeds the reap
// TTL — otherwise a peer could be reaped while its computed status would
// still be "connected", silently dropping live peers from the roster.
if (CONNECTED_WINDOW_SECONDS >= REAP_TTL_SECONDS) {
  throw new Error(
    `CLAUDE_PEERS_CONNECTED_WINDOW_SECONDS (${CONNECTED_WINDOW_SECONDS}) must be ` +
      `strictly less than CLAUDE_PEERS_REAP_TTL_SECONDS (${REAP_TTL_SECONDS}); ` +
        `otherwise connected peers get reaped before ever appearing disconnected.`,
  );
}

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// Idempotent migration: add polled_at column if missing. Tracks when a
// message was last returned to a polling MCP server, so we can retry
// after POLL_LEASE_SECONDS if the LLM ack never arrived. Pre-migration
// behavior of marking delivered=1 on poll caused silent message loss
// when channel-notification pushes failed.
{
  const columns = (db.query("PRAGMA table_info(messages)").all() as { name: string }[]).map(
    (c) => c.name,
  );
  if (!columns.includes("polled_at")) {
    db.run("ALTER TABLE messages ADD COLUMN polled_at TEXT");
    console.error("[claude-peers broker] migration: added polled_at column to messages");
  }
}

// Idempotent migration: add session_id column if missing. Stable
// per-session identity keyed on (pid, cwd, tty), so the same logical
// session keeps the same session_id across MCP subprocess restarts
// and broker restarts. handleRegister uses it to reuse the existing
// peer row's ephemeral `id` on re-registration, fixing the bug where
// cached `to_id` values became stale after a disconnect/resume.
{
  const peerCols = (db.query("PRAGMA table_info(peers)").all() as { name: string }[]).map(
    (c) => c.name,
  );
  if (!peerCols.includes("session_id")) {
    db.run("ALTER TABLE peers ADD COLUMN session_id TEXT NOT NULL DEFAULT ''");
    console.error("[claude-peers broker] migration: added session_id column to peers");
  }
  // Partial UNIQUE index excluding the empty-string default: pre-migration
  // rows and any pathological empty session_id get colliding '' values that
  // must NOT trip the constraint. Real session_ids (always non-empty) get
  // uniqueness enforced, which is the backstop for the handleRegister race.
  db.run(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_peers_session_id ON peers(session_id) WHERE session_id != ''",
  );
}

// Derive a peer's liveness status from its last_seen timestamp.
// "connected" if within CONNECTED_WINDOW_SECONDS, else "disconnected".
// Rows older than REAP_TTL_SECONDS are deleted by reapStalePeers and
// never reach this helper.
function peerStatus(lastSeenIso: string): PeerStatus {
  const ageMs = Date.now() - new Date(lastSeenIso).getTime();
  return ageMs <= CONNECTED_WINDOW_SECONDS * 1000 ? "connected" : "disconnected";
}

// Reap peers whose last_seen is older than REAP_TTL_SECONDS. Replaces the
// old PID-liveness cleanStalePeers, which conflated "MCP subprocess alive"
// (an ephemeral condition) with "session alive" (the durable property we
// actually care about). Time-based reaping decouples the two: a session
// whose subprocess is momentarily down stays in the roster as
// "disconnected" and keeps its session_id mapping + queued messages,
// delivering on resume. Also fixes a latent bug where PID reuse by an
// unrelated local process made a dead peer look "alive" forever.
function reapStalePeers() {
  const cutoff = new Date(Date.now() - REAP_TTL_SECONDS * 1000).toISOString();
  const stale = db.query("SELECT id FROM peers WHERE last_seen < ?").all(cutoff) as
    | { id: string }[]
    | null;
  if (!stale || stale.length === 0) return;
  for (const row of stale) {
    db.run("DELETE FROM peers WHERE id = ?", [row.id]);
    // Drop undelivered messages addressed to the reaped peer. Delivered
    // messages are retained as historical record (same as prior behavior).
    db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [row.id]);
  }
  console.error(
    `[claude-peers broker] reaped ${stale.length} stale peer(s) (last_seen < ${cutoff})`,
  );
}

reapStalePeers();

// Periodically reap stale peers. Interval is the smaller of 30s or
// REAP_TTL_SECONDS/2, so reaping stays responsive even with a very short
setInterval(reapStalePeers, Math.min(30_000, Math.max(1_000, (REAP_TTL_SECONDS * 1000) / 2)));

// Force-deliver messages that have been polled but never acked for too long.
// Protects against old MCP server clients that don't call /ack-messages —
// without this, their messages would loop in the polled-not-acked state
// forever. Runs every 5 minutes.
function forceDeliverStuck() {
  const cutoff = new Date(Date.now() - FORCE_DELIVERED_SECONDS * 1000).toISOString();
  const result = db.run(
    "UPDATE messages SET delivered = 1 WHERE delivered = 0 AND polled_at IS NOT NULL AND polled_at < ?",
    [cutoff],
  );
  if (result.changes > 0) {
    console.error(
      `[claude-peers broker] force-delivered ${result.changes} stuck message(s) (polled > ${FORCE_DELIVERED_SECONDS}s ago)`,
    );
  }
}

forceDeliverStuck();
setInterval(forceDeliverStuck, 300_000); // every 5 min

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, session_id, pid, cwd, git_root, tty, summary, registered_at, last_seen)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

// Re-register an existing session in place: keep the ephemeral `id` stable
// (so cached to_id values survive a subprocess restart) but refresh the
// mutable context fields. Used by handleRegister when session_id matches.
const reRegisterPeer = db.prepare(`
  UPDATE peers SET
    pid = ?,
    cwd = ?,
    git_root = ?,
    tty = ?,
    summary = ?,
    last_seen = ?
    -- registered_at intentionally preserved: a re-register is the same
    -- logical session reconnecting, not a new session, so the original
    -- session-start timestamp stays meaningful to list_peers consumers.
  WHERE session_id = ?
`);

// Resolve a peer by its stable session_id. Used both by handleRegister
// (to detect an existing row to reuse the ephemeral id for) and by
// handleSendMessage (to resolve "session:<session_id>" to_id forms).
const selectIdBySessionId = db.prepare(`
  SELECT id FROM peers WHERE session_id = ?
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

const selectPollable = db.prepare(`
  SELECT * FROM messages
  WHERE to_id = ?
    AND delivered = 0
    AND (polled_at IS NULL OR polled_at < ?)
  ORDER BY sent_at ASC
`);

const markPolled = db.prepare(`
  UPDATE messages SET polled_at = ? WHERE id = ?
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ? AND to_id = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

function handleRegister(body: RegisterRequest): RegisterResponse {
  const session_id =
    body.session_id && body.session_id.length > 0
      ? body.session_id
      : computeSessionId(body.pid, body.cwd, body.tty);
  const now = new Date().toISOString();

  // The lookup+write must be atomic: two concurrent /register calls for
  // the same session_id (broker restart racing an MCP subprocess restart,
  // or two MCP servers that compute the same session_id) could otherwise
  // both observe "no existing row", both mint distinct ephemeral ids,
  // and both insert — producing duplicate session_id rows that make
  // session: routing silently pick the wrong one. The UNIQUE index on
  // session_id (WHERE session_id != '') is the backstop; this transaction
  // makes the happy path not rely on catching a constraint violation.
  return db.transaction(() => {
    // Reuse the existing ephemeral id for this session_id if present.
    // This is the fix for the disconnect/resume identity-change bug: a
    // cached to_id stays valid across the peer's subprocess restarts
    // because the broker hands back the same id. Also clear any stale row
    // registered under the same pid but a *different* session_id (can
    // happen if the session_id derivation changed between versions, or
    // the OS reused a pid for a new session).
    const existingBySession = selectIdBySessionId.get(session_id) as { id: string } | null;
    if (existingBySession) {
      reRegisterPeer.run(
        body.pid,
        body.cwd,
        body.git_root,
        body.tty,
        body.summary,
        now,
        session_id,
      );
      // Also clear any row that shares this pid but a different session_id
      // (pid reuse by a different logical session).
      const staleByPid = db
        .query("SELECT id FROM peers WHERE pid = ? AND session_id != ?")
        .all(body.pid, session_id) as { id: string }[];
      for (const row of staleByPid) {
        deletePeer.run(row.id);
      }
      return { id: existingBySession.id, session_id };
    }

    // No existing session — mint a fresh ephemeral id, but first remove
    // any stale row registered under the same pid (pid reuse). If a
    // concurrent /register already inserted a row with this session_id
    // (race lost), the UNIQUE index on session_id makes insertPeer throw
    // and the caller sees a 500; the next /register retry wins. This is
    // strictly better than the pre-fix silent-duplicate-row behavior.
    const existingByPid = db.query("SELECT id FROM peers WHERE pid = ?").get(body.pid) as
      | { id: string }
      | null;
    if (existingByPid) {
      deletePeer.run(existingByPid.id);
    }

    const id = generateId();
    insertPeer.run(
      id,
      session_id,
      body.pid,
      body.cwd,
      body.git_root,
      body.tty,
      body.summary,
      now,
      now,
    );
    return { id, session_id };
  })();
}

function handleHeartbeat(body: HeartbeatRequest): void {
  updateLastSeen.run(new Date().toISOString(), body.id);
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  // Opportunistically reap before listing so the roster is self-cleaning
  // even if the periodic reaper hasn't ticked yet (e.g. very short TTL
  // in tests, or a broker that just resumed). Cheap: one query.
  reapStalePeers();
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // No PID-liveness filter: a peer whose MCP subprocess is momentarily
  // down (crash, terminal close, host sleep) stays in the roster as
  // "disconnected" and keeps its session_id mapping + queued messages.
  // Rows older than REAP_TTL_SECONDS are removed by reapStalePeers; rows
  // still here are within the reap window and worth showing. Compute
  // status from last_seen recency.
  return peers.map((p) => ({ ...p, status: peerStatus(p.last_seen) }));
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Accept two to_id forms:
  //   1. Ephemeral id  ("ab12cd34")      — direct lookup.
  //   2. Stable handle ("session:<sid>")  — resolve to the current ephemeral
  //      id registered under that session_id. Survives the target's
  //      subprocess restart and broker restart.
  let resolvedToId = body.to_id;
  if (body.to_id.startsWith("session:")) {
    const sid = body.to_id.slice("session:".length);
    if (!sid) {
      return { ok: false, error: "session: handle requires a session_id" };
    }
    const row = selectIdBySessionId.get(sid) as { id: string } | null;
    if (!row) {
      return { ok: false, error: `No peer currently registered for session ${sid}` };
    }
    resolvedToId = row.id;
  }

  // Verify target exists
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(resolvedToId) as
    | { id: string }
    | null;
  if (!target) {
    return { ok: false, error: `Peer ${resolvedToId} not found` };
  }

  insertMessage.run(body.from_id, resolvedToId, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  // Backwards-compat path: legacy clients that don't implement /ack-messages
  // pass ack_supported=undefined. For them we retain the original
  // mark-delivered-on-poll behavior so a broker upgrade doesn't trigger a
  // duplicate-storm against old MCP server subprocesses that outlive it.
  // The silent-loss bug stays present for those clients until they're
  // restarted — but it's no worse than before the fix.
  if (!body.ack_supported) {
    const messages = selectUndelivered.all(body.id) as Message[];
    for (const msg of messages) {
      markDelivered.run(msg.id, body.id);
    }
    return { messages };
  }

  // New ack-aware path: messages stay delivered=0 until the client calls
  // /ack-messages. Within POLL_LEASE_SECONDS of being polled, a message is
  // considered "in flight" and not re-returned. After the lease expires
  // without an ack, the message is re-pollable (at-least-once retry).
  // markPolled + select happens in one transaction so concurrent pollers
  // don't double-deliver.
  const cutoff = new Date(Date.now() - POLL_LEASE_SECONDS * 1000).toISOString();
  const now = new Date().toISOString();

  const messages = db.transaction(() => {
    const msgs = selectPollable.all(body.id, cutoff) as Message[];
    for (const msg of msgs) {
      markPolled.run(now, msg.id);
    }
    return msgs;
  })();

  return { messages };
}

function handleAckMessages(body: AckMessagesRequest): { ok: boolean } {
  if (body.message_ids.length === 0) return { ok: true };

  // Scope the ack to messages addressed to this peer (defense in depth —
  // a buggy or malicious client can't ack-and-delete messages destined for
  // other peers).
  const ack = db.transaction(() => {
    for (const msgId of body.message_ids) {
      markDelivered.run(msgId, body.id);
    }
  });
  ack();

  return { ok: true };
}

function handleUnregister(body: { id: string }): void {
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const body = await req.json();

      switch (path) {
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/ack-messages":
          return Response.json(handleAckMessages(body as AckMessagesRequest));
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);

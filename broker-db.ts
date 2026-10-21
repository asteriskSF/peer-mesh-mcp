/**
 * claude-peers broker database: connection setup, schema, migrations, and
 * the message-table SQL whose query plans matter. Split from broker.ts so
 * tests can open a DB through the same code the broker runs at startup.
 */

import { Database } from "bun:sqlite";

// Open the broker DB and bring its schema up to date. Every statement is
// idempotent, so this runs on every broker start against new and existing
// DBs alike.
export function openBrokerDb(path: string): Database {
  const db = new Database(path);
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

  // Every MCP server polls once a second, so the poll must not scan the
  // table. Partial on delivered = 0: it covers only the pending rows, which
  // is usually none, and ORDER BY sent_at reads straight off the index.
  db.run(
    "CREATE INDEX IF NOT EXISTS idx_messages_pending ON messages(to_id, sent_at) WHERE delivered = 0",
  );

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

  return db;
}

// The two poll queries. `delivered = 0` must stay a literal: SQLite uses a
// partial index only when the query's WHERE implies the index's WHERE.
export const SELECT_UNDELIVERED_SQL = `
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`;

export const SELECT_POLLABLE_SQL = `
  SELECT * FROM messages
  WHERE to_id = ?
    AND delivered = 0
    AND (polled_at IS NULL OR polled_at < ?)
  ORDER BY sent_at ASC
`;

// Delete delivered messages sent before cutoffIso; undelivered rows are
// never touched. Returns the number deleted. After a non-empty prune the
// WAL is checkpointed in TRUNCATE mode, which copies it into the DB and
// resets the WAL file to zero bytes, so the space the pruned rows took in
// the WAL is released. If another connection is reading, the checkpoint
// waits up to busy_timeout and otherwise leaves the WAL for the next prune.
export function pruneDeliveredMessages(db: Database, cutoffIso: string): number {
  const result = db.run("DELETE FROM messages WHERE delivered = 1 AND sent_at < ?", [cutoffIso]);
  if (result.changes > 0) {
    db.run("PRAGMA wal_checkpoint(TRUNCATE)");
  }
  return result.changes;
}

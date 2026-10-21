#!/usr/bin/env bun
/**
 * test-message-retention.ts
 *
 * Verifies the poll index and delivered-message retention:
 *
 *   1. Both poll queries use idx_messages_pending (no table scan, no temp
 *      sort) on a DB built by the broker's own openBrokerDb.
 *   2. openBrokerDb creates the index on an existing DB that lacks it, and
 *      is safe to run repeatedly.
 *   3. pruneDeliveredMessages deletes only delivered rows sent strictly
 *      before the cutoff; undelivered rows are kept however old.
 *   4. A prune that deletes rows truncates the WAL; one that deletes
 *      nothing leaves it alone.
 *   5. A running broker honours CLAUDE_PEERS_MESSAGE_RETENTION_SECONDS on
 *      its periodic timer, and creates the index on an existing DB.
 *      The default retention is 7 days.
 *   6. An invalid CLAUDE_PEERS_MESSAGE_RETENTION_SECONDS fails broker boot.
 *
 * Run: bun test/test-message-retention.ts
 */

import { Database } from "bun:sqlite";
import { existsSync, statSync, unlinkSync } from "node:fs";
import {
  openBrokerDb,
  pruneDeliveredMessages,
  SELECT_POLLABLE_SQL,
  SELECT_UNDELIVERED_SQL,
} from "../broker-db.ts";

const TEST_PORT = 7904;
const STAMP = Date.now();
const UNIT_DB = `/tmp/claude-peers-retention-unit-${STAMP}.db`;
const E2E_DB = `/tmp/claude-peers-retention-e2e-${STAMP}.db`;
const BAD_DB = `/tmp/claude-peers-retention-bad-${STAMP}.db`;
const BROKER_URL = `http://127.0.0.1:${TEST_PORT}`;
const brokerScript = new URL("../broker.ts", import.meta.url).pathname;

let passed = 0;
let failed = 0;
function assert(cond: boolean, msg: string) {
  if (cond) {
    passed++;
    console.log(`  ✓ ${msg}`);
  } else {
    failed++;
    console.error(`  ✗ ${msg}`);
  }
}
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function removeDb(path: string) {
  for (const p of [path, `${path}-wal`, `${path}-shm`]) {
    if (existsSync(p)) {
      try { unlinkSync(p); } catch { /* ignore */ }
    }
  }
}

function walSize(path: string): number {
  return existsSync(`${path}-wal`) ? statSync(`${path}-wal`).size : 0;
}

function hasPendingIndex(db: Database): boolean {
  return (
    db
      .query("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_messages_pending'")
      .all().length === 1
  );
}

function insertMessage(db: Database, toId: string, sentAt: string, delivered: 0 | 1, text = "x") {
  db.run(
    "INSERT INTO messages (from_id, to_id, text, sent_at, delivered) VALUES (?, ?, ?, ?, ?)",
    ["sender", toId, text, sentAt, delivered],
  );
}

function plan(db: Database, sql: string, params: string[]): string {
  return (db.query(`EXPLAIN QUERY PLAN ${sql}`).all(...params) as { detail: string }[])
    .map((r) => r.detail)
    .join(" | ");
}

async function fetchJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status} — ${await res.text()}`);
  return (await res.json()) as T;
}

async function waitForBroker(timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      if ((await fetch(`${BROKER_URL}/health`)).ok) return;
    } catch { /* not up yet */ }
    await sleep(100);
  }
  throw new Error("broker didn't start within timeout");
}

let broker: Bun.Subprocess | null = null;
const dummies: Bun.Subprocess[] = [];

try {
  // --- Test 1: poll queries use the partial index ---
  console.log("[test 1] poll queries use idx_messages_pending");
  {
    const db = openBrokerDb(UNIT_DB);
    const base = Date.now() - 3600_000;
    db.transaction(() => {
      for (let i = 0; i < 3000; i++) {
        insertMessage(db, `peer${i % 20}`, new Date(base + i).toISOString(), 1);
      }
      insertMessage(db, "peer0", new Date().toISOString(), 0);
    })();
    for (const [name, sql, params] of [
      ["pollable", SELECT_POLLABLE_SQL, ["peer0", new Date().toISOString()]],
      ["undelivered", SELECT_UNDELIVERED_SQL, ["peer0"]],
    ] as const) {
      const p = plan(db, sql, [...params]);
      assert(p.includes("USING INDEX idx_messages_pending"), `${name} plan uses the index (${p})`);
      assert(!p.includes("SCAN"), `${name} plan has no table scan`);
      assert(!p.includes("TEMP B-TREE"), `${name} plan has no temp sort for ORDER BY sent_at`);
    }
    let partial = false;
    try {
      db.query("SELECT COUNT(*) FROM messages INDEXED BY idx_messages_pending WHERE to_id = 'peer0'").get();
    } catch {
      partial = true;
    }
    assert(partial, "index is partial: unusable for a query without delivered = 0");
    db.close();
  }

  // --- Test 2: index created idempotently on an existing DB ---
  console.log("[test 2] openBrokerDb adds the index to an existing DB, repeatably");
  {
    let db = openBrokerDb(UNIT_DB);
    db.run("DROP INDEX idx_messages_pending");
    assert(!hasPendingIndex(db), "precondition: index dropped");
    db.close();
    db = openBrokerDb(UNIT_DB);
    assert(hasPendingIndex(db), "reopening recreates the index");
    db.close();
    db = openBrokerDb(UNIT_DB);
    assert(hasPendingIndex(db), "a second reopen leaves exactly one index");
    assert(
      (db.query("SELECT COUNT(*) AS n FROM messages").get() as { n: number }).n === 3001,
      "reopening keeps existing rows",
    );
    db.close();
    removeDb(UNIT_DB);
  }

  // --- Test 3: prune boundaries ---
  console.log("[test 3] prune deletes only delivered rows sent before the cutoff");
  {
    const db = openBrokerDb(UNIT_DB);
    const cutoffMs = Date.parse("2026-10-14T12:00:00.000Z");
    const iso = (ms: number) => new Date(ms).toISOString();
    insertMessage(db, "p", iso(cutoffMs - 1), 1, "delivered-just-older");
    insertMessage(db, "p", iso(cutoffMs), 1, "delivered-at-cutoff");
    insertMessage(db, "p", iso(cutoffMs + 1), 1, "delivered-just-newer");
    insertMessage(db, "p", iso(cutoffMs - 30 * 86400_000), 0, "undelivered-old");
    db.run("UPDATE messages SET polled_at = ? WHERE text = 'undelivered-old'", [
      iso(cutoffMs - 30 * 86400_000),
    ]);

    const pruned = pruneDeliveredMessages(db, iso(cutoffMs));
    const left = (db.query("SELECT text FROM messages ORDER BY id").all() as { text: string }[]).map(
      (r) => r.text,
    );
    assert(pruned === 1, `returns the number deleted (got ${pruned})`);
    assert(!left.includes("delivered-just-older"), "delivered row 1 ms older than cutoff is pruned");
    assert(left.includes("delivered-at-cutoff"), "delivered row exactly at cutoff is kept");
    assert(left.includes("delivered-just-newer"), "delivered row 1 ms newer than cutoff is kept");
    assert(left.includes("undelivered-old"), "undelivered row 30 days old is kept");
    db.close();
    removeDb(UNIT_DB);
  }

  // --- Test 4: WAL checkpoint after a non-empty prune only ---
  console.log("[test 4] a non-empty prune truncates the WAL; an empty one does not");
  {
    const db = openBrokerDb(UNIT_DB);
    const old = new Date(Date.now() - 30 * 86400_000).toISOString();
    db.transaction(() => {
      for (let i = 0; i < 500; i++) insertMessage(db, "p", old, 1, "y".repeat(2000));
    })();
    const before = walSize(UNIT_DB);
    assert(before > 0, `precondition: WAL holds the inserts (${before} bytes)`);

    const none = pruneDeliveredMessages(db, new Date(Date.now() - 60 * 86400_000).toISOString());
    assert(none === 0, "prune with an earlier cutoff deletes nothing");
    assert(walSize(UNIT_DB) === before, "an empty prune leaves the WAL untouched");

    const some = pruneDeliveredMessages(db, new Date().toISOString());
    assert(some === 500, `prune deletes all 500 old rows (got ${some})`);
    assert(walSize(UNIT_DB) === 0, `WAL truncated to 0 bytes (got ${walSize(UNIT_DB)})`);
    db.close();
    removeDb(UNIT_DB);
  }

  // --- Test 5: running broker prunes on its timer, honouring the env var ---
  console.log("[test 5] broker prunes on its timer and creates the index at startup");
  {
    const seed = openBrokerDb(E2E_DB);
    insertMessage(seed, "ghost", new Date(Date.now() - 10 * 86400_000).toISOString(), 0, "undelivered-old");
    seed.run("DROP INDEX idx_messages_pending");
    seed.close();

    broker = Bun.spawn(["bun", brokerScript], {
      env: {
        ...process.env,
        CLAUDE_PEERS_PORT: String(TEST_PORT),
        CLAUDE_PEERS_DB: E2E_DB,
        CLAUDE_PEERS_CONNECTED_WINDOW_SECONDS: "1",
        // Timer period = REAP_TTL/2 = 1.5 s.
        CLAUDE_PEERS_REAP_TTL_SECONDS: "3",
        CLAUDE_PEERS_MESSAGE_RETENTION_SECONDS: "2",
      },
      stdio: ["ignore", "inherit", "inherit"],
    });
    await waitForBroker();

    const pids = [0, 1].map(() => {
      const d = Bun.spawn(["sleep", "300"], { stdio: ["ignore", "ignore", "inherit"] });
      dummies.push(d);
      return d.pid!;
    });
    const [alice, bob] = await Promise.all(
      pids.map((pid, i) =>
        fetchJson<{ id: string }>("/register", {
          pid,
          cwd: `/test/${i}`,
          git_root: null,
          tty: null,
          summary: "",
          session_id: `retention-${STAMP}-${i}`,
        }),
      ),
    );
    await fetchJson("/send-message", { from_id: bob!.id, to_id: alice!.id, text: "delivered-now" });
    const polled = await fetchJson<{ messages: { id: number }[] }>("/poll-messages", {
      id: alice!.id,
      ack_supported: true,
    });
    await fetchJson("/ack-messages", { id: alice!.id, message_ids: polled.messages.map((m) => m.id) });
    assert(polled.messages.length === 1, "message delivered and acked through the broker");

    // Retention 2 s + up to 1.5 s for the next tick.
    await sleep(5000);
    broker.kill("SIGKILL");
    await broker.exited;
    broker = null;

    const db = new Database(E2E_DB, { readonly: true });
    const texts = (db.query("SELECT text FROM messages").all() as { text: string }[]).map((r) => r.text);
    assert(!texts.includes("delivered-now"), "delivered message pruned after the retention window");
    assert(texts.includes("undelivered-old"), "undelivered message kept");
    assert(hasPendingIndex(db), "broker startup created the index on the existing DB");
    db.close();
  }

  // --- Test 5b: default retention is 7 days, checked at 7 days +/- 1 hour ---
  console.log("[test 5b] default retention is 7 days");
  {
    removeDb(E2E_DB);
    const seed = openBrokerDb(E2E_DB);
    insertMessage(seed, "p", new Date(Date.now() - 7 * 86400_000 - 3600_000).toISOString(), 1, "delivered-7d-plus-1h");
    insertMessage(seed, "p", new Date(Date.now() - 7 * 86400_000 + 3600_000).toISOString(), 1, "delivered-7d-minus-1h");
    seed.close();

    const env: Record<string, string | undefined> = {
      ...process.env,
      CLAUDE_PEERS_PORT: String(TEST_PORT),
      CLAUDE_PEERS_DB: E2E_DB,
      CLAUDE_PEERS_CONNECTED_WINDOW_SECONDS: "1",
      CLAUDE_PEERS_REAP_TTL_SECONDS: "3",
    };
    delete env.CLAUDE_PEERS_MESSAGE_RETENTION_SECONDS;
    broker = Bun.spawn(["bun", brokerScript], { env, stdio: ["ignore", "inherit", "inherit"] });
    await waitForBroker();
    await sleep(2500);
    broker.kill("SIGKILL");
    await broker.exited;
    broker = null;

    const db = new Database(E2E_DB, { readonly: true });
    const texts = (db.query("SELECT text FROM messages").all() as { text: string }[]).map((r) => r.text);
    assert(!texts.includes("delivered-7d-plus-1h"), "delivered message 7 days + 1 h old pruned");
    assert(texts.includes("delivered-7d-minus-1h"), "delivered message 7 days - 1 h old kept");
    db.close();
  }

  // --- Test 6: invalid retention fails boot ---
  console.log("[test 6] invalid CLAUDE_PEERS_MESSAGE_RETENTION_SECONDS fails broker boot");
  {
    const bad = Bun.spawn(["bun", brokerScript], {
      env: {
        ...process.env,
        CLAUDE_PEERS_PORT: String(TEST_PORT + 1),
        CLAUDE_PEERS_DB: BAD_DB,
        CLAUDE_PEERS_MESSAGE_RETENTION_SECONDS: "0",
      },
      stdio: ["ignore", "ignore", "pipe"],
    });
    const code = await Promise.race([bad.exited, sleep(5000).then(() => null)]);
    if (code === null) bad.kill("SIGKILL");
    const stderr = await new Response(bad.stderr).text();
    assert(code !== null && code !== 0, `broker exits non-zero (got ${code})`);
    assert(
      stderr.includes("CLAUDE_PEERS_MESSAGE_RETENTION_SECONDS must be a positive integer"),
      "error names the variable",
    );
  }
} finally {
  if (broker) {
    try { broker.kill("SIGKILL"); } catch { /* ignore */ }
  }
  for (const d of dummies) {
    try { d.kill("SIGKILL"); } catch { /* ignore */ }
  }
  for (const p of [UNIT_DB, E2E_DB, BAD_DB]) removeDb(p);
}

console.log("---");
console.log(`Result: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);

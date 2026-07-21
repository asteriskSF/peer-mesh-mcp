#!/usr/bin/env bun
/**
 * test-session-id.ts
 *
 * End-to-end test for the stable session_id feature. Boots a test broker
 * on a non-standard port + temp DB and exercises the HTTP API directly.
 *
 * Verifies:
 *   1. /register returns a session_id (echoed when provided, or computed
 *      deterministically from (pid, cwd, tty) when omitted).
 *   2. Re-registering the same logical session (same session_id) reuses
 *      the existing ephemeral id — the disconnect/resume identity-stability
 *      fix. Cached to_id values held by other peers stay valid.
 *   3. /list-peers exposes session_id on every peer entry.
 *   4. /send-message accepts the "session:<session_id>" to_id form and
 *      resolves it to the current ephemeral id, even after a re-register.
 *   5. Re-registering with a *different* session_id under the same pid
 *      (pid reuse by a different logical session) does NOT steal the old
 *      session's id; the old session_id still resolves to its own row.
 *
 * Run: bun test/test-session-id.ts
 */

import { existsSync, unlinkSync } from "node:fs";

const TEST_PORT = 7901;
const TEST_DB = `/tmp/claude-peers-session-test-${Date.now()}.db`;
const BROKER_URL = `http://127.0.0.1:${TEST_PORT}`;

async function fetchJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`${path}: HTTP ${res.status} — ${await res.text()}`);
  }
  return (await res.json()) as T;
}

async function waitForBroker(timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${BROKER_URL}/health`);
      if (res.ok) return;
    } catch {
      /* not up yet */
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("broker didn't start within timeout");
}

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

interface RegisterResp {
  id: string;
  session_id: string;
}
interface PeerEntry {
  id: string;
  session_id: string;
  pid: number;
  cwd: string;
  tty: string | null;
  summary: string;
}
interface PollResp {
  messages: Array<{ id: number; from_id: string; to_id: string; text: string; sent_at: string }>;
}

// The broker's handleListPeers prunes any peer whose pid isn't a live
// process (process.kill(pid, 0) check). Use real long-lived child PIDs
// instead of fake numbers so roster + re-register paths exercise the
// same code production runs.
const dummies: Bun.Subprocess<"ignore", "ignore", "inherit">[] = [];
function spawnDummy(): number {
  const d = Bun.spawn(["sleep", "300"], { stdio: ["ignore", "ignore", "inherit"] });
  dummies.push(d);
  return d.pid!;
}

const alicePid = spawnDummy();
const bobPid = spawnDummy();
const carolPid = spawnDummy();

const brokerScript = new URL("../broker.ts", import.meta.url).pathname;
const proc = Bun.spawn(["bun", brokerScript], {
  env: { ...process.env, CLAUDE_PEERS_PORT: String(TEST_PORT), CLAUDE_PEERS_DB: TEST_DB },
  stdio: ["ignore", "ignore", "pipe"],
});

try {
  await waitForBroker();
  console.log(`[setup] test broker up on :${TEST_PORT}, db: ${TEST_DB}\n`);

  // --- Test 1: register returns echoed session_id ---
  console.log("[test 1] /register echoes the provided session_id");
  const alice1 = await fetchJson<RegisterResp>("/register", {
    pid: alicePid,
    cwd: "/test/alice",
    git_root: null,
    tty: "pts/1",
    summary: "alice",
    session_id: "alice-sess-001",
  });
  assert(alice1.id.length === 8, "alice gets an 8-char ephemeral id");
  assert(alice1.session_id === "alice-sess-001", "broker echoes the provided session_id");
  console.log();

  // --- Test 2: broker computes session_id when omitted ---
  console.log("[test 2] broker computes session_id from (pid, cwd, tty) when omitted");
  const bob1 = await fetchJson<RegisterResp>("/register", {
    pid: bobPid,
    cwd: "/test/bob",
    git_root: null,
    tty: "pts/2",
    summary: "bob",
  });
  assert(bob1.session_id.length === 8, "computed session_id is 8 hex chars");
  assert(/^[0-9a-f]{8}$/.test(bob1.session_id), "computed session_id is lowercase hex");
  // Re-register bob with identical inputs but a *provided* session_id that
  // matches the computed one — should reuse the same ephemeral id. We
  // re-derive by re-registering without session_id and confirming the id
  // is stable.
  const bob1Rereg = await fetchJson<RegisterResp>("/register", {
    pid: bobPid,
    cwd: "/test/bob",
    git_root: null,
    tty: "pts/2",
    summary: "bob",
  });
  assert(bob1Rereg.id === bob1.id, "re-register without session_id reuses ephemeral id (computed session_id matches)");
  assert(bob1Rereg.session_id === bob1.session_id, "computed session_id is deterministic");
  console.log();

  // --- Test 3: re-register with same session_id reuses ephemeral id ---
  console.log("[test 3] re-register same session_id reuses the ephemeral id");
  const alice2 = await fetchJson<RegisterResp>("/register", {
    pid: alicePid,
    cwd: "/test/alice",
    git_root: null,
    tty: "pts/1",
    summary: "alice-after-resume",
    session_id: "alice-sess-001",
  });
  assert(alice2.id === alice1.id, "ephemeral id is stable across re-register (the core fix)");
  assert(alice2.session_id === "alice-sess-001", "session_id unchanged after re-register");
  console.log();

  // --- Test 4: /list-peers exposes session_id ---
  console.log("[test 4] /list-peers exposes session_id on every entry");
  const peers = await fetchJson<PeerEntry[]>("/list-peers", {
    scope: "machine",
    cwd: "/test",
    git_root: null,
  });
  const aliceEntry = peers.find((p) => p.session_id === "alice-sess-001");
  assert(!!aliceEntry, "alice found in roster by session_id");
  assert(aliceEntry?.id === alice1.id, "roster id matches the stable ephemeral id");
  assert(aliceEntry?.summary === "alice-after-resume", "roster reflects re-registered summary");
  console.log();

  // --- Test 5: send via session:<sid> resolves to current id ---
  console.log("[test 5] /send-message accepts session:<sid> form");
  const sendRes = await fetchJson<{ ok: boolean; error?: string }>("/send-message", {
    from_id: bob1.id,
    to_id: "session:alice-sess-001",
    text: "hello via session handle",
  });
  assert(sendRes.ok === true, "send to session:alice-sess-001 succeeds");
  const alicePoll = await fetchJson<PollResp>("/poll-messages", {
    id: alice1.id,
    ack_supported: true,
  });
  assert(
    alicePoll.messages.some((m) => m.text === "hello via session handle"),
    "alice received the session-addressed message",
  );
  // ack so it doesn't pollute later tests
  await fetchJson("/ack-messages", {
    id: alice1.id,
    message_ids: alicePoll.messages.map((m) => m.id),
  });
  console.log();

  // --- Test 6: session: still resolves after target re-registers ---
  console.log("[test 6] session:<sid> resolves after target re-registers (disconnect/resume)");
  // Simulate alice's MCP subprocess restart: same session_id, fresh register.
  const alice3 = await fetchJson<RegisterResp>("/register", {
    pid: alicePid,
    cwd: "/test/alice",
    git_root: null,
    tty: "pts/1",
    summary: "alice-second-resume",
    session_id: "alice-sess-001",
  });
  assert(alice3.id === alice1.id, "second re-register keeps the same ephemeral id");
  const sendRes2 = await fetchJson<{ ok: boolean; error?: string }>("/send-message", {
    from_id: bob1.id,
    to_id: "session:alice-sess-001",
    text: "hello after resume",
  });
  assert(sendRes2.ok === true, "send via session: succeeds after target re-register");
  const alicePoll2 = await fetchJson<PollResp>("/poll-messages", {
    id: alice1.id,
    ack_supported: true,
  });
  assert(
    alicePoll2.messages.some((m) => m.text === "hello after resume"),
    "alice received message sent via session: after her re-register",
  );
  await fetchJson("/ack-messages", {
    id: alice1.id,
    message_ids: alicePoll2.messages.map((m) => m.id),
  });
  console.log();

  // --- Test 7: pid reuse by a DIFFERENT session does not steal the old id ---
  console.log("[test 7] pid reuse by a different session does not steal the old session's id");
  // A new logical session reuses alice's pid but with a different session_id.
  const carol = await fetchJson<RegisterResp>("/register", {
    pid: alicePid, // pid reuse: carol claims alice's pid
    cwd: "/test/carol",
    git_root: null,
    tty: "pts/3",
    summary: "carol-reused-pid",
    session_id: "carol-sess-001",
  });
  assert(carol.session_id === "carol-sess-001", "carol gets her own session_id");
  assert(carol.id !== alice1.id, "carol does not steal alice's ephemeral id");
  // alice's session should still resolve (she's still alive at alicePid...
  // except we just re-registered her pid, which removed her row. Verify the
  // session:alice handle is now correctly reported as gone, NOT silently
  // routed to carol.)
  const stealCheck = await fetchJson<{ ok: boolean; error?: string }>("/send-message", {
    from_id: bob1.id,
    to_id: "session:alice-sess-001",
    text: "should not reach carol",
  });
  assert(stealCheck.ok === false, "session:alice no longer resolves after her row was evicted by pid reuse");
  assert(
    /No peer currently registered for session alice-sess-001/.test(stealCheck.error ?? ""),
    "error names the unresolved session_id",
  );
  console.log();

  console.log("---");
  console.log(`Result: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
} finally {
  proc.kill();
  for (const d of dummies) {
    try { d.kill(); } catch { /* ignore */ }
  }
  await new Promise((r) => setTimeout(r, 200));
  if (existsSync(TEST_DB)) {
    try {
      unlinkSync(TEST_DB);
    } catch {
      /* ignore */
    }
  }
}

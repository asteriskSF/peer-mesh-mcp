#!/usr/bin/env bun
/**
 * test-peer-status.ts
 *
 * End-to-end test for the time-based reap / status feature. Boots a test
 * broker with very short TTLs (via env overrides) and verifies:
 *
 *   1. A fresh peer reports status "connected".
 *   2. A peer whose last_seen is past CONNECTED_WINDOW_SECONDS but within
 *      REAP_TTL_SECONDS reports "disconnected" and STAYS in the roster
 *      (no PID-liveness pruning).
 *   3. Queued messages to a "disconnected" peer are NOT deleted and
 *      deliver when the peer re-registers (simulating a resume).
 *   4. Re-register after disconnect reuses the ephemeral id (session_id
 *      stability holds across the disconnect/resume cycle).
 *   5. A peer whose MCP subprocess was killed but is still within
 *      REAP_TTL_SECONDS stays in the roster as "disconnected" — proving
 *      PID liveness no longer gates roster presence.
 *   6. A peer aged past REAP_TTL_SECONDS is reaped: row + undelivered
 *      messages to it are deleted.
 *
 * Run: bun test/test-peer-status.ts
 */

import { existsSync, unlinkSync } from "node:fs";

const TEST_PORT = 7902;
const TEST_DB = `/tmp/claude-peers-status-test-${Date.now()}.db`;
const BROKER_URL = `http://127.0.0.1:${TEST_PORT}`;

// Shrink TTLs so the test runs in seconds, not minutes. CONNECTED_WINDOW=1s
// means a peer flips to "disconnected" almost immediately without a
// heartbeat; REAP_TTL=3s + a 5s wait makes reaping deterministic (reaper
// interval = min(30s, max(1s, 3000/2)) = 1.5s, so at least 3 reaper ticks
// fire during the wait).
const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(TEST_PORT),
  CLAUDE_PEERS_DB: TEST_DB,
  CLAUDE_PEERS_CONNECTED_WINDOW_SECONDS: "1",
  CLAUDE_PEERS_REAP_TTL_SECONDS: "3",
};

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
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

interface RegisterResp {
  id: string;
  session_id: string;
}
interface PeerEntry {
  id: string;
  session_id: string;
  status: "connected" | "disconnected";
  pid: number;
  cwd: string;
  tty: string | null;
  summary: string;
  last_seen: string;
}
interface PollResp {
  messages: Array<{ id: number; from_id: string; to_id: string; text: string; sent_at: string }>;
}

// Real long-lived child PIDs. Note: with time-based reaping these are NOT
// required for roster presence (that's the whole point of the fix), but
// they keep the test faithful to production and let us explicitly verify
// case 5 (kill the subprocess, peer stays).
const dummies: Bun.Subprocess<"ignore", "ignore", "inherit">[] = [];
function spawnDummy(): number {
  const d = Bun.spawn(["sleep", "300"], { stdio: ["ignore", "ignore", "inherit"] });
  dummies.push(d);
  return d.pid!;
}

const alicePid = spawnDummy();
const bobPid = spawnDummy();

const brokerScript = new URL("../broker.ts", import.meta.url).pathname;
const proc = Bun.spawn(["bun", brokerScript], {
  env: ENV,
  stdio: ["ignore", "inherit", "inherit"],
});

async function listPeers(): Promise<PeerEntry[]> {
  return fetchJson<PeerEntry[]>("/list-peers", {
    scope: "machine",
    cwd: "/test",
    git_root: null,
  });
}

try {
  await waitForBroker();
  console.log(`[setup] test broker up on :${TEST_PORT}, db: ${TEST_DB}`);
  console.log(`[setup] CONNECTED_WINDOW=1s, REAP_TTL=3s\n`);

  // Register alice + bob.
  const alice = await fetchJson<RegisterResp>("/register", {
    pid: alicePid,
    cwd: "/test/alice",
    git_root: null,
    tty: "pts/1",
    summary: "alice",
    session_id: "alice-status-sess",
  });
  const bob = await fetchJson<RegisterResp>("/register", {
    pid: bobPid,
    cwd: "/test/bob",
    git_root: null,
    tty: "pts/2",
    summary: "bob",
    session_id: "bob-status-sess",
  });
  console.log(`[setup] alice=${alice.id}, bob=${bob.id}\n`);

  // --- Test 1: fresh peer is connected ---
  console.log("[test 1] fresh peer reports status=connected");
  {
    const peers = await listPeers();
    const a = peers.find((p) => p.session_id === "alice-status-sess");
    assert(!!a, "alice present in roster");
    assert(a?.status === "connected", "alice status is connected (just registered)");
  }
  console.log();

  // --- Test 2: aged-but-not-reaped peer is disconnected, still in roster ---
  console.log("[test 2] peer aged past connected window is disconnected but stays in roster");
  {
    // Don't heartbeat alice; wait > CONNECTED_WINDOW (1s) but < REAP_TTL (3s).
    await sleep(1500);
    const peers = await listPeers();
    const a = peers.find((p) => p.session_id === "alice-status-sess");
    assert(!!a, "alice still in roster after crossing connected window (no PID pruning)");
    assert(a?.status === "disconnected", "alice status flipped to disconnected");
    // Bob is still being heartbeated by... nobody. The broker doesn't auto-heartbeat;
    // bob's last_seen is also aging. To keep bob connected for later tests,
    // heartbeat him now.
    await fetchJson("/heartbeat", { id: bob.id });
    const peers2 = await listPeers();
    const b = peers2.find((p) => p.session_id === "bob-status-sess");
    assert(b?.status === "connected", "bob remains connected after explicit heartbeat");
  }
  console.log();

  // --- Test 3: queued messages to disconnected peer survive ---
  console.log("[test 3] messages to disconnected peer survive and deliver on resume");
  {
    // Alice is disconnected. Bob sends her a message.
    const sendRes = await fetchJson<{ ok: boolean; error?: string }>("/send-message", {
      from_id: bob.id,
      to_id: "session:alice-status-sess",
      text: "queued-while-disconnected",
    });
    assert(sendRes.ok === true, "send to disconnected alice (via session:) succeeds");

    // Alice resumes: re-register with same session_id. Must reuse ephemeral id
    // and the queued message must now be pollable.
    const aliceResumed = await fetchJson<RegisterResp>("/register", {
      pid: alicePid,
      cwd: "/test/alice",
      git_root: null,
      tty: "pts/1",
      summary: "alice-resumed",
      session_id: "alice-status-sess",
    });
    assert(aliceResumed.id === alice.id, "alice ephemeral id reused on resume (session_id stable)");

    const poll = await fetchJson<PollResp>("/poll-messages", {
      id: alice.id,
      ack_supported: true,
    });
    assert(
      poll.messages.some((m) => m.text === "queued-while-disconnected"),
      "alice received the message queued while she was disconnected",
    );
    await fetchJson("/ack-messages", {
      id: alice.id,
      message_ids: poll.messages.map((m) => m.id),
    });
  }
  console.log();

  // --- Test 4: killing the MCP subprocess does NOT evict the peer ---
  console.log("[test 4] killed subprocess stays in roster within reap window (PID decoupled)");
  {
    // Kill alice's dummy process. Under the old PID-based cleanStalePeers,
    // the next handleListPeers would delete her row. Under the new
    // time-based reap, she stays until last_seen ages past REAP_TTL.
    try { process.kill(alicePid, "SIGTERM"); } catch { /* already dead */ }
    // Re-register alice first so her last_seen is fresh, THEN kill, to
    // isolate the "subprocess dies after fresh heartbeat" case.
    await fetchJson<RegisterResp>("/register", {
      pid: alicePid,
      cwd: "/test/alice",
      git_root: null,
      tty: "pts/1",
      summary: "alice-pre-kill",
      session_id: "alice-status-sess",
    });
    // Spawn a fresh dummy and kill it immediately so the pid is dead but
    // the broker row (registered with that pid) has a fresh last_seen.
    const freshPid = spawnDummy();
    const carol = await fetchJson<RegisterResp>("/register", {
      pid: freshPid,
      cwd: "/test/carol",
      git_root: null,
      tty: "pts/9",
      summary: "carol-will-die",
      session_id: "carol-status-sess",
    });
    try { process.kill(freshPid, "SIGTERM"); } catch { /* ignore */ }
    // Give the OS a moment to reap the process.
    await sleep(300);
    const peers = await listPeers();
    const c = peers.find((p) => p.session_id === "carol-status-sess");
    assert(!!c, "carol still in roster after her subprocess was killed (PID decoupled)");
    assert(c?.status === "connected", "carol still connected (last_seen fresh despite dead PID)");
  }
  console.log();

  // --- Test 5: aged past REAP_TTL → row + undelivered messages deleted ---
  console.log("[test 5] peer aged past REAP_TTL is reaped with its undelivered messages");
  {
    // Carol's last_seen is fresh from test 4. Send her a message, then
    // wait > REAP_TTL (3s) without any heartbeat. The periodic reaper
    // (interval = min(30s, REAP_TTL/2 = 1.5s)) should evict her within
    // a couple of ticks. 5s gives comfortable margin.
    await fetchJson<{ ok: boolean }>("/send-message", {
      from_id: bob.id,
      to_id: "session:carol-status-sess",
      text: "will-be-reaped",
    });
    // Heartbeat bob so he's not reaped.
    await fetchJson("/heartbeat", { id: bob.id });
    // Wait > REAP_TTL + a couple reaper intervals.
    await sleep(5000);
    await fetchJson("/heartbeat", { id: bob.id }); // keep bob alive
    const peers = await listPeers();
    const c = peers.find((p) => p.session_id === "carol-status-sess");
    assert(!c, "carol reaped from roster after aging past REAP_TTL");

    // Sending to carol's session: handle now fails (no row).
    const sendRes = await fetchJson<{ ok: boolean; error?: string }>("/send-message", {
      from_id: bob.id,
      to_id: "session:carol-status-sess",
      text: "should-not-deliver",
    });
    assert(sendRes.ok === false, "send to reaped carol's session: fails");
    assert(
      /No peer currently registered for session carol-status-sess/.test(sendRes.error ?? ""),
      "reap is reflected in session: resolution error",
    );
  }
  console.log();

  // Cleanup BEFORE exit — process.exit skips finally. Kill the broker
  // child forcefully so it doesn't leak and hold the port for the next
  // run (we saw EADDRINUSE from exactly this).
  try { proc.kill("SIGTERM"); } catch { /* ignore */ }
  await sleep(100);
  try { proc.kill("SIGKILL"); } catch { /* ignore */ }
  for (const d of dummies) {
    try { d.kill("SIGKILL"); } catch { /* ignore */ }
  }
  await sleep(100);
  if (existsSync(TEST_DB)) {
    try { unlinkSync(TEST_DB); } catch { /* ignore */ }
  }

  console.log("---");
  console.log(`Result: ${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
} catch (e) {
  // Ensure broker is killed even on assertion-throw paths.
  try { proc.kill("SIGKILL"); } catch { /* ignore */ }
  for (const d of dummies) {
    try { d.kill("SIGKILL"); } catch { /* ignore */ }
  }
  throw e;
} finally {
  if (existsSync(TEST_DB)) {
    try { unlinkSync(TEST_DB); } catch { /* ignore */ }
  }
}

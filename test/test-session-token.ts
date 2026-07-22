#!/usr/bin/env bun
/**
 * test-session-token.ts
 *
 * Unit test for the persisted session token (shared/session.ts).
 * Verifies that getOrCreateSessionId returns a stable UUID across
 * calls with different PIDs but the same CWD/TTY — the OS-restart
 * survival scenario (WSL crash, host reboot, new PID on resume).
 *
 * Also verifies distinct TTYs in the same CWD get distinct tokens.
 *
 * Run: bun test/test-session-token.ts
 */

import { existsSync, rmSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

const { getOrCreateSessionId } = await import("../shared/session.ts");

// Create temp directories for test CWDs
const tempDirs: string[] = [];
function makeTempDir(): string {
  const d = mkdtempSync(join(tmpdir(), "claude-peers-token-test-"));
  tempDirs.push(d);
  return d;
}

try {
  // --- Test 1: same CWD + TTY returns same token across calls (simulates OS restart) ---
  console.log("[test 1] same CWD + TTY returns same session_id across calls (new PID)");
  {
    const cwd = makeTempDir();
    const sid1 = getOrCreateSessionId(cwd, "pts/1");
    const sid2 = getOrCreateSessionId(cwd, "pts/1");
    assert(sid1 === sid2, "same session_id returned on second call (same CWD/TTY)");
    assert(sid1.length > 0, "session_id is non-empty");
    assert(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(sid1), "session_id is a UUID v4");
    // Simulate OS restart: different PID, same CWD/TTY. The function doesn't
    // take pid anymore, so calling it again with the same CWD/TTY reads the
    // persisted token file — same UUID.
    const sid3 = getOrCreateSessionId(cwd, "pts/1");
    assert(sid3 === sid1, "session_id survives OS restart (same CWD/TTY, no PID in key)");
  }
  console.log();

  // --- Test 2: distinct TTYs in same CWD get distinct tokens ---
  console.log("[test 2] distinct TTYs in same CWD get distinct session_ids");
  {
    const cwd = makeTempDir();
    const sid1 = getOrCreateSessionId(cwd, "pts/1");
    const sid2 = getOrCreateSessionId(cwd, "pts/2");
    assert(sid1 !== sid2, "different TTYs produce different session_ids");
  }
  console.log();

  // --- Test 3: token file is persisted to .claude-peers/session-<tty> ---
  console.log("[test 3] token file persisted to .claude-peers/session-<tty>");
  {
    const cwd = makeTempDir();
    const sid = getOrCreateSessionId(cwd, "pts/13");
    const tokenFile = join(cwd, ".claude-peers", "session-pts-13");
    assert(existsSync(tokenFile), "token file exists at .claude-peers/session-pts-13");
    const content = await Bun.file(tokenFile).text();
    assert(content.trim() === sid, "token file content matches returned session_id");
  }
  console.log();

  // --- Test 4: null TTY falls back to session-default ---
  console.log("[test 4] null TTY uses session-default filename");
  {
    const cwd = makeTempDir();
    const sid = getOrCreateSessionId(cwd, null);
    const tokenFile = join(cwd, ".claude-peers", "session-default");
    assert(existsSync(tokenFile), "token file exists at .claude-peers/session-default");
    const sid2 = getOrCreateSessionId(cwd, null);
    assert(sid === sid2, "null TTY returns same token on second call");
  }
  console.log();

  // --- Test 5: different CWDs get different tokens (same TTY) ---
  console.log("[test 5] different CWDs get different session_ids (same TTY)");
  {
    const cwd1 = makeTempDir();
    const cwd2 = makeTempDir();
    const sid1 = getOrCreateSessionId(cwd1, "pts/1");
    const sid2 = getOrCreateSessionId(cwd2, "pts/1");
    assert(sid1 !== sid2, "different CWDs produce different session_ids");
  }
  console.log();

  console.log("---");
  console.log(`Result: ${passed} passed, ${failed} failed`);
  process.exitCode = failed > 0 ? 1 : 0;
} finally {
  for (const d of tempDirs) {
    try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
}

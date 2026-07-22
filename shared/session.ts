// Stable per-session identity. Shared by server.ts and broker.ts so the
// two cannot drift — the whole session_id reuse fix depends on both
// producing the same value for the same logical session.
//
// The session_id is a UUID persisted to a file inside the session's CWD
// (.claude-peers/session-<tty>), so it survives OS-level restarts (WSL
// crash, host reboot, terminal kill) where the PID changes. The file is
// created on first run and read on every subsequent startup.

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

/**
 * Directory inside the CWD where session token files are stored.
 */
const SESSION_DIR = ".claude-peers";

/**
 * Sanitize a TTY string into a safe filename component. Null/unknown TTY
 * falls back to "default" so sessions without a detectable TTY still work.
 */
function ttyToFilePart(tty: string | null): string {
  if (!tty || tty === "?" || tty === "??") return "default";
  // pts/13 -> pts-13, tty/0 -> tty-0, etc.
  return tty.replace(/[^a-zA-Z0-9]/g, "-");
}

/**
 * Read or create a stable session_id for the given (cwd, tty) pair.
 *
 * The session_id is a UUID v4 persisted to `{cwd}/.claude-peers/session-{tty}`.
 * On first call, the file is created with a fresh UUID. On subsequent calls
 * (including after OS restart with a new PID), the same UUID is read back.
 *
 * Two concurrent sessions in the same CWD get distinct session_ids because
 * the TTY is part of the filename — each terminal/pane has its own TTY.
 *
 * NOTE: session_id is NOT authenticated. The inputs (cwd, tty) are
 * self-reported by the MCP server with no verification, and the broker
 * trusts them. Under the repo's localhost-only trust model this is
 * acceptable — any local process can claim any peer identity — but
 * callers must not treat session_id as a cryptographically secure handle.
 * See README "Session identity and addressing".
 */
export function getOrCreateSessionId(cwd: string, tty: string | null): string {
  const dir = join(cwd, SESSION_DIR);
  const tokenFile = join(dir, `session-${ttyToFilePart(tty)}`);

  if (existsSync(tokenFile)) {
    const token = readFileSync(tokenFile, "utf-8").trim();
    if (token) return token;
  }

  // First run (or corrupted/empty file): generate a fresh UUID and persist it.
  const sessionId = crypto.randomUUID();
  try {
    mkdirSync(dir, { recursive: true });
    writeFileSync(tokenFile, sessionId, "utf-8");
  } catch {
    // If we can't persist (read-only FS, permissions, etc.), return the
    // UUID anyway — it won't survive a restart, but the session works
    // for the current run. The broker still accepts it.
  }
  return sessionId;
}

// Stable per-session identity derivation. Shared by server.ts and broker.ts
// so the two cannot drift — the whole session_id reuse fix depends on both
// computing identical output for the same (pid, cwd, tty).

/**
 * Deterministic 8-hex-char session identity keyed on (pid, cwd, tty).
 *
 * Stable across MCP subprocess restart (same process lineage re-uses it
 * on re-register) and across broker restart (value is a pure function of
 * inputs, no DB persistence needed to recover it). tty is part of the key
 * so two concurrent Claude sessions in the same CWD (e.g. two panes in one
 * tmux) get distinct session_ids; null tty is tolerated.
 *
 * NOTE: session_id is NOT authenticated. The inputs (pid, cwd, tty) are
 * self-reported by the MCP server with no verification, and the broker
 * trusts them. Under the repo's localhost-only trust model this is
 * acceptable — any local process can claim any peer identity — but
 * callers must not treat session_id as a cryptographically secure handle.
 * See README "Session identity and addressing".
 */
export function computeSessionId(
  pid: number,
  cwd: string,
  tty: string | null,
): string {
  const key = `${pid}\x1f${cwd}\x1f${tty ?? ""}`;
  // Bun.hash returns a u64; render as 16 lowercase hex chars and keep the
  // low 8 (32-bit) for a compact, readable handle.
  const h = BigInt.asUintN(64, BigInt(Bun.hash(key)));
  return h.toString(16).padStart(16, "0").slice(8);
}

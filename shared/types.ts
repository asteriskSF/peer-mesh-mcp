// Stable per-session identity. Derived deterministically from
// (pid, cwd, tty) so it survives MCP subprocess restart (disconnect/
// resume) AND broker restart — unlike `id`, which is a random ephemeral
// transport handle minted on each /register and reused only as a
// side effect of session_id matching. Callers that need to address a
// peer across reconnects should use `session:<session_id>` as the
// `to_id` in send_message, or cache `session_id` rather than `id`.
export type PeerId = string;
export type SessionId = string;

// Liveness state of a peer, derived from last_seen recency in the broker.
//   "connected"    — heartbeating within the connected window (recent).
//   "disconnected" — missed heartbeats but within the reap window; the MCP
//                    server subprocess is likely down but the session may
//                    resume (e.g. terminal closed, host sleep, crash). The
//                    peer stays in the roster and queued messages to it are
//                    NOT deleted — they deliver on resume.
// Beyond the reap window the row is deleted entirely by reapStalePeers.
export type PeerStatus = "connected" | "disconnected";

export interface Peer {
  id: PeerId;
  session_id: SessionId;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
  status: PeerStatus;
  registered_at: string; // ISO timestamp
  last_seen: string; // ISO timestamp
}

export interface Message {
  id: number;
  from_id: PeerId;
  to_id: PeerId;
  text: string;
  sent_at: string; // ISO timestamp
  delivered: boolean;
}

// --- Broker API types ---

export interface RegisterRequest {
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
  /**
   * Optional stable session identity. When provided, the broker reuses
   * the existing peer row's `id` if a peer with the same session_id is
   * already registered (or was registered before a subprocess restart),
   * keeping the ephemeral transport `id` stable across reconnects for
   * the same logical session. When omitted, the broker computes one
   * deterministically from (pid, cwd, tty) as a fallback.
   */
  session_id?: SessionId;
}

export interface RegisterResponse {
  id: PeerId;
  session_id: SessionId;
}

export interface HeartbeatRequest {
  id: PeerId;
}

export interface SetSummaryRequest {
  id: PeerId;
  summary: string;
}

export interface ListPeersRequest {
  scope: "machine" | "directory" | "repo";
  // The requesting peer's context (used for filtering)
  cwd: string;
  git_root: string | null;
  exclude_id?: PeerId;
}

export interface SendMessageRequest {
  from_id: PeerId;
  /**
   * Target peer. Two accepted forms:
   *   1. Ephemeral transport id (e.g. "ab12cd34") — resolved directly.
   *   2. Stable session handle "session:<session_id>" — resolved to the
   *      peer row currently registered with that session_id. Survives
   *      the target's subprocess restart (disconnect/resume) and broker
   *      restart, unlike form 1 which can go stale after a reconnect.
   */
  to_id: PeerId;
  text: string;
}

export interface PollMessagesRequest {
  id: PeerId;
  /**
   * Set to true by MCP server clients that implement /ack-messages. When
   * true, the broker uses the new at-least-once delivery semantics:
   * messages stay delivered=0 until explicitly acked, with a per-message
   * polled_at lease that allows retry on push failure. When omitted (old
   * clients), the broker falls back to legacy at-most-once: messages are
   * marked delivered=1 immediately on poll, with the original silent-loss
   * risk on push failure — but no duplicate-storm during a rollout where
   * old MCP server subprocesses outlive a broker upgrade.
   */
  ack_supported?: boolean;
}

export interface PollMessagesResponse {
  messages: Message[];
}

export interface AckMessagesRequest {
  id: PeerId;
  message_ids: number[];
}

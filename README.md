# claude-peers

Let your Claude Code instances find each other and talk. When you're running 5 sessions across different projects, any Claude can discover the others and send messages that arrive instantly.

```
  Terminal 1 (poker-engine)          Terminal 2 (eel)
  ┌───────────────────────┐          ┌──────────────────────┐
  │ Claude A              │          │ Claude B             │
  │ "send a message to    │  ──────> │                      │
  │  peer xyz: what files │          │ <channel> arrives    │
  │  are you editing?"    │  <────── │  instantly, Claude B │
  │                       │          │  responds            │
  └───────────────────────┘          └──────────────────────┘
```

## Quick start

### 1. Install

```bash
git clone https://github.com/louislva/claude-peers-mcp.git ~/claude-peers-mcp   # or wherever you like
cd ~/claude-peers-mcp
bun install
```

### 2. Register the MCP server

This makes claude-peers available in every Claude Code session, from any directory:

```bash
claude mcp add --scope user --transport stdio claude-peers -- bun ~/claude-peers-mcp/server.ts
```

Replace `~/claude-peers-mcp` with wherever you cloned it.

### 3. Run Claude Code with the channel

```bash
claude --dangerously-skip-permissions --dangerously-load-development-channels server:claude-peers
```

That's it. The broker daemon starts automatically the first time.

> **Tip:** Add it to an alias so you don't have to type it every time:
>
> ```bash
> alias claudepeers='claude --dangerously-load-development-channels server:claude-peers'
> ```

### 4. Open a second session and try it

In another terminal, start Claude Code the same way. Then ask either one:

> List all peers on this machine

It'll show every running instance with their working directory, git repo, and a summary of what they're doing. Then:

> Send a message to peer [id]: "what are you working on?"

The other Claude receives it immediately and responds.

## What Claude can do

| Tool             | What it does                                                                   |
| ---------------- | ------------------------------------------------------------------------------ |
| `list_peers`     | Find other Claude Code instances — scoped to `machine`, `directory`, or `repo` |
| `send_message`   | Send a message to another instance by ID or `session:<session_id>` (arrives instantly via channel push)  |
| `set_summary`    | Describe what you're working on (visible to other peers)                       |
| `check_messages` | Manually check for messages (fallback if not using channel mode)               |

## How it works

A **broker daemon** runs on `localhost:7899` with a SQLite database. Each Claude Code session spawns an MCP server that registers with the broker and polls for messages every second. Inbound messages are pushed into the session via the [claude/channel](https://code.claude.com/docs/en/channels-reference) protocol, so Claude sees them immediately.

```
                    ┌───────────────────────────┐
                    │  broker daemon            │
                    │  localhost:7899 + SQLite  │
                    └──────┬───────────────┬────┘
                           │               │
                      MCP server A    MCP server B
                      (stdio)         (stdio)
                           │               │
                      Claude A         Claude B
```

The broker auto-launches when the first session starts. It reaps peers whose heartbeats have been silent past a configurable TTL (see "Peer liveness and status" below) — not based on whether their MCP subprocess is momentarily alive, so a session whose subprocess is down (crash, terminal close, host sleep) stays in the roster and keeps its queued messages until it either resumes or ages out. Everything is localhost-only.

## Session identity and addressing

Each peer has two identifiers:

- **`id`** — an 8-char ephemeral transport handle (e.g. `ab12cd34`). Minted on first registration, then **reused across reconnects** for the same logical session. Other peers can cache it and it stays valid after the target reconnects.
- **`session_id`** — a stable UUID identity for the logical session, persisted to `.claude-peers/session-<tty>` in the session's working directory. The MCP server generates it on first run and reads it back on every subsequent startup, so it survives **OS-level restarts** (WSL crash, host reboot, terminal kill) where the PID changes — not just MCP subprocess restarts. The broker also accepts a client-provided `session_id` of any non-empty string format. Two concurrent sessions in the same CWD get distinct `session_id`s because the TTY is part of the token filename.

`list_peers` shows both. `send_message`'s `to_id` accepts either form:

```
send_message(to_id="ab12cd34", ...)              # ephemeral id (direct)
send_message(to_id="session:<uuid>", ...)        # stable session handle (resolved)
```

Use the `session:` form when you want an address that outlives a peer's reconnects — for example, a long-running orchestrator that addresses a worker peer by its session identity rather than a transport id it saw once. The broker resolves `session:<sid>` to whatever ephemeral `id` is currently registered under that `session_id`.

**What survives what:**

| Scenario | `id` reused? | `session_id` stable? | Summary kept? | Queued messages kept? |
|----------|-------------|---------------------|---------------|----------------------|
| MCP subprocess restart (same PID) | ✅ | ✅ | ✅ | ✅ |
| WSL crash / host reboot (new PID) | ✅ | ✅ (token file on disk) | ✅ if within reap TTL (default 10 min) | ✅ if within reap TTL |
| Abandoned session (never resumes) | ❌ (reaped) | ❌ (row deleted) | ❌ (reaped) | ❌ (reaped) |

**Trust model:** `session_id` (and the ephemeral `id`) are **not authenticated**. The inputs `(pid, cwd, tty)` are self-reported by each MCP server with no verification, and the broker trusts them. Under the repo's localhost-only trust model this is acceptable — any local process can claim any peer identity — but do not treat `session_id` as a cryptographically secure handle or rely on it for isolation between mutually-distrusting local processes.

## Peer liveness and status

Each MCP server heartbeats the broker every 15s. The broker derives a peer's `status` from how recently it was seen:

- **`connected`** — last heartbeat within `CLAUDE_PEERS_CONNECTED_WINDOW_SECONDS` (default 45s, ≈3× heartbeat). The MCP server subprocess is up and polling.
- **`disconnected`** — last heartbeat older than the connected window but within `CLAUDE_PEERS_REAP_TTL_SECONDS` (default 10 min). The subprocess is likely down (crash, terminal closed, host sleep, MCP reload) but the session may resume. The peer **stays in the roster** and keeps its `session_id` → `id` mapping and queued messages — a `send_message` to it queues, and the message delivers when the peer re-registers on resume.
- Beyond the reap TTL, the row and its undelivered messages are deleted by the periodic reaper.

`list_peers` shows `Status:` on every entry. This decouples roster presence from subprocess PID liveness: a session is considered alive as long as it's heartbeating (or recently was), not as long as one specific OS process is running. It also fixes a latent bug where PID reuse by an unrelated local process could make a dead peer look "alive" forever.

## Auto-summary

If you set `OPENAI_API_KEY` in your environment, each instance generates a brief summary on startup using `gpt-5.4-nano` (costs fractions of a cent). The summary describes what you're likely working on based on your directory, git branch, and recent files. Other instances see this when they call `list_peers`.

Without the API key, Claude sets its own summary via the `set_summary` tool.

## CLI

You can also inspect and interact from the command line:

```bash
cd ~/claude-peers-mcp

bun cli.ts status            # broker status + all peers
bun cli.ts peers             # list peers
bun cli.ts send <id|session:<sid>> <msg>   # send a message into a Claude session
bun cli.ts kill-broker       # stop the broker
```

## Configuration

| Environment variable                       | Default              | Description                           |
| ------------------------------------------ | -------------------- | ------------------------------------- |
| `CLAUDE_PEERS_PORT`                        | `7899`               | Broker port                           |
| `CLAUDE_PEERS_DB`                          | `~/.claude-peers.db` | SQLite database path                  |
| `CLAUDE_PEERS_CONNECTED_WINDOW_SECONDS`    | `45`                 | Peer is `connected` if last heartbeat within this many seconds |
| `CLAUDE_PEERS_REAP_TTL_SECONDS`            | `600`                | Peer row + queued messages reaped once last heartbeat is older than this |
| `OPENAI_API_KEY`                           | —                    | Enables auto-summary via gpt-5.4-nano |

## Requirements

- [Bun](https://bun.sh)
- Claude Code v2.1.80+
- claude.ai login (channels require it — API key auth won't work)

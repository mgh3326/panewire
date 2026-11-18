// panewire omp session-message receiver (hk 4261; docs/session-message-receivers.md).
//
// Install beside the herdr extension: copy this file to ~/.omp/agent/extensions/.
//
// In an omp session that panewire addresses by lane (PANEWIRE_LANE and
// HERDR_PANE_ID set, not a nested `omp` started from another session's shell),
// this extension binds a Unix socket, records its path with
// `panewire omp-socket register`, and turns each line the node daemon posts into
// pi.sendMessage. An idle session starts a turn with it; a working session reads
// it at the next step boundary without interrupting the tool batch in flight
// (deliverAs: "aside"). The extension answers {"ok":true} only after
// pi.sendMessage returned, so the node reports what it has evidence for.
//
// Wire, one JSON line each way:
//   node -> extension   {"type":"panewire.message","text":"..."}
//   extension -> node   {"ok":true}   (or {"ok":false}; then the node types the line)
// @ts-nocheck

import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const LANE = process.env.PANEWIRE_LANE;
const PANE = process.env.HERDR_PANE_ID;
// OMP marks every shell it spawns with OMPCODE=1; a nested omp is not the pane's session.
const NESTED = process.env.OMPCODE === "1";
const MAX_LINE = 64 * 1024;
const MESSAGE_TYPE = "panewire.message";

export default function (pi) {
  if (!LANE || !PANE || NESTED) {
    return;
  }
  let server;
  let dir;

  function reply(socket, ok) {
    socket.end(`${JSON.stringify({ ok })}\n`);
  }

  function handle(socket) {
    let buffered = "";
    let answered = false;
    socket.setEncoding("utf8");
    socket.setTimeout(5000, () => socket.destroy());
    socket.on("error", () => socket.destroy());
    socket.on("data", (chunk) => {
      if (answered) return;
      buffered += chunk;
      if (buffered.length > MAX_LINE) {
        answered = true;
        reply(socket, false);
        return;
      }
      const end = buffered.indexOf("\n");
      if (end < 0) return;
      answered = true;
      let ok = false;
      try {
        const line = JSON.parse(buffered.slice(0, end));
        if (line && line.type === MESSAGE_TYPE && typeof line.text === "string" && line.text.length > 0) {
          pi.sendMessage(
            { customType: "panewire-hub", content: line.text, display: true },
            { triggerTurn: true, deliverAs: "aside" },
          );
          ok = true;
        }
      } catch {
        ok = false;
      }
      reply(socket, ok);
    });
  }

  function stop() {
    if (server) {
      server.close();
      server = undefined;
    }
    if (dir) {
      fs.rmSync(dir, { recursive: true, force: true });
      dir = undefined;
    }
  }

  pi.on("session_start", async () => {
    if (server) return;
    // A short path: a Unix socket path holds about 104 bytes. The directory is
    // private to this user, as Claude Code's own socket directory is.
    dir = fs.mkdtempSync(path.join(process.env.PANEWIRE_OMP_SOCKET_DIR || os.tmpdir(), "pwomp-"));
    const socketPath = path.join(dir, "s.sock");
    server = net.createServer(handle);
    server.on("error", () => stop());
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(socketPath, resolve);
    }).catch(() => stop());
    if (!server) return;
    fs.chmodSync(socketPath, 0o600);
    const registered = await pi
      .exec("panewire", ["omp-socket", "register", "--lane", LANE, "--pane", PANE, "--socket", socketPath], { timeout: 10000 })
      .catch(() => ({ code: 1 }));
    if (registered.code !== 0) {
      // Without a registration the node types the line; a socket nobody finds is waste.
      stop();
    }
  });

  pi.on("session_shutdown", async () => stop());
}

#!/bin/sh
# New hub + previous-release node client compatibility (fleet CI r2 §7).
#
# usage: check-hub-old-client.sh <candidate-binary> <previous-release-binary> <tag>
#
# The hub is deployed before the nodes, so the candidate hub must accept a node
# client from the previous release. This starts the candidate hub on loopback,
# fronts it with a throwaway TLS terminator (the node client dials wss), and
# sends one transient hello plus a note from the previous release binary's
# `hub-emit`. A rejected hello is counted as an unknown message by the hub, so
# the check reads the hub's own counter: zero means the old client's hello and
# event were both accepted.
set -eu

candidate=${1:?usage: check-hub-old-client.sh <candidate> <old> <tag>}
old=${2:?usage: check-hub-old-client.sh <candidate> <old> <tag>}
tag=${3:?usage: check-hub-old-client.sh <candidate> <old> <tag>}
[ -x "$candidate" ] || { echo "candidate binary is not executable: $candidate" >&2; exit 1; }
[ -x "$old" ] || { echo "previous release binary is not executable: $old" >&2; exit 1; }

work=$(mktemp -d)
hub_pid=
proxy_pid=
cleanup() {
	if [ -n "$proxy_pid" ]; then kill "$proxy_pid" 2>/dev/null || true; fi
	if [ -n "$hub_pid" ]; then kill "$hub_pid" 2>/dev/null || true; fi
	if [ -n "$proxy_pid" ]; then wait "$proxy_pid" 2>/dev/null || true; fi
	if [ -n "$hub_pid" ]; then wait "$hub_pid" 2>/dev/null || true; fi
	rm -rf "$work"
}
trap cleanup EXIT INT TERM

pick_port() {
	python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()'
}
hub_port=$(pick_port)
tls_port=$(pick_port)
while [ "$tls_port" = "$hub_port" ]; do tls_port=$(pick_port); done

umask 077
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$work/ca.key" -out "$work/ca.crt" \
	-subj '/CN=panewire-ci-compat-ca' -days 1 >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$work/server.key" -out "$work/server.csr" \
	-subj '/CN=127.0.0.1' >/dev/null 2>&1
printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >"$work/server.ext"
openssl x509 -req -in "$work/server.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" \
	-CAcreateserial -out "$work/server.crt" -days 1 -extfile "$work/server.ext" >/dev/null 2>&1

operator_token=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
node_token=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
printf 'HUB_TOKEN_operator=%s\nHUB_TOKEN_old-node=%s\n' "$operator_token" "$node_token" >"$work/auth.env"
printf 'HUB_MACHINE_ID=old-node\nHUB_TOKEN=%s\n' "$node_token" >"$work/node.env"
printf '{"lanes":{"compat-lane":{"machine":"old-node","pane":"w1:p1"}}}\n' >"$work/lanes.json"

"$candidate" hub --listen "127.0.0.1:$hub_port" --hub-auth "$work/auth.env" \
	--lanes "$work/lanes.json" --delivery-policy off --ui-allow-cf-only \
	>"$work/hub.log" 2>&1 &
hub_pid=$!
tries=0
until curl -fsS "http://127.0.0.1:$hub_port/healthz" >/dev/null 2>&1; do
	tries=$((tries + 1))
	if [ "$tries" -ge 100 ]; then
		echo "candidate hub did not become healthy:" >&2
		cat "$work/hub.log" >&2
		exit 1
	fi
	sleep 0.2
done

cat >"$work/tlsproxy.py" <<'PY'
import socket
import ssl
import sys
import threading

listen_port, upstream_port, cert, key = (
    int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4])
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(cert, key)


def pump(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def handle(client):
    try:
        tls = context.wrap_socket(client, server_side=True)
    except (ssl.SSLError, OSError):
        client.close()
        return
    try:
        upstream = socket.create_connection(("127.0.0.1", upstream_port))
    except OSError:
        tls.close()
        return
    threading.Thread(target=pump, args=(tls, upstream), daemon=True).start()
    pump(upstream, tls)
    try:
        tls.close()
    except OSError:
        pass
    upstream.close()


listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", listen_port))
listener.listen(16)
while True:
    conn, _ = listener.accept()
    threading.Thread(target=handle, args=(conn,), daemon=True).start()
PY
python3 "$work/tlsproxy.py" "$tls_port" "$hub_port" "$work/server.crt" "$work/server.key" \
	>"$work/proxy.log" 2>&1 &
proxy_pid=$!
tries=0
until curl -fsS --cacert "$work/ca.crt" "https://127.0.0.1:$tls_port/healthz" >/dev/null 2>&1; do
	tries=$((tries + 1))
	if [ "$tries" -ge 100 ]; then
		echo "TLS terminator did not become healthy:" >&2
		cat "$work/proxy.log" >&2
		exit 1
	fi
	sleep 0.2
done

echo "previous release node client: $tag ($("$old" version))"
SSL_CERT_FILE="$work/ca.crt" "$old" hub-emit --hub-url "wss://127.0.0.1:$tls_port" \
	--hub-token-env "$work/node.env" --text "old-client compatibility $tag"

# The hub counts a hello or event it cannot decode as an unknown message.
# Give the read loop a moment to consume both frames, then read its counter.
sleep 1
curl -fsS "http://127.0.0.1:$hub_port/ui/data.json" >"$work/ui.json"
python3 - "$work/ui.json" "$tag" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    data = json.load(handle)
unknown = data["hub"]["unknown_messages"]
if unknown != 0:
    raise SystemExit(f"candidate hub did not accept the {sys.argv[2]} node client: unknown_messages={unknown}")
print(f"candidate hub accepted the {sys.argv[2]} node client (unknown_messages=0)")
PY

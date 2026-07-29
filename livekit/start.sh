#!/usr/bin/env bash
# Launch the three processes that make up the LiveKit sandbox agent:
#   1. livekit-server  — the SFU, on localhost:7880
#   2. the Agents worker (app.agent) — connects out to the SFU and is
#      auto-dispatched into every room that gets created
#   3. the FastAPI voice_ws bridge (app.bridge) — serves the voice_ws channel on
#      :PORT and relays PCM16 audio into/out of a LiveKit room
#
# Ordering matters. A caller opening /voice is what creates a room, and the SFU
# can only dispatch the agent into that room if the worker has already
# registered. So we start the SFU, wait for it, start the worker, and wait for
# it to actually register (by tailing its log for "registered worker") before
# bringing up the bridge. This avoids a dispatch race under load where the room
# is created before the worker can be dispatched into it, so the agent never
# joins and the caller hears no answer.
#
# Supervision: all three run in the background; a trap + `wait -n` brings the
# container down as a unit when any one dies. Deliberately no `set -e` — it
# would kill the shell on the first child's non-zero exit before cleanup runs.

export PORT="${PORT:-8008}"
export LIVEKIT_URL="${LIVEKIT_URL:-ws://localhost:7880}"
export LIVEKIT_API_KEY="${LIVEKIT_API_KEY:-devkey}"
export LIVEKIT_API_SECRET="${LIVEKIT_API_SECRET:-secret}"

wait_for_port() {
  local host="$1" port="$2" tries="${3:-100}"
  for _ in $(seq 1 "$tries"); do
    (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    sleep 0.2
  done
  return 1
}

# 1. SFU in dev mode (devkey/secret, ws://, no TLS).
livekit-server --dev --bind 0.0.0.0 &
LK_PID=$!

if ! wait_for_port localhost 7880; then
  echo "[start] livekit-server never came up on :7880" >&2
  kill "$LK_PID" 2>/dev/null
  exit 1
fi
echo "[start] livekit-server ready on :7880"

# 2. Agents worker — registers with the SFU so it can be dispatched. Capture
# its log so we can gate on *actual* registration, and mirror it to stdout so
# it still lands in agent.log for debugging.
uv run --no-sync python -m app.agent start > /tmp/worker.log 2>&1 &
WK_PID=$!
tail -f /tmp/worker.log 2>/dev/null &
TAIL_PID=$!

# Gate the bridge on the worker actually registering with the SFU — NOT a fixed
# sleep. Under load the worker can take 10s+ to register; if the bridge accepts
# a caller before then, the room is created before the worker can be dispatched
# into it, so the agent never joins and the caller hears no answer. Wait (up to
# 60s) for the "registered worker" log line.
echo "[start] waiting for the agent worker to register with the SFU..."
for _ in $(seq 1 120); do
  grep -q "registered worker" /tmp/worker.log 2>/dev/null && { echo "[start] worker registered — bringing up voice_ws bridge"; break; }
  kill -0 "$WK_PID" 2>/dev/null || { echo "[start] worker exited before registering" >&2; break; }
  sleep 0.5
done

# 3. voice_ws bridge.
uv run --no-sync uvicorn app.bridge:app --host 0.0.0.0 --port "$PORT" &
WEB_PID=$!

cleanup() { kill "$LK_PID" "$WK_PID" "$WEB_PID" "$TAIL_PID" 2>/dev/null || true; }
trap 'cleanup; exit 143' TERM INT

wait -n
status=$?
echo "[start] a peer exited (status=$status) — shutting down siblings"
cleanup
wait || true
exit "$status"

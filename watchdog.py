#!/usr/bin/env python3
"""
falix-watchdog — 24/7 no-captcha auto-start watchdog for FalixNodes.

How it works:
  1. Checks server state via GET /servers/{id}/status.
  2. If state is 'running' or 'starting', does nothing (never interrupts traffic).
  3. If state is 'stopping', polls every 5s until it reaches 'offline'.
  4. When 'offline', mints a console WebSocket token and sends:
       {"event": "set state", "args": ["start"]}
     directly to the node WebSocket, bypassing the REST API captcha.
"""

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.request

API = "https://client.falixnodes.net/api/v2"
ALIVE = ("running", "starting")
BUSY = ("installing", "suspended", "restoring", "transferring")


def log(*parts):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}]", *parts, flush=True)


def api(key, path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        API + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "User-Agent": "falix-watchdog/2.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read().decode()).get("error", {})
        except Exception:
            err = {"code": f"http_{e.code}"}
        return {"error": err}
    except Exception as e:
        return {"error": {"code": type(e).__name__}}


def get_status(key, sid):
    r = api(key, f"/servers/{sid}/status")
    if "error" in r:
        return None, 0, r["error"].get("code", "unknown")
    d = r.get("data", {})
    state = d.get("state")
    uptime = (d.get("resources") or {}).get("uptime_seconds", 0)
    return state, uptime, None


async def ws_start(key, sid):
    """Mint a console token and send 'set state start' over the node WebSocket."""
    import websockets

    sess = api(key, f"/servers/{sid}/console/token", method="POST", body={})
    if "error" in sess:
        log("could not mint console token:", sess["error"].get("code"))
        return False
    s = sess["data"]

    try:
        async with websockets.connect(s["socket"], origin=None, open_timeout=25) as ws:
            await ws.send(json.dumps({"event": "auth", "args": [s["token"]]}))

            # Wait up to 5s for auth confirmation
            auth_deadline = time.time() + 5
            while time.time() < auth_deadline:
                try:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    if msg.get("event") == "auth success":
                        break
                except asyncio.TimeoutError:
                    break

            log("sending 'set state start' over WebSocket...")
            await ws.send(json.dumps({"event": "set state", "args": ["start"]}))

            deadline = time.time() + 45
            seen = None
            while time.time() < deadline:
                try:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                except asyncio.TimeoutError:
                    continue
                if msg.get("event") == "status":
                    seen = (msg.get("args") or [None])[0]
                    if seen in ALIVE:
                        log(f"node reported status='{seen}' ✅")
                        return True
            log(f"start sent; last seen status='{seen}'")
            return seen in ALIVE
    except Exception as e:
        log(f"websocket error: {type(e).__name__}")
        return False


def check_and_heal(key, sid):
    state, uptime, err = get_status(key, sid)
    if err:
        log(f"status check failed ({err}) — will retry")
        return False

    if state in ALIVE:
        log(f"state={state} (uptime={uptime}s) — OK")
        return True

    if state in BUSY:
        log(f"state={state} — server busy, skipping")
        return True

    if state == "stopping":
        log("state=stopping — waiting for server to reach 'offline' before restarting...")
        for _ in range(20):
            time.sleep(5)
            state, _, err = get_status(key, sid)
            if state == "offline":
                break
            if state in ALIVE:
                log(f"state={state} — already back up ✅")
                return True

    log(f"state={state} — waking server up via console WebSocket...")
    for attempt in range(1, 4):
        ok = asyncio.run(ws_start(key, sid))
        time.sleep(6)
        new_state, _, _ = get_status(key, sid)
        if ok or new_state in ALIVE:
            log(f"recovery succeeded on attempt {attempt} (state={new_state}) 🎉")
            return True
        log(f"attempt {attempt} ended with state={new_state}; retrying in 8s...")
        time.sleep(8)

    log("recovery did not reach running state after 3 attempts")
    return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--key", default=os.environ.get("FALIX_KEY"))
    p.add_argument("--server", default=os.environ.get("FALIX_SERVER_ID"))
    p.add_argument("--loop", type=int, default=0,
                   help="seconds between checks (0 = single check)")
    p.add_argument("--max-seconds", type=int, default=0,
                   help="exit cleanly after N seconds when --loop > 0 (0 = run forever)")
    a = p.parse_args()

    if not a.key or not a.server:
        log("FATAL: missing FALIX_KEY or FALIX_SERVER_ID")
        sys.exit(2)

    if a.loop <= 0:
        check_and_heal(a.key, a.server)
        sys.exit(0)

    start_ts = time.time()
    log(f"watchdog started (interval={a.loop}s, max_seconds={a.max_seconds or 'inf'})")
    while True:
        try:
            check_and_heal(a.key, a.server)
        except Exception as e:
            log(f"unexpected error: {type(e).__name__}")

        if a.max_seconds > 0 and (time.time() - start_ts + a.loop) >= a.max_seconds:
            log("max-seconds reached; exiting cleanly for next scheduled run")
            sys.exit(0)
        time.sleep(a.loop)


if __name__ == "__main__":
    main()

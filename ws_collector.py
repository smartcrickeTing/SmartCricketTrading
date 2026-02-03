# ==============================================================================
# SRL LIVE VACUUM — MARKET SUBSCRIBING + LIVE TELEMETRY
# ==============================================================================

import asyncio
import websockets
import json
import os
import time
from datetime import datetime, timezone
from google.colab import drive

# -----------------------------
# SETUP
# -----------------------------
if not os.path.exists("/content/drive"):
    drive.mount("/content/drive")

WS_URL = "wss://betex-dfbt.kryptium.io/feedhub"
RECORD_SEPARATOR = "\x1e"
OUTPUT_FILE = "/content/drive/MyDrive/srl_ws_collector.jsonl"

# -----------------------------
# STATE
# -----------------------------
SRL_EVENT_IDS = set()          # internal ids accepted
SUBSCRIBED_EVENT_IDS = set()  # ids subscribed
BUFFER = []

EVENT_STATS = {}              # eventId -> counters

invocation_id = 0
msg_count = 0


# -----------------------------
# HELPERS
# -----------------------------
def is_srl_event(payload):
    if payload.get("eventType") != "Match":
        return False

    text = (
        str(payload.get("homeTeam")) +
        str(payload.get("awayTeam")) +
        str(payload.get("competitionName"))
    ).lower()

    return "srl" in text or "simulated" in text


def save_to_disk():
    global BUFFER
    if not BUFFER:
        return

    try:
        with open(OUTPUT_FILE, "a") as f:
            for line in BUFFER:
                f.write(json.dumps(line) + "\n")
            f.flush()
            os.fsync(f.fileno())
        BUFFER = []
    except Exception as e:
        print(f"Write Error: {e}")


async def subscribe_all_markets(ws, event_id):
    global invocation_id

    sub = {
        "arguments": [event_id],
        "invocationId": str(invocation_id),
        "target": "SubscribeEventAllMarkets",
        "type": 1
    }

    await ws.send(json.dumps(sub) + RECORD_SEPARATOR)
    invocation_id += 1

    print(f"\n📡 SUBSCRIBE | eventId={event_id}", flush=True)


def init_event_stats(eid):
    EVENT_STATS.setdefault(
        eid,
        {"messages": 0, "markets": set(), "odds": 0}
    )


# -----------------------------
# MAIN LOOP
# -----------------------------
async def run_vacuum():
    global invocation_id, msg_count

    print("🚀 SRL VACUUM STARTED")

    with open(OUTPUT_FILE, "w") as f:
        f.write(json.dumps({
            "meta": "SESSION_START",
            "ts": datetime.now().isoformat()
        }) + "\n")

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                max_size=None,
                ping_interval=20
            ) as ws:

                # -----------------------------
                # HANDSHAKE
                # -----------------------------
                handshake_req = {"protocol": "json", "version": 1}
                await ws.send(json.dumps(handshake_req) + RECORD_SEPARATOR)

                await ws.recv()

                init_req = {
                    "arguments": [
                        datetime.now(timezone.utc).isoformat(),
                        None,
                        None,
                        None,
                        None,
                        None,
                        "betex-dfbt.kryptium.io"
                    ],
                    "invocationId": str(invocation_id),
                    "streamIds": [],
                    "target": "InitFeed",
                    "type": 1
                }

                await ws.send(json.dumps(init_req) + RECORD_SEPARATOR)
                invocation_id += 1

                print("✅ CONNECTED — LISTENING")

                # -----------------------------
                # DATA LOOP
                # -----------------------------
                async for message in ws:
                    for raw in message.split(RECORD_SEPARATOR):

                        if not raw.strip():
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        target = msg.get("target")
                        args = msg.get("arguments", [])
                        payload = args[0] if args else None

                        if not payload:
                            continue

                        should_save = False

                        # -----------------------------
                        # EVENT DISCOVERY
                        # -----------------------------
                        if target == "E" and is_srl_event(payload):

                            iid = payload.get("id")
                            status = payload.get("status") or payload.get("phase")

                            ms = payload.get("matchStatus") or {}
                            score = ""
                            if ms:
                                score = f"{ms.get('homeScore')} / {ms.get('awayScore')}"

                            if iid:
                                SRL_EVENT_IDS.add(iid)
                                init_event_stats(iid)

                                if iid not in SUBSCRIBED_EVENT_IDS:
                                    await subscribe_all_markets(ws, iid)
                                    SUBSCRIBED_EVENT_IDS.add(iid)

                            should_save = True

                            print(
                                f"\r🎯 EVENT | id={iid} | {status} | "
                                f"{payload.get('homeTeam')} vs {payload.get('awayTeam')} | "
                                f"{score}",
                                end="",
                                flush=True
                            )

                        # -----------------------------
                        # MARKET / ODDS FLOW
                        # -----------------------------
                        elif target in ["M", "O"]:

                            eid = payload.get("eventId") or payload.get("eId")

                            if eid in SRL_EVENT_IDS:

                                should_save = True
                                init_event_stats(eid)

                                EVENT_STATS[eid]["messages"] += 1

                                if target == "M":
                                    mid = payload.get("id") or payload.get("marketId")
                                    EVENT_STATS[eid]["markets"].add(mid)

                                    print(
                                        f"\n📈 MARKET | event={eid} | "
                                        f"{payload.get('name')} | "
                                        f"status={payload.get('status')}",
                                        flush=True
                                    )

                                elif target == "O":
                                    EVENT_STATS[eid]["odds"] += 1

                        # -----------------------------
                        # SAVE
                        # -----------------------------
                        if should_save:

                            msg["_captured_ts"] = datetime.now().isoformat()
                            BUFFER.append(msg)
                            msg_count += 1

                            if len(BUFFER) >= 20:
                                save_to_disk()

                            # periodic telemetry
                            if msg_count % 250 == 0:
                                for eid, st in EVENT_STATS.items():
                                    print(
                                        f"\n📊 STATS | event={eid} | "
                                        f"msgs={st['messages']} | "
                                        f"markets={len(st['markets'])} | "
                                        f"odds={st['odds']}",
                                        flush=True
                                    )

        except Exception as e:
            print(f"\n⚠️ RECONNECTING: {e}")
            time.sleep(5)


await run_vacuum()

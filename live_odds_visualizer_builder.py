# ==============================================================================
# LIVE ODDS VISUALIZER BUILDER — JSONL TAIL + REALTIME PLOTS
# ==============================================================================

import json
import time
from collections import defaultdict, deque
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
JSONL_FILE = "/mnt/data/srl_ws_collector.jsonl"
REFRESH_SEC = 2
WINDOW_POINTS = 300   # rolling window per line

# -----------------------------
# STATE
# -----------------------------
EVENTS = {}          # eventId -> meta
MARKETS = {}         # marketId -> meta
EVENT_MARKETS = defaultdict(set)

# odds[eventId][marketId][runnerId] -> deque
ODDS = defaultdict(lambda: defaultdict(lambda: defaultdict(
    lambda: deque(maxlen=WINDOW_POINTS)
)))

LAST_POS = 0


# -----------------------------
# HELPERS
# -----------------------------
def classify_market(name: str):
    n = name.lower()

    if "winner" in n:
        return "WINNER"

    if "1st innings" in n:
        return "TOTALS"

    return "OTHER"


# -----------------------------
# JSONL TAIL
# -----------------------------
def read_new_lines(path: Path):
    global LAST_POS

    with path.open("r") as f:
        f.seek(LAST_POS)

        while True:
            line = f.readline()
            if not line:
                break

            LAST_POS = f.tell()
            yield line


# -----------------------------
# MATPLOTLIB SETUP
# -----------------------------
plt.ion()
FIGS = {}   # eventId -> (fig, axes)


def ensure_figure(event_id):
    if event_id in FIGS:
        return FIGS[event_id]

    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle(f"Event {event_id}")

    axes[0].set_title("WINNER")
    axes[1].set_title("TOTALS")

    FIGS[event_id] = (fig, axes)
    return fig, axes


# -----------------------------
# UPDATE PLOTS
# -----------------------------
def refresh_plots():

    for eid, markets in EVENT_MARKETS.items():

        fig, axes = ensure_figure(eid)

        for ax in axes:
            ax.clear()

        axes[0].set_title("WINNER")
        axes[1].set_title("TOTALS")

        for mid in markets:

            meta = MARKETS.get(mid)
            if not meta:
                continue

            mtype = meta["type"]
            ax = axes[0] if mtype == "WINNER" else axes[1]

            for rid, series in ODDS[eid][mid].items():

                if len(series) < 2:
                    continue

                xs = range(len(series))
                ys = list(series)

                label = f"{meta['name']} | r{rid}"
                ax.plot(xs, ys, label=label)

        for ax in axes:
            ax.legend(fontsize=7)
            ax.grid(True)

        fig.canvas.draw()
        fig.canvas.flush_events()


# -----------------------------
# MAIN LOOP
# -----------------------------
print("📊 LIVE ODDS VISUALIZER STARTED")

path = Path(JSONL_FILE)

while True:

    for line in read_new_lines(path):

        try:
            row = json.loads(line)
        except Exception:
            continue

        if "target" not in row:
            continue

        payload = row.get("arguments", [None])[0]
        if not payload:
            continue

        t = row["target"]

        # -----------------------------
        # EVENT
        # -----------------------------
        if t == "E":

            eid = payload.get("id")

            if eid:
                EVENTS[eid] = payload

        # -----------------------------
        # MARKET
        # -----------------------------
        elif t == "M":

            mid = payload.get("id")
            eid = payload.get("eventId")

            if mid and eid:

                name = payload.get("name", "")
                mtype = classify_market(name)

                MARKETS[mid] = {
                    "eventId": eid,
                    "name": name,
                    "type": mtype
                }

                EVENT_MARKETS[eid].add(mid)

        # -----------------------------
        # ODDS
        # -----------------------------
        elif t == "O":

            eid = payload.get("eId")
            mid = payload.get("mId")

            if not eid or not mid:
                continue

            for side in payload.get("back", []):

                rid = payload.get("rId")
                odds = side.get("odds")

                if odds:
                    ODDS[eid][mid][rid].append(odds)

    refresh_plots()
    time.sleep(REFRESH_SEC)

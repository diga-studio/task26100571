"""ChalkSync: layer-clear and timed-undo replication kernel.

Up to 16 replicas, two fixed layers. Replicas exchange only serialized
events; no shared state. All state lives in memory.
"""

from __future__ import annotations

UNDO_WINDOW_MS = 10_000
MAX_REPLICAS = 16
LAYERS = (0, 1)
MAX_EVENT_BYTES = 4096


class Replica:
    """One ChalkSync replica.

    emit("add", now_ms, layer=..., content=...) adds a stroke and returns
    the event. emit("clear", now_ms, layer=...) hides every add on that
    layer that is currently effective on this replica (including ones
    already hidden by other clears); the covered set is frozen at emit
    time. emit("undo", now_ms, target=clear_seq) cancels the hiding
    effect of one locally generated clear; it is only issuable while
    now_ms - clear_emit_ms < UNDO_WINDOW_MS and returns None (without
    changing state) when rejected.
    """

    def __init__(self, replica_id: int) -> None:
        if not isinstance(replica_id, int) or not 0 <= replica_id < MAX_REPLICAS:
            raise ValueError("replica_id must be an int in [0, 16)")
        self.rid = replica_id
        self.seq = 0
        # author -> highest applied seq (causal frontier of applied events)
        self.clock: dict[int, int] = {}
        # layer -> stroke_id -> [content, active_cover_count]
        self.strokes: dict[int, dict[str, list]] = {layer: {} for layer in LAYERS}
        # (author, seq) -> (layer, [covered stroke ids]); coverage is frozen
        self.clears: dict[tuple[int, int], tuple[int, list]] = {}
        self.undone: set[tuple[int, int]] = set()
        self.pending: list[dict] = []
        # local emit time (ms) of own clears, for the undo window
        self.clear_times: dict[int, int] = {}

    def emit(self, action: str, now_ms: int, **kwargs) -> dict | None:
        if action == "add":
            layer = kwargs["layer"]
            self._check_layer(layer)
            extra = {"l": layer, "id": f"{self.rid}:{self.seq + 1}",
                     "d": kwargs["content"]}
        elif action == "clear":
            layer = kwargs["layer"]
            self._check_layer(layer)
            extra = {"l": layer}
        elif action == "undo":
            target = kwargs["target"]
            if target not in self.clear_times:
                raise ValueError("undo target is not a local clear")
            if now_ms - self.clear_times[target] >= UNDO_WINDOW_MS:
                return None
            extra = {"t": target}
        else:
            raise ValueError(f"unknown action: {action!r}")

        self.seq += 1
        event = {"k": action, "r": self.rid, "s": self.seq,
                 "c": [[a, n] for a, n in self.clock.items()]}
        event.update(extra)
        if action == "clear":
            self.clear_times[self.seq] = now_ms
        self._apply(event)
        return event

    def receive(self, event: dict) -> None:
        if self.clock.get(event["r"], 0) >= event["s"]:
            return  # duplicate
        if any(p["r"] == event["r"] and p["s"] == event["s"]
               for p in self.pending):
            return  # already buffered
        if self._ready(event):
            self._apply(event)
            self._drain()
        else:
            self.pending.append(event)

    def visible(self, layer: int) -> list:
        self._check_layer(layer)
        out = [(sid, entry[0]) for sid, entry in self.strokes[layer].items()
               if entry[1] == 0]
        out.sort()
        return out

    @staticmethod
    def _check_layer(layer: int) -> None:
        if layer not in LAYERS:
            raise ValueError(f"layer must be one of {LAYERS}")

    def _ready(self, event: dict) -> bool:
        if self.clock.get(event["r"], 0) != event["s"] - 1:
            return False
        return all(self.clock.get(a, 0) >= n for a, n in event["c"])

    def _drain(self) -> None:
        progressed = True
        while progressed:
            progressed = False
            for event in list(self.pending):
                if self._ready(event):
                    self.pending.remove(event)
                    self._apply(event)
                    progressed = True

    def _apply(self, event: dict) -> None:
        kind = event["k"]
        if kind == "add":
            self.strokes[event["l"]][event["id"]] = [event["d"], 0]
        elif kind == "clear":
            layer = event["l"]
            # coverage is frozen at generation time: exactly the adds the
            # generator had effective, i.e. those dominated by the event's
            # causal clock. Causal delivery guarantees they are already
            # applied here; concurrent/later adds are never covered.
            ctx = dict(event["c"])
            covered = []
            for sid in self.strokes[layer]:
                author, _, sseq = sid.rpartition(":")
                if ctx.get(int(author), 0) >= int(sseq):
                    covered.append(sid)
            for sid in covered:
                self.strokes[layer][sid][1] += 1
            self.clears[(event["r"], event["s"])] = (layer, covered)
        elif kind == "undo":
            key = (event["r"], event["t"])
            if key not in self.undone:
                self.undone.add(key)
                layer, covered = self.clears[key]
                for sid in covered:
                    self.strokes[layer][sid][1] -= 1
        else:
            raise ValueError(f"unknown event kind: {kind!r}")
        self.clock[event["r"]] = event["s"]

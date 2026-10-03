"""Automated tests for the ChalkSync kernel (stdlib unittest).

Run with:  python3 -m unittest -v
"""

import json
import random
import unittest

from chalksync import LAYERS, MAX_EVENT_BYTES, Replica


def wire(event):
    """Simulate the network: every event crosses a JSON round trip."""
    return json.loads(json.dumps(event))


def deliver(replica, *events):
    for event in events:
        replica.receive(wire(event))


class OverlappingClearsTest(unittest.TestCase):
    """Three replicas share old strokes, partition, then two replicas
    issue overlapping clears while the third concurrently adds a stroke."""

    def setUp(self):
        self.a, self.b, self.c = Replica(0), Replica(1), Replica(2)
        # shared old strokes on layer 0, known to all three replicas
        self.old1 = self.a.emit("add", 0, layer=0, content="old-1")
        self.old2 = self.a.emit("add", 1, layer=0, content="old-2")
        deliver(self.b, self.old1, self.old2)
        deliver(self.c, self.old1, self.old2)
        # partition: no delivery between replicas until later
        self.clear_a = self.a.emit("clear", 100, layer=0)
        self.clear_b = self.b.emit("clear", 100, layer=0)
        self.new_c = self.c.emit("add", 100, layer=0, content="new-c")

    def test_overlapping_clears_and_concurrent_add(self):
        a, b, c = self.a, self.b, self.c
        # locally each replica already reflects its own operations
        self.assertEqual(a.visible(0), [])
        self.assertEqual(b.visible(0), [])
        self.assertEqual(c.visible(0),
                         [("0:1", "old-1"), ("0:2", "old-2"), ("2:1", "new-c")])
        # phase 1: exchange the two clears and the concurrent add,
        # shuffled and duplicated per receiver
        rng = random.Random(42)
        for replica in (a, b, c):
            events = [e for e in (self.clear_a, self.clear_b, self.new_c)
                      if e["r"] != replica.rid]
            events = events + events  # duplicates
            rng.shuffle(events)
            deliver(replica, *events)
        # both clears are effective everywhere: old strokes hidden,
        # the concurrent add was never observed by either clear
        for replica in (a, b, c):
            self.assertEqual(replica.visible(0), [("2:1", "new-c")])
        # phase 2: only A undoes its clear; old strokes stay hidden
        # because B's clear still covers them
        undo_a = a.emit("undo", 5000, target=self.clear_a["s"])
        self.assertIsNotNone(undo_a)
        deliver(b, undo_a)
        deliver(c, undo_a)
        for replica in (a, b, c):
            self.assertEqual(replica.visible(0), [("2:1", "new-c")])
        # phase 3: B undoes too; old strokes reappear, ids unchanged
        undo_b = b.emit("undo", 5000, target=self.clear_b["s"])
        self.assertIsNotNone(undo_b)
        deliver(a, undo_b)
        deliver(c, undo_b)
        expected = [("0:1", "old-1"), ("0:2", "old-2"), ("2:1", "new-c")]
        for replica in (a, b, c):
            self.assertEqual(replica.visible(0), expected)
        # per-stroke pairwise comparison across all replicas
        views = [r.visible(0) for r in (a, b, c)]
        for left, right in zip(views, views[1:]):
            self.assertEqual(dict(left), dict(right))

    def test_convergence_under_shuffle_and_duplicates(self):
        # a fresh replica receiving the same event set in random order
        # with duplicates must converge to the same view
        undo_a = self.a.emit("undo", 5000, target=self.clear_a["s"])
        undo_b = self.b.emit("undo", 5000, target=self.clear_b["s"])
        all_events = [self.old1, self.old2, self.clear_a, self.clear_b,
                      self.new_c, undo_a, undo_b]
        rng = random.Random(7)
        shuffled = all_events * 3
        rng.shuffle(shuffled)
        fresh = Replica(3)
        deliver(fresh, *shuffled)
        self.assertEqual(fresh.visible(0),
                         [("0:1", "old-1"), ("0:2", "old-2"), ("2:1", "new-c")])


class CausalOrderingTest(unittest.TestCase):
    def test_gap_then_late_clear_does_not_cover_unobserved(self):
        a, b = Replica(0), Replica(1)
        add1 = a.emit("add", 0, layer=0, content="a1")
        add2 = a.emit("add", 1, layer=0, content="a2")
        # deliver the later same-author event first: it must not apply
        deliver(b, add2)
        self.assertEqual(b.visible(0), [])
        # B clears while the gap is still missing: the clear observes
        # nothing, so it must not cover add1/add2 once they arrive
        clear_b = b.emit("clear", 10, layer=0)
        deliver(b, add1)  # fills the gap; add2 now applies too
        self.assertEqual(b.visible(0), [("0:1", "a1"), ("0:2", "a2")])
        # A receives B's clear: B generated it without observing A's
        # adds, so they survive on A as well
        deliver(a, clear_b)
        self.assertEqual(a.visible(0), [("0:1", "a1"), ("0:2", "a2")])

    def test_cross_author_causal_dependency(self):
        a, b, c = Replica(0), Replica(1), Replica(2)
        add = a.emit("add", 0, layer=1, content="shared")
        deliver(b, add)
        clear_b = b.emit("clear", 5, layer=1)  # causally after add
        # C gets the clear before the add it depends on: must not apply
        deliver(c, clear_b)
        self.assertEqual(c.visible(1), [])
        deliver(c, add)
        self.assertEqual(c.visible(1), [])  # add applied, but covered
        self.assertEqual(b.visible(1), [])
        self.assertEqual(a.visible(1), [("0:1", "shared")])

    def test_undo_arrives_before_clear(self):
        a, b = Replica(0), Replica(1)
        add = a.emit("add", 0, layer=0, content="x")
        clear = a.emit("clear", 10, layer=0)
        undo = a.emit("undo", 20, target=clear["s"])
        self.assertIsNotNone(undo)
        # undo delivered first: no effect yet, must wait for the clear
        deliver(b, undo)
        self.assertEqual(b.visible(0), [])
        deliver(b, add)
        self.assertEqual(b.visible(0), [("0:1", "x")])
        deliver(b, clear)
        self.assertEqual(b.visible(0), [("0:1", "x")])


class UndoWindowTest(unittest.TestCase):
    def test_window_boundaries(self):
        a = Replica(0)
        a.emit("add", 0, layer=0, content="s")
        clear1 = a.emit("clear", 1000, layer=0)
        # 9999 ms after the clear: allowed
        undo_ok = a.emit("undo", 1000 + 9999, target=clear1["s"])
        self.assertIsNotNone(undo_ok)
        self.assertEqual(a.visible(0), [("0:1", "s")])
        # exactly 10000 ms after a clear: rejected, state unchanged
        clear2 = a.emit("clear", 2000, layer=0)
        self.assertEqual(a.visible(0), [])
        undo_late = a.emit("undo", 2000 + 10000, target=clear2["s"])
        self.assertIsNone(undo_late)
        self.assertEqual(a.visible(0), [])
        # well past the window: also rejected
        self.assertIsNone(a.emit("undo", 2000 + 10001, target=clear2["s"]))
        self.assertEqual(a.visible(0), [])

    def test_legal_undo_valid_when_delivered_a_day_late(self):
        a, b = Replica(0), Replica(1)
        add = a.emit("add", 0, layer=0, content="s")
        clear = a.emit("clear", 1000, layer=0)
        undo = a.emit("undo", 1000 + 9999, target=clear["s"])
        self.assertIsNotNone(undo)
        deliver(b, add, clear)
        self.assertEqual(b.visible(0), [])
        # the undo only reaches B a day later; still fully effective
        deliver(b, undo)  # arrival time is irrelevant to validity
        self.assertEqual(b.visible(0), [("0:1", "s")])

    def test_unrevoked_clear_outlives_window(self):
        a, b = Replica(0), Replica(1)
        add = a.emit("add", 0, layer=0, content="s")
        clear = a.emit("clear", 0, layer=0)
        deliver(b, add, clear)
        # long past the window with no undo: still hidden everywhere
        self.assertIsNone(a.emit("undo", 10_000_000, target=clear["s"]))
        self.assertEqual(a.visible(0), [])
        self.assertEqual(b.visible(0), [])


class ScaleAndSizeTest(unittest.TestCase):
    def test_event_size_limit_with_100k_strokes(self):
        a, b = Replica(0), Replica(1)
        adds = [a.emit("add", i, layer=0, content=f"stroke-{i}")
                for i in range(100_000)]
        deliver(b, *adds)
        self.assertEqual(len(b.visible(0)), 100_000)
        clear = a.emit("clear", 100_000, layer=0)
        self.assertEqual(a.visible(0), [])
        undo = a.emit("undo", 100_000 + 9999, target=clear["s"])
        self.assertIsNotNone(undo)
        # clear and undo events stay tiny regardless of stroke count:
        # no per-stroke target lists
        for event in (clear, undo):
            size = len(json.dumps(event).encode("utf-8"))
            self.assertLessEqual(size, MAX_EVENT_BYTES)
        deliver(b, clear)
        self.assertEqual(b.visible(0), [])
        deliver(b, undo)
        self.assertEqual(len(a.visible(0)), 100_000)
        self.assertEqual(a.visible(0), b.visible(0))


class BasicSemanticsTest(unittest.TestCase):
    def test_layers_are_independent(self):
        a = Replica(0)
        a.emit("add", 0, layer=0, content="on-0")
        a.emit("add", 1, layer=1, content="on-1")
        a.emit("clear", 2, layer=0)
        self.assertEqual(a.visible(0), [])
        self.assertEqual(a.visible(1), [("0:2", "on-1")])

    def test_invalid_inputs(self):
        a = Replica(0)
        with self.assertRaises(ValueError):
            a.emit("add", 0, layer=7, content="bad")
        with self.assertRaises(ValueError):
            a.emit("bogus", 0)
        with self.assertRaises(ValueError):
            a.emit("undo", 0, target=99)
        with self.assertRaises(ValueError):
            a.visible(7)
        with self.assertRaises(ValueError):
            Replica(16)
        self.assertEqual(LAYERS, (0, 1))

    def test_offline_replica_is_self_sufficient(self):
        a = Replica(15)
        a.emit("add", 0, layer=0, content="solo")
        clear = a.emit("clear", 1, layer=0)
        a.emit("undo", 2, target=clear["s"])
        self.assertEqual(a.visible(0), [("15:1", "solo")])


if __name__ == "__main__":
    unittest.main()

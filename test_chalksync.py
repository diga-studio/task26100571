"""Tests for chalksync. Run: python3 -m unittest test_chalksync -v"""

import json
import random
import unittest

from chalksync import MAX_REPLICAS, UNDO_WINDOW_MS, Replica


def wire(event):
    """Simulate the network: independent JSON serialization per event."""
    return json.loads(json.dumps(event))


def broadcast(replicas, event, times=1):
    for replica in replicas:
        for _ in range(times):
            replica.receive(wire(event))


def assert_same_view(testcase, replicas, layer):
    views = [r.visible(layer) for r in replicas]
    for view in views[1:]:
        testcase.assertEqual(views[0], view)
    return views[0]


class OverlappingClearsTest(unittest.TestCase):
    """Three replicas share old strokes, partition, two overlap clears,
    the third concurrently adds. Staged out-of-order + duplicate delivery."""

    def test_overlapping_clears(self):
        A, B, C = (Replica(name) for name in "ABC")
        all_replicas = [A, B, C]

        # Phase 0: shared old strokes on layer 0.
        old1 = A.emit({"op": "add", "layer": 0, "content": {"text": "old-1"}}, 0)
        old2 = A.emit({"op": "add", "layer": 0, "content": {"text": "old-2"}}, 1)
        broadcast(all_replicas, old1)
        broadcast(all_replicas, old2, times=2)  # duplicates are harmless
        view = assert_same_view(self, all_replicas, 0)
        self.assertEqual([sid for sid, _ in view], ["A:1", "A:2"])

        # Partition. B and C each clear layer 0; A concurrently adds.
        clear_b = B.emit({"op": "clear", "layer": 0}, 100)
        clear_c = C.emit({"op": "clear", "layer": 0}, 200)
        new_a = A.emit({"op": "add", "layer": 0, "content": {"text": "new"}}, 2)

        # Phase 1: deliver clears out of order, with duplicates.
        for replica in all_replicas:
            replica.receive(wire(clear_c))
            replica.receive(wire(clear_b))
            replica.receive(wire(clear_c))  # duplicate
        # Both old strokes hidden on B and C; A additionally sees its own
        # concurrent add, which neither clear observed.
        self.assertEqual(B.visible(0), [])
        self.assertEqual(C.visible(0), [])
        self.assertEqual(A.visible(0), [("A:3", {"text": "new"})])

        # The concurrent add was never observed by either clear.
        broadcast(all_replicas, new_a)
        view = assert_same_view(self, all_replicas, 0)
        self.assertEqual(view, [("A:3", {"text": "new"})])

        # Phase 2: undo only B's clear -> old strokes stay hidden (C's clear).
        undo_b = B.emit({"op": "undo", "target": clear_b}, 100 + 5000)
        self.assertIsNotNone(undo_b)
        broadcast(all_replicas, undo_b, times=2)
        view = assert_same_view(self, all_replicas, 0)
        self.assertEqual(view, [("A:3", {"text": "new"})])

        # Phase 3: undo C's clear too -> old strokes restored, ids intact.
        undo_c = C.emit({"op": "undo", "target": clear_c}, 200 + 9999)
        self.assertIsNotNone(undo_c)
        # Staged shuffled delivery with duplicates.
        for replica in [C, A, B]:
            replica.receive(wire(undo_c))
            replica.receive(wire(undo_c))
        view = assert_same_view(self, all_replicas, 0)
        self.assertEqual(
            view,
            [("A:1", {"text": "old-1"}),
             ("A:2", {"text": "old-2"}),
             ("A:3", {"text": "new"})],
        )


class CausalDeliveryTest(unittest.TestCase):
    def test_same_author_gap_not_observed_by_clear(self):
        A, B = Replica("A"), Replica("B")
        add1 = A.emit({"op": "add", "layer": 0, "content": "one"}, 0)
        add2 = A.emit({"op": "add", "layer": 0, "content": "two"}, 1)

        # Deliver the later same-author event first: it must be buffered.
        B.receive(wire(add2))
        self.assertEqual(B.visible(0), [])

        # B clears before the gap is filled: the clear observed nothing.
        clear = B.emit({"op": "clear", "layer": 0}, 10)
        self.assertEqual(clear["d"], {})

        # Fill the gap; the buffered add2 becomes applicable too.
        B.receive(wire(add1))
        # Neither add may be hidden by the clear that never observed them.
        self.assertEqual(
            B.visible(0), [("A:1", "one"), ("A:2", "two")])

        # A receives the clear afterwards and agrees.
        A.receive(wire(clear))
        self.assertEqual(A.visible(0), B.visible(0))

    def test_cross_author_causal_dependency(self):
        A, B, C = Replica("A"), Replica("B"), Replica("C")
        add = A.emit({"op": "add", "layer": 1, "content": "dep"}, 0)
        B.receive(wire(add))
        clear = B.emit({"op": "clear", "layer": 1}, 10)
        self.assertEqual(clear["d"], {"A": 1})

        # C gets the clear before its causal predecessor: no effect yet.
        C.receive(wire(clear))
        self.assertEqual(C.visible(1), [])
        C.receive(wire(add))
        self.assertEqual(C.visible(1), [])  # clear now applies and covers it

        # A concurrent add B never observed survives the clear.
        add2 = A.emit({"op": "add", "layer": 1, "content": "free"}, 1)
        A.receive(wire(clear))  # A catches up on the clear too
        for r in (B, C):
            r.receive(wire(add2))
        self.assertEqual(A.visible(1), [("A:2", "free")])
        self.assertEqual(B.visible(1), A.visible(1))
        self.assertEqual(C.visible(1), A.visible(1))

    def test_undo_arrives_before_clear(self):
        A, B = Replica("A"), Replica("B")
        add = A.emit({"op": "add", "layer": 0, "content": "x"}, 0)
        B.receive(wire(add))
        clear = B.emit({"op": "clear", "layer": 0}, 100)
        undo = B.emit({"op": "undo", "target": clear}, 200)

        # Undo first: buffered, stroke stays visible.
        A.receive(wire(undo))
        self.assertEqual(A.visible(0), [("A:1", "x")])
        # Clear arrives; both apply; net effect is "clear was undone".
        A.receive(wire(clear))
        A.receive(wire(undo))  # duplicate
        self.assertEqual(A.visible(0), [("A:1", "x")])
        self.assertEqual(A.visible(0), B.visible(0))


class UndoWindowTest(unittest.TestCase):
    def test_window_boundary_and_late_delivery(self):
        A, B = Replica("A"), Replica("B")
        add = A.emit({"op": "add", "layer": 0, "content": "s"}, 0)
        B.receive(wire(add))

        clear1 = B.emit({"op": "clear", "layer": 0}, 1_000)
        # 9999 ms after the clear: allowed.
        undo1 = B.emit({"op": "undo", "target": clear1}, 1_000 + 9_999)
        self.assertIsNotNone(undo1)

        clear2 = B.emit({"op": "clear", "layer": 0}, 5_000)
        # Exactly 10000 ms: rejected, no event, no state change.
        self.assertIsNone(B.emit({"op": "undo", "target": clear2}, 15_000))
        self.assertEqual(B.visible(0), [])
        # Later is also rejected.
        self.assertIsNone(B.emit({"op": "undo", "target": clear2}, 99_000))

        # A legal undo stays valid even delivered a day late.
        day_later = 1_000 + 86_400_000
        A.emit({"op": "add", "layer": 1, "content": "tick"}, day_later)
        A.receive(wire(clear1))
        self.assertEqual(A.visible(0), [])
        A.receive(wire(undo1))  # emitted at +9999, arrives a day late
        self.assertEqual(A.visible(0), [("A:1", "s")])

        # clear2 was never undone and keeps hiding.
        A.receive(wire(clear2))
        self.assertEqual(A.visible(0), [])

    def test_undo_requires_own_clear(self):
        A, B = Replica("A"), Replica("B")
        clear = A.emit({"op": "clear", "layer": 0}, 0)
        self.assertIsNone(B.emit({"op": "undo", "target": clear}, 1))
        self.assertIsNone(B.emit({"op": "undo", "target": 1}, 1))


class DeterminismTest(unittest.TestCase):
    def test_delivery_order_and_duplicates_irrelevant(self):
        base = Replica("base")
        events = []
        for i in range(30):
            events.append(base.emit(
                {"op": "add", "layer": i % 2, "content": {"n": i}}, i))
        events.append(base.emit({"op": "clear", "layer": 0}, 100))
        for i in range(30, 40):
            events.append(base.emit(
                {"op": "add", "layer": i % 2, "content": {"n": i}}, 100 + i))
        clear2 = base.emit({"op": "clear", "layer": 1}, 500)
        events.append(clear2)
        events.append(base.emit({"op": "undo", "target": clear2}, 600))

        expected = {layer: base.visible(layer) for layer in (0, 1)}
        for seed in range(5):
            replica = Replica("r")
            shuffled = [wire(e) for e in events]
            random.Random(seed).shuffle(shuffled)
            for event in shuffled:
                replica.receive(event)
                if seed % 2:
                    replica.receive(dict(event))  # duplicates
            for layer in (0, 1):
                self.assertEqual(replica.visible(layer), expected[layer])


class EventSizeTest(unittest.TestCase):
    def test_clear_and_undo_stay_small_with_100k_strokes(self):
        A, B = Replica("A"), Replica("B")
        adds = []
        for i in range(100_000):
            adds.append(A.emit(
                {"op": "add", "layer": i % 2, "content": i}, i))
        clear = A.emit({"op": "clear", "layer": 0}, 200_000)
        undo = A.emit({"op": "undo", "target": clear}, 200_001)
        for event in (clear, undo):
            encoded = json.dumps(event).encode("utf-8")
            self.assertLessEqual(len(encoded), 4096)

        # All 100k strokes are covered by the clear on layer 0 only.
        for event in adds:
            B.receive(wire(event))
        B.receive(wire(clear))
        self.assertEqual(B.visible(0), [])
        self.assertEqual(len(B.visible(1)), 50_000)
        B.receive(wire(undo))
        self.assertEqual(len(B.visible(0)), 50_000)
        self.assertEqual(B.visible(0), A.visible(0))
        self.assertEqual(B.visible(1), A.visible(1))


class ConstantsTest(unittest.TestCase):
    def test_limits(self):
        self.assertEqual(MAX_REPLICAS, 16)
        self.assertEqual(UNDO_WINDOW_MS, 10_000)


if __name__ == "__main__":
    unittest.main()

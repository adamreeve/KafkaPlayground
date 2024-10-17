from __future__ import annotations

import threading


class ProducerConsumerData:
    def __init__(self):
        self._consumed = 0
        self._produced = 0
        self._duplicated = 0
        self._out_of_order = 0
        self._lock = threading.Lock()

    def increment_consumed(self):
        with self._lock:
            self._consumed += 1

    def increment_produced(self):
        with self._lock:
            self._produced += 1

    def increment_duplicated(self):
        with self._lock:
            self._duplicated += 1

    def increment_out_of_order(self):
        with self._lock:
            self._out_of_order += 1

    def get_stats(self) -> (int, int, int, int):
        with self._lock:
            return self._consumed, self._produced, self._duplicated, self._out_of_order

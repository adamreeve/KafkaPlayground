from __future__ import annotations

import itertools
import logging
import time
import threading
from typing import Sequence


log = logging.getLogger(__name__)


def get_topic_name(stem: str, index: int) -> str:
    return f"{stem}-{index}"


def run_tasks(threads: Sequence[threading.Thread], shutdown: threading.Event):
    for thread in threads:
        thread.start()

    try:
        while True:
            time.sleep(0.2)
            if not all(t.is_alive() for t in threads):
                # Unexpected stop of thread
                log.info("Detected a stopped thread, stopping all tasks")
                break
    except KeyboardInterrupt:
        log.info("Ctrl-C detected, stopping all tasks")

    shutdown.set()
    for thread in threads:
        thread.join(timeout=10.0)


def batched(iterable, n):
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, n))
        if not batch:
            return
        yield batch
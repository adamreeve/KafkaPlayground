from __future__ import annotations

import logging
import time
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kafka_tool.data import ProducerConsumerData


log = logging.getLogger(__name__)


def run_reporter_task(
        data: ProducerConsumerData,
        shutdown: threading.Event):
    start = time.monotonic()
    prev_produced = 0
    prev_consumed = 0

    log.info("Running reporter task")

    while True:
        for _ in range(10):
            if not shutdown.is_set():
                time.sleep(1.0)
            else:
                return
        consumed, produced, duplicated, out_of_order = data.get_stats()
        newly_produced = produced - prev_produced
        newly_consumed = consumed - prev_consumed
        prev_produced = produced
        prev_consumed = consumed

        elapsed_s = int(time.monotonic() - start)
        log.info(
            "Elapsed: %d s, %d (+%d) messages produced, %d (+%d) messages consumed, %d duplicated, %d out of sequence.",
            elapsed_s, produced, newly_produced, consumed, newly_consumed, duplicated, out_of_order
        )

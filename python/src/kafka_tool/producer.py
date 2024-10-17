from __future__ import annotations

import threading
from typing import Dict, TYPE_CHECKING

from kafka_tool.data import ProducerConsumerData
from kafka_tool.reporter_task import run_reporter_task
from kafka_tool.utils import run_tasks

if TYPE_CHECKING:
    from kafka_tool.kafka_tasks import KafkaTasks
    from kafka_tool.settings import ProducerConsumerSettings


def run_producer(tasks: KafkaTasks, config: Dict[str, str], settings: ProducerConsumerSettings) -> None:
    tasks.recreate_topics(config, settings)
    data = ProducerConsumerData()
    shutdown = threading.Event()
    threads = [
        threading.Thread(target=tasks.run_producer_task, args=[config, settings, data, producer_index, shutdown])
        for producer_index in range(settings.producers)]
    threads.append(threading.Thread(target=run_reporter_task, args=[data, shutdown]))

    run_tasks(threads, shutdown)

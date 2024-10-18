from __future__ import annotations

import threading
from typing import Dict, TYPE_CHECKING

from kafka_tool.kafka_tasks import KafkaTasks
from kafka_tool.kafka_python.consumer_task import run_consumer_task

if TYPE_CHECKING:
    from kafka_tool import ProducerConsumerSettings
    from kafka_tool.data import ProducerConsumerData


class KafkaPythonTasks(KafkaTasks):
    @staticmethod
    def recreate_topics(config: Dict[str, str], settings: ProducerConsumerSettings):
        raise NotImplementedError()

    @staticmethod
    def run_consumer_task(
            config: Dict[str, str],
            settings: ProducerConsumerSettings,
            data: ProducerConsumerData,
            shutdown: threading.Event):
        run_consumer_task(config, settings, data, shutdown)

    @staticmethod
    def run_producer_task(
            config: Dict[str, str],
            settings: ProducerConsumerSettings,
            data: ProducerConsumerData,
            producer_index: int,
            shutdown: threading.Event):
        raise NotImplementedError()
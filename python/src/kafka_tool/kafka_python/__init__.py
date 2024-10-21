from __future__ import annotations

import threading
from typing import Dict, TYPE_CHECKING

from kafka_tool.kafka_tasks import KafkaTasks
from kafka_tool.kafka_python.admin import recreate_topics
from kafka_tool.kafka_python.consumer_task import run_consumer_task
from kafka_tool.kafka_python.producer_task import run_producer_task

if TYPE_CHECKING:
    from kafka_tool import ProducerConsumerSettings
    from kafka_tool.data import ProducerConsumerData


class KafkaPythonTasks(KafkaTasks):
    @staticmethod
    def recreate_topics(config: Dict[str, str], settings: ProducerConsumerSettings):
        recreate_topics(config, settings)

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
        run_producer_task(config, settings, data, producer_index, shutdown)

from __future__ import annotations

from abc import ABC, abstractmethod
import threading
from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from kafka_tool import ProducerConsumerSettings
    from kafka_tool.data import ProducerConsumerData


class KafkaTasks(ABC):
    """ Base class for abstracting Kafka operations """

    @staticmethod
    @abstractmethod
    def recreate_topics(config: Dict[str, str], settings: ProducerConsumerSettings):
        ...

    @staticmethod
    @abstractmethod
    def run_consumer_task(
            config: Dict[str, str],
            settings: ProducerConsumerSettings,
            data: ProducerConsumerData,
            shutdown: threading.Event):
        ...

    @staticmethod
    @abstractmethod
    def run_producer_task(
            config: Dict[str, str],
            settings: ProducerConsumerSettings,
            data: ProducerConsumerData,
            producer_index: int,
            shutdown: threading.Event):
        ...

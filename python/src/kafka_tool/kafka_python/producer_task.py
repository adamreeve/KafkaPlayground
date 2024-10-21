from __future__ import annotations

import logging
import time
import threading
from typing import Dict, TYPE_CHECKING

from kafka import KafkaProducer

from kafka_tool.kafka_python.admin import get_admin_client
from kafka_tool.kafka_python.config import config_to_kwargs
from kafka_tool.utils import get_topic_name

if TYPE_CHECKING:
    from kafka_tool.data import ProducerConsumerData
    from kafka_tool.settings import ProducerConsumerSettings


log = logging.getLogger(__name__)


def run_producer_task(
        config: Dict[str, str],
        settings: ProducerConsumerSettings,
        data: ProducerConsumerData,
        producer_index: int,
        shutdown: threading.Event):

    if settings.topics % settings.producers != 0:
        raise Exception(
            f"Cannot evenly schedule {settings.topics} topics on {settings.producers} producers")

    topics_per_producer = settings.topics // settings.producers

    producer = KafkaProducer(**config_to_kwargs(config, 'producer'))
    log.info("Running producer task %d", producer_index)

    exception = None

    def handle_error(err: Exception, topic: str, partition: int):
        nonlocal exception
        if err is not None:
            log.error(f"Delivery failed for {topic}: {err}")
            if exception is None:
                try:
                    admin_client = get_admin_client(config)
                    topic_metadata = admin_client.describe_topics(topics=[topic])
                    partitions_count = len(topic_metadata[0].partitions)
                except Exception:
                    partitions_count = -1
                exception = Exception(
                    f"DeliveryReport.Error, Exception = {err}"
                    f", topic = {topic}, partition = {partition}, partitionsCount = {partitions_count}"
                )

    messages_until_sleep = 0
    flush_counter = 0
    time_since_sleep = time.monotonic()

    try:
        current_value = 0
        while not shutdown.is_set():
            for topic_index in range(topics_per_producer):
                topic_name = get_topic_name(
                    settings.topic_stem, topic_index + producer_index * topics_per_producer)
                for k in range(settings.partitions * 7):
                    if exception is not None:
                        raise exception

                    if messages_until_sleep <= 0:
                        elapsed = time.monotonic() - time_since_sleep
                        if elapsed < 0.1:
                            time.sleep(0.1 - elapsed)
                        time_since_sleep = time.monotonic()
                        messages_until_sleep = settings.messages_per_second // 10

                    messages_until_sleep -= 1

                    fut = producer.send(
                        topic_name, key=str(k).encode('utf-8'), value=str(current_value).encode('utf-8'))
                    fut.add_errback(
                        handle_error, topic=topic_name, partition=fut._produce_future.topic_partition)

                    data.increment_produced()

                    flush_counter += 1
                    if flush_counter % 100_000 == 0:
                        log.info("Flushing producer task %d", producer_index)
                        producer.flush()
                        log.info("Flush of producer %d completed", producer_index)

            current_value += 1
    finally:
        producer.close()
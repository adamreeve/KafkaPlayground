from __future__ import annotations

import logging
import threading
from typing import Any, Dict, TYPE_CHECKING
import uuid

from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.fetcher import ConsumerRecord

from kafka_tool.utils import get_topic_name

if TYPE_CHECKING:
    from kafka_tool.data import ProducerConsumerData
    from kafka_tool.settings import ProducerConsumerSettings

log = logging.getLogger(__name__)


def run_consumer_task(
        config: Dict[str, str],
        settings: ProducerConsumerSettings,
        data: ProducerConsumerData,
        shutdown: threading.Event):
    topics = [get_topic_name(settings.topic_stem, i) for i in range (settings.topics)]
    topic_partitions = []
    for topic in topics:
        for partition in range(settings.partitions):
            topic_partitions.append(TopicPartition(topic, partition))

    group_id = str(uuid.uuid4())

    consumer_kwargs = {
        'auto_offset_reset': 'error',
        'enable_auto_commit': False,
    }
    consumer_kwargs.update(_config_to_consumer_args(config))

    consumer = KafkaConsumer(
        group_id=group_id,
        consumer_timeout_ms=200,
        **consumer_kwargs,
    )
    consumer.assign(topic_partitions)
    for tp in topic_partitions:
        consumer.seek(tp, 0)

    value_dictionary: Dict[(str, int), ConsumerRecord] = {}

    try:
        while not shutdown.is_set():
            for message in consumer:
                topic = message.topic
                key = int(message.key)
                value = int(message.value)
                value_key = (topic, key)
                try:
                    prev_message = value_dictionary[value_key]
                    prev_value = int(prev_message.value)
                    if value != prev_value + 1:
                        partition = message.partition
                        log.error(
                            "Unexpected message value, topic/k [p]=%s/%d %s, Offset=%d/%d, Timestamp=%d/%d,  previous value=%d, messageValue=%d",
                            topic, key, partition, prev_message.offset, message.offset,
                            prev_message.timestamp, message.timestamp, prev_value, value)

                    if value <= prev_value:
                        data.increment_duplicated()
                    if value > prev_value + 1:
                        data.increment_out_of_order()
                except KeyError:
                    pass
                value_dictionary[value_key] = message

                data.increment_consumed()

                if shutdown.is_set():
                    break
    finally:
        consumer.close()


def _config_to_consumer_args(config: Dict[str, str]) -> Dict[str, Any]:
    consumer_kwargs = {}
    for (config_key, arg, converter) in [
        ('bootstrap.servers', 'bootstrap_servers', lambda s: s.split(',')),
        ('max.in.flight.requests.per.connection', 'max_in_flight_requests_per_connection', int),
        ('request.timeout.ms', 'request_timeout_ms', int),
    ]:
        try:
            consumer_kwargs[arg] = converter(config[config_key])
        except KeyError:
            pass

    return consumer_kwargs

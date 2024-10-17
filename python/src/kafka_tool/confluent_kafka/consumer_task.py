from __future__ import annotations

import logging
import threading
from typing import Dict, TYPE_CHECKING
import uuid

from confluent_kafka import Consumer, TopicPartition

from kafka_tool.utils import get_topic_name

if TYPE_CHECKING:
    from confluent_kafka import Message
    from kafka_tool.data import ProducerConsumerData
    from kafka_tool.settings import ProducerConsumerSettings

log = logging.getLogger(__name__)


def run_consumer_task(
        config: Dict[str, str],
        settings: ProducerConsumerSettings,
        data: ProducerConsumerData,
        shutdown: threading.Event):

    consumer_config = {
        'group.id': str(uuid.uuid4()),
        'enable.auto.offset.store': False,
        'enable.auto.commit': False,
        'auto.offset.reset': 'error',
    }
    consumer_config.update(config)

    consumer = Consumer(consumer_config, logger=log)
    log.info("Running consumer task")

    topics = [get_topic_name(settings.topic_stem, i) for i in range (settings.topics)]
    topic_partitions = []
    for topic in topics:
        for partition in range(settings.partitions):
            topic_partitions.append(TopicPartition(topic, partition, offset=0))
    consumer.assign(topic_partitions)

    value_dictionary: Dict[(str, int), Message] = {}

    while not shutdown.is_set():
        messages = consumer.consume(num_messages=1, timeout=0.2)
        for message in messages:
            err = message.error()
            if err is not None:
                raise RuntimeError(f"Error consuming message: {err}")
            topic = message.topic()
            key = int(message.key())
            value = int(message.value())
            value_key = (topic, key)
            try:
                prev_message = value_dictionary[value_key]
                prev_value = int(prev_message.value())
                if value != prev_value + 1:
                    partition = message.partition()
                    log.error(
                        "Unexpected message value, topic/k [p]=%s/%d %s, Offset=%d/%d, LeaderEpoch=%d/%d,  previous value=%d, messageValue=%d",
                        topic, key, partition, prev_message.offset(), message.offset(), prev_message.leader_epoch(), message.leader_epoch(), prev_value, value)

                if value <= prev_value:
                    data.increment_duplicated()
                if value > prev_value + 1:
                    data.increment_out_of_order()
            except KeyError:
                pass
            value_dictionary[value_key] = message

            data.increment_consumed()

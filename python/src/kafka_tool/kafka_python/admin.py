from __future__ import annotations

import logging
import time
from typing import Dict, TYPE_CHECKING

from kafka_tool.utils import batched, get_topic_name
from kafka_tool.kafka_python.config import config_to_kwargs

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import NoError

if TYPE_CHECKING:
    from kafka_tool.settings import ProducerConsumerSettings

log = logging.getLogger(__name__)


def get_admin_client(config: Dict[str, str]):
    return KafkaAdminClient(**config_to_kwargs(config, 'consumer'))


def recreate_topics(config: Dict[str, str], settings: ProducerConsumerSettings):
    log.info("Recreating %d topics", settings.topics)
    required_topics = set(get_topic_name(settings.topic_stem, i) for i in range(settings.topics))
    admin_client = get_admin_client(config)
    existing_topics = admin_client.list_topics()
    batch_size = settings.recreate_topics_batch_size
    for batch in batched(
            required_topics.intersection(existing_topics), batch_size):
        log.info("Deleting a batch of %d topics", len(batch))
        response = admin_client.delete_topics(list(batch))
        errors = [e for e in response.topic_error_codes if e[1] != NoError.errno]
        if any(errors):
            raise Exception(f"Errors deleting topics: {errors}")

    time.sleep(settings.recreate_topics_delay_s)

    for batch in batched(required_topics, batch_size):
        log.info("Creating a batch of %d topics", len(batch))
        new_topics = [topic_spec(name, settings) for name in batch]
        response = admin_client.create_topics(new_topics, timeout_ms=30_000)
        errors = [e for e in response.topic_errors if e[1] != NoError.errno]
        if any(errors):
            raise Exception(f"Errors creating topics: {errors}")

    time.sleep(settings.recreate_topics_delay_s)
    log.info("Topics recreated")


def topic_spec(name: str, settings: ProducerConsumerSettings) -> NewTopic:
    return NewTopic(
        name=name,
        num_partitions=settings.partitions,
        replication_factor=settings.replication_factor,
        topic_configs={
            "min.insync.replicas": str(settings.min_isr),
        }
    )

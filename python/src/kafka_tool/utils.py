import itertools
import logging
import time
import threading
from typing import Dict, Sequence

from confluent_kafka.admin import AdminClient, NewTopic

from .settings import ProducerConsumerSettings


log = logging.getLogger(__name__)


def get_admin_client(config: Dict[str, str]):
    # Passing a logger to AdminClient doesn't work unless you poll the client
    # (https://github.com/confluentinc/confluent-kafka-python/issues/1699)
    client = AdminClient(config)
    return client


def get_topic_name(stem: str, index: int) -> str:
    return f"{stem}-{index}"


def recreate_topics(config: Dict[str, str], settings: ProducerConsumerSettings):
    log.info("Recreating %d topics", settings.topics)
    required_topics = set(get_topic_name(settings.topic_stem, i) for i in range(settings.topics))
    admin_client = get_admin_client(config)
    existing_topics = admin_client.list_topics(timeout=30).topics
    batch_size = settings.recreate_topics_batch_size
    for batch in batched(
            required_topics.intersection(existing_topics), batch_size):
        log.info("Deleting a batch of %d topics", len(batch))
        futures = admin_client.delete_topics(list(batch), operation_timeout=30, request_timeout=30)
        for fut in futures.values():
            fut.result()

    time.sleep(settings.recreate_topics_delay_s)

    for batch in batched(required_topics, batch_size):
        log.info("Creating a batch of %d topics", len(batch))
        new_topics = [topic_spec(name, settings) for name in batch]
        futures = admin_client.create_topics(new_topics, operation_timeout=30, request_timeout=30)
        for fut in futures.values():
            fut.result()

    time.sleep(settings.recreate_topics_delay_s)
    log.info("Topics recreated")


def topic_spec(name: str, settings: ProducerConsumerSettings) -> NewTopic:
    return NewTopic(
        name,
        num_partitions=settings.partitions,
        replication_factor=settings.replication_factor,
        config={
            "min.insync.replicas": str(settings.min_isr),
        }
    )


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
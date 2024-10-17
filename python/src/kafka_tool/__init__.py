from __future__ import annotations

import argparse
import logging
from typing import Dict

from kafka_tool.confluent_kafka import ConfluentKafkaTasks
from kafka_tool.consumer import run_consumer
from kafka_tool.kafka_python import KafkaPythonTasks
from kafka_tool.producer import run_producer
from kafka_tool.producer_consumer import run_producer_consumer
from kafka_tool.settings import ProducerConsumerSettings


log = logging.getLogger(__name__)


def main():
    arg_parser = argparse.ArgumentParser("kafka_tool")
    default_settings = ProducerConsumerSettings()

    arg_parser.add_argument(
        "command",
        choices=["producer", "consumer", "producer-consumer"])
    arg_parser.add_argument(
        "--library", "-l",
        choices=["confluent-kafka", "kafka-python"],
        default="confluent-kafka")
    arg_parser.add_argument(
        "--config", "-c",
        dest='config',
        action=StoreConfigEntry,
        help="Config entry in the form key=value. This argument may be repeated multiple times "
             "to set multiple configuration values")
    arg_parser.add_argument(
        "--producers",
        type=int,
        help="Number of concurrent producer tasks to run",
        default=default_settings.producers)
    arg_parser.add_argument(
        "--topics",
        type=int,
        help="Number of topics to create",
        default=default_settings.topics)
    arg_parser.add_argument(
        "--topic-stem",
        type=str,
        help="Prefix for topic names",
        default=default_settings.topic_stem)
    arg_parser.add_argument(
        "--recreate-topics-batch-size",
        type=int,
        help="Number of topics to recreate at once",
        default=default_settings.recreate_topics_batch_size)
    arg_parser.add_argument(
        "--recreate-topics-delay",
        type=int,
        help="Time to wait before recreating topics in ms",
        default=default_settings.recreate_topics_delay_ms)
    arg_parser.add_argument(
        "--partitions",
        type=int,
        help="Number of partitions per topic",
        default=default_settings.partitions)
    arg_parser.add_argument(
        "--replication-factor",
        type=int,
        help="Number of replicas",
        default=default_settings.replication_factor)
    arg_parser.add_argument(
        "--min-isr",
        type=int,
        help="Minimum number of in-sync replicas for created topics",
        default=default_settings.min_isr)
    arg_parser.add_argument(
        "--messages-per-second",
        type=int,
        help="Number of messages to send per-second for each producer task",
        default=default_settings.messages_per_second)

    args = arg_parser.parse_args()

    config: Dict[str, str] = args.config or {}
    settings = ProducerConsumerSettings(
        producers=args.producers,
        topics=args.topics,
        topic_stem=args.topic_stem,
        recreate_topics_batch_size=args.recreate_topics_batch_size,
        recreate_topics_delay_ms=args.recreate_topics_delay,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        min_isr=args.min_isr,
        messages_per_second=args.messages_per_second,
    )

    logging.basicConfig(level=logging.INFO)

    if args.library == "confluent-kafka":
        import confluent_kafka
        log.info("confluent_kafka version = %s", confluent_kafka.__version__)

        tasks = ConfluentKafkaTasks
    elif args.library == "kafka-python":
        import kafka
        log.info("kafka-python version = %s", kafka.__version__)

        tasks = KafkaPythonTasks
    else:
        raise ValueError(f"Invalid library: {args.library}")

    {
        'producer': run_producer,
        'consumer': run_consumer,
        'producer-consumer': run_producer_consumer,
    }[args.command](tasks, config, settings)


class StoreConfigEntry(argparse.Action):
    def __init__(self, option_strings, dest, **kwargs):
        super(StoreConfigEntry, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, arg_value, option_string=None):
        config = getattr(namespace, self.dest)
        if config is None:
            config = {}
            setattr(namespace, self.dest, config)
        key, value = arg_value.split("=", 1)
        config[key] = value


if __name__ == '__main__':
    main()
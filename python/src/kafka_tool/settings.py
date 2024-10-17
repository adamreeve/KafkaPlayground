from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ProducerConsumerSettings:
    producers: int = 1
    topics: int = 1
    topic_stem: str = "my-topic"
    recreate_topics_batch_size: int = 500
    recreate_topics_delay_ms: int = 1_000
    partitions: int = 10
    replication_factor: int = 2
    min_isr: int = 1
    messages_per_second: int = 1_000

    @property
    def recreate_topics_delay_s(self) -> float:
        return self.recreate_topics_delay_ms / 1_000
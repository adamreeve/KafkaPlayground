use crate::config;
use anyhow::{anyhow, Result};
use rskafka::client::partition::{OffsetAt, PartitionClient};
use rskafka::record::RecordAndOffset;
use rskafka::{
    client::{partition::UnknownTopicHandling, ClientBuilder},
    BackoffConfig,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub async fn run_consumer(
    cfg: HashMap<String, String>,
    cancellation: CancellationToken,
) -> Result<()> {
    let servers = config::get_bootstrap_servers(&cfg);
    let mut backoff_config = BackoffConfig::default();
    backoff_config.deadline = Some(Duration::from_secs(10));
    let client = ClientBuilder::new(servers)
        .backoff_config(backoff_config)
        .build()
        .await?;

    // Unlike other higher level libraries, there's no abstraction for subscribing to multiple
    // topics and partitions in one client, so we create separate tasks for each topic/partition
    // combination.
    let mut children = JoinSet::new();
    for topic_idx in 0..2 {
        for partition_idx in 0..2 {
            let partition_client = client
                .partition_client(
                    config::get_topic_name(topic_idx),
                    partition_idx,
                    UnknownTopicHandling::Retry,
                )
                .await?;

            let cancellation_c = cancellation.clone();
            children.spawn(run_partition_consumer(partition_client, cancellation_c));
        }
    }

    // Note: join_all will panic on error, need to use join_next in a loop
    // to properly handle errors
    children.join_all().await;
    Ok(())
}

pub async fn run_partition_consumer(
    partition_client: PartitionClient,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut offset = partition_client.get_offset(OffsetAt::Earliest).await?;
    while !cancellation.is_cancelled() {
        println!(
            "Fetching records from offset {} for topic {}, partition {}",
            offset,
            partition_client.topic(),
            partition_client.partition()
        );
        let (records, _high_watermark) = partition_client
            .fetch_records(
                offset,
                1..1_000_000, // min..max bytes
                1_000,        // max wait time in ms
            )
            .await?;
        for RecordAndOffset {
            record,
            offset: record_offset,
        } in records.into_iter()
        {
            let key = record.key.ok_or_else(|| anyhow!("Message has no key"))?;
            let value = record
                .value
                .ok_or_else(|| anyhow!("Message has no value"))?;
            let key: i64 = String::from_utf8(key)?.parse()?;
            let value: i64 = String::from_utf8(value)?.parse()?;
            println!(
                "Got key={}, value={} at offset={} for topic {}, partition {}",
                key,
                value,
                record_offset,
                partition_client.topic(),
                partition_client.partition()
            );
            offset = record_offset + 1;
        }
    }
    Ok(())
}

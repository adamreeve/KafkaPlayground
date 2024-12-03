use crate::config;
use anyhow::{anyhow, Result};
use rskafka::client::partition::OffsetAt;
use rskafka::record::RecordAndOffset;
use rskafka::{
    client::{partition::UnknownTopicHandling, ClientBuilder},
    BackoffConfig,
};
use std::collections::HashMap;
use std::time::Duration;
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
    // topics and partitions in one client. It would probably be fairly straightforward to implement
    // this though.
    let partition_client = client
        .partition_client(config::get_topic_name(0), 0, UnknownTopicHandling::Retry)
        .await?;

    let mut offset = partition_client.get_offset(OffsetAt::Earliest).await?;
    while !cancellation.is_cancelled() {
        println!("Fetching records from offset {}", offset);
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
            println!("Got key={key}, value={value} at offset={record_offset}");
            offset = record_offset + 1;
        }
    }
    Ok(())
}

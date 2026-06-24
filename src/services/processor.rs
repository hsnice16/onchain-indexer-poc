use alloy::{
    primitives::{Address, B256, Bytes, Log, LogData},
    sol,
    sol_types::SolEvent,
};
use std::{error::Error, time::Duration};
use tokio::time::Instant;

use crate::services::postgres;

sol! {
    event Transfer(address indexed from , address indexed to, uint256 value);
}

pub async fn start() -> Result<(), Box<dyn Error>> {
    let mut last_processed_pk: (i64, i64) = (0, 0);
    let pg_client = postgres::setup().await?;

    loop {
        let raw_logs =
            postgres::fetch_raw_log(&pg_client, last_processed_pk.0, last_processed_pk.1).await;

        for log in raw_logs {
            let block_number: i64 = log.get("block_number");
            let log_index: i64 = log.get("log_index");

            let address: Vec<u8> = log.get("address");
            let transaction_hash: Vec<u8> = log.get("transaction_hash");
            let block_timestamp: i64 = log.get("block_timestamp");

            let data: Vec<u8> = log.get("data");
            let topics: Vec<Vec<u8>> = log.get("topics");

            let decoded_data = Transfer::decode_log(&Log {
                address: Address::from_slice(&address),
                data: LogData::new(
                    topics.iter().map(|topic| B256::from_slice(topic)).collect(),
                    Bytes::from(data),
                )
                .ok_or("Failed to convert data")?,
            })?;

            let to = decoded_data.to.to_vec();
            let from = decoded_data.from.to_vec();
            let value = decoded_data.value.to_string();

            postgres::insert_processed_logs(
                &pg_client,
                block_number,
                log_index,
                transaction_hash,
                block_timestamp,
                to,
                from,
                &value,
            )
            .await;

            postgres::update_raw_log(&pg_client, block_number, log_index).await;
            last_processed_pk = (block_number, log_index);
        }

        tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
    }
}

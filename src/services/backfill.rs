use alloy::{
    primitives::{Address, B256},
    providers::{Provider, ProviderBuilder},
    rpc::types::Filter,
};

use reqwest::Url;
use std::{error::Error, str::FromStr, time::Duration};
use tokio::time::Instant;

use crate::{
    services::postgres::{self, insert_raw_log},
    utils::constants,
};

pub async fn start(block_number: u64) -> Result<(), Box<dyn Error>> {
    let pg_client = postgres::setup().await?;

    let url = Url::parse(constants::RISE_HTTP_RPC_URL)?;
    let provider = ProviderBuilder::new().connect_http(url);

    let bridged_usdc_address = Address::from_str(constants::BRIDGED_USDC_ADDRESS)?;
    let transfer_event_signature = B256::from_str(constants::TRANSFER_EVENT)?;

    // IDEALLY we should start from 0 (genesis) block
    let mut from_block = 5484128;

    // IDEALLY we should do - if batch_size > block_number { block_number } else { batch_size }
    let mut to_block = from_block + constants::BACKFILL_BLOCKS_BATCH;

    loop {
        println!("Backfill from {} to {}", from_block, to_block);

        let filter = Filter::new()
            .to_block(to_block)
            .from_block(from_block)
            .address(bridged_usdc_address)
            .event_signature(transfer_event_signature);

        let logs = provider.get_logs(&filter);

        match logs.await {
            Ok(logs) => {
                for log in logs {
                    insert_raw_log(log, &pg_client).await;
                }
            }
            Err(e) => {
                println!(
                    "Failed to backfill from {} to {}. Error: {}",
                    from_block, to_block, e
                );
                tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                continue;
            }
        }

        if to_block >= block_number {
            break;
        }

        from_block = to_block;
        to_block += constants::BACKFILL_BLOCKS_BATCH;

        if to_block > block_number {
            to_block = block_number
        }

        tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
    }

    Ok(())
}

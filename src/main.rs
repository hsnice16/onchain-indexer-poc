mod services;
mod utils;

use alloy::{
    primitives::{Address, B256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
};

use dotenvy::dotenv;
use futures_util::stream::StreamExt;
use std::{error::Error, str::FromStr};

use crate::{
    services::{backfill, postgres, processor},
    utils::constants,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv()?;
    // 1. Spawn a thread to Backfill logs

    let ws = WsConnect::new(constants::RISE_WS_RPC_URL);
    let provider = ProviderBuilder::new().connect_ws(ws).await?;

    let block_number = provider.get_block_number().await?;

    tokio::spawn(async move {
        let _ = backfill::start(block_number).await;
    });

    // 2. Spawn a thread to Process raw logs

    tokio::spawn(async {
        let _ = processor::start().await;
    });

    // 3. Look for new logs

    let bridged_usdc_address = Address::from_str(constants::BRIDGED_USDC_ADDRESS)?;
    let transfer_event_signature = B256::from_str(constants::TRANSFER_EVENT)?;

    let filter = Filter::new()
        .from_block(block_number)
        .address(bridged_usdc_address)
        .event_signature(transfer_event_signature);

    let sub = provider.subscribe_logs(&filter).await?;
    let mut stream = sub.into_stream();

    let pg_client = postgres::setup().await?;

    while let Some(log) = stream.next().await {
        postgres::insert_raw_log(log, &pg_client).await;
    }

    Ok(())
}

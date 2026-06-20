use alloy::{
    primitives::{Address, B256, Log as PrimitiveLog},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};

use futures_util::stream::StreamExt;
use serde::Deserialize;
use serde_json::json;
use std::{error::Error, str::FromStr, time::Duration};
use tokio::time::Instant;

sol! {
    event Transfer(address indexed from , address indexed to, uint256 value);
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct LogData {
    id: u8,
    jsonrpc: String,
    result: Vec<Log>,
}

// USDC.e (Bridged USDC)
const BRIDGED_USDC_ADDRESS: &str = "e436820ba0C69702c1d3E601d421c0eF38262739";

// Transfer(from, to, value) Event
const TRANSFER_EVENT: &str = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

async fn backfill_logs(block_number: u64) {
    const BLOCKS_BATCH: u64 = 5000;
    const RISE_HTTP_RPC_URL: &str = "https://rpc.risechain.com";

    let mut from_block = 0;
    let client = reqwest::Client::new();

    let mut to_block = if BLOCKS_BATCH > block_number {
        block_number
    } else {
        BLOCKS_BATCH
    };

    loop {
        println!("Backfill from {} to {}", from_block, to_block);

        let resp = client
            .post(RISE_HTTP_RPC_URL)
            .json(&json!({
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [{
                    "toBlock": format!("{:#x}", to_block),
                    "fromBlock": format!("{:#x}", from_block),
                    "topics": [format!("0x{}", TRANSFER_EVENT)],
                    "address": format!("0x{}", BRIDGED_USDC_ADDRESS),
                }],
            }))
            .send()
            .await;

        match resp {
            Ok(resp) => match resp.json::<LogData>().await {
                Ok(logs) => {
                    for log in logs.result {
                        println!("Log: {:#?}", log);
                    }
                }
                Err(_) => {
                    println!("Failed to backfill from {} to {}", from_block, to_block);
                    tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                    continue;
                }
            },
            Err(_) => {
                println!("Failed to backfill from {} to {}", from_block, to_block);
                tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
                continue;
            }
        }

        if to_block >= block_number {
            break;
        }

        from_block = to_block;
        to_block += BLOCKS_BATCH;

        if to_block > block_number {
            to_block = block_number
        }

        tokio::time::sleep_until(Instant::now() + Duration::from_secs(1)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    const RISE_WS_RPC_URL: &str = "wss://rpc.risechain.com/ws";

    let ws = WsConnect::new(RISE_WS_RPC_URL);
    let provider = ProviderBuilder::new().connect_ws(ws).await?;

    let block_number = provider.get_block_number().await?;

    // 1. Backfill logs

    tokio::spawn(async move {
        backfill_logs(block_number).await;
    });

    // 2. Look for new logs

    let bridged_usdc_address = Address::from_str(BRIDGED_USDC_ADDRESS)?;
    let transfer_event_signature = B256::from_str(TRANSFER_EVENT)?;

    let filter = Filter::new()
        .from_block(block_number)
        .address(bridged_usdc_address)
        .event_signature(transfer_event_signature);

    let sub = provider.subscribe_logs(&filter).await?;
    let mut stream = sub.into_stream();

    while let Some(log) = stream.next().await {
        println!("Log: {:#?}", log);

        let decoded_log = Transfer::decode_log(&PrimitiveLog {
            address: log.address(),
            data: log.data().clone(),
        })?;

        println!("Decoded Log -- To: {}", decoded_log.to);
        println!("Decoded Log -- From: {}", decoded_log.from);
        println!("Decoded Log -- Value: {}", decoded_log.value);
        println!("Decoded Log -- Address: {}", decoded_log.address);
    }

    Ok(())
}

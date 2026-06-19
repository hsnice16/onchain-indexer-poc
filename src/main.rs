use alloy::{primitives::Log as PrimitiveLog, rpc::types::Log, sol, sol_types::SolEvent};
use serde::Deserialize;
use serde_json::json;
use std::error::Error;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    const RISE_RPC_URL: &str = "https://rpc.risechain.com";

    let client = reqwest::Client::new();
    let resp = client
        .post(RISE_RPC_URL)
        .json(&json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": "0xD9EF96",
                "address": "0xe436820ba0C69702c1d3E601d421c0eF38262739", // USDC.e (Bridged USDC)
                "topics": [
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // Transfer(from, to, value) Event
                ]
            }],
        }))
        .send()
        .await?
        .json::<LogData>()
        .await?;

    for log in resp.result {
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

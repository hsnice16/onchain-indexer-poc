// More than this the REST RPC starts throwing error. Tested for 7500 and 10000
pub const BACKFILL_BLOCKS_BATCH: u64 = 5000;

pub const RISE_WS_RPC_URL: &str = "wss://rpc.risechain.com/ws";

pub const RISE_HTTP_RPC_URL: &str = "https://rpc.risechain.com";

// USDC.e (Bridged USDC)
pub const BRIDGED_USDC_ADDRESS: &str = "e436820ba0C69702c1d3E601d421c0eF38262739";

// Transfer(from, to, value) Event
pub const TRANSFER_EVENT: &str = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

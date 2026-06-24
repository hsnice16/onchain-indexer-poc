use alloy::rpc::types::Log;
use std::{env, error::Error};
use tokio_postgres::{Client, NoTls};

pub async fn setup() -> Result<Client, Box<dyn Error>> {
    let pg_db = env::var("PG_DB")?;
    let pg_user = env::var("PG_USER")?;
    let pg_password = env::var("PG_PASSWORD")?;

    let config = format!("host=127.0.0.1 user={pg_user} password={pg_password} dbname={pg_db}");
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            println!("Database connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn insert_raw_log(log: Log, client: &Client) {
    let address = log.address().to_vec();
    let data = log.data().data.to_vec();

    let topics: Vec<Vec<u8>> = log
        .topics()
        .to_vec()
        .iter()
        .map(|topic| topic.to_vec())
        .collect();

    let block_hash = match log.block_hash {
        Some(b_hash) => b_hash.to_vec(),
        None => Vec::new(),
    };

    let block_number = log.block_number.map(|b_number| b_number as i64);
    let log_index = log.log_index.map(|log_idx| log_idx as i64);
    let block_timestamp = log.block_timestamp.map(|b_ts| b_ts as i64);

    let transaction_hash = match log.transaction_hash {
        Some(txn_hash) => txn_hash.to_vec(),
        None => Vec::new(),
    };

    let transaction_index = log.transaction_index.map(|txn_idx| txn_idx as i64);
    let is_removed = log.removed;

    let query = "
        INSERT INTO raw_logs (address, data, topics, block_hash, block_number, log_index, block_timestamp, transaction_hash, transaction_index, is_removed) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (block_number, log_index)
        DO UPDATE SET
            address = EXCLUDED.address,
            data = EXCLUDED.data,
            topics = EXCLUDED.topics,
            block_hash = EXCLUDED.block_hash,
            block_number = EXCLUDED.block_number,
            log_index = EXCLUDED.log_index,
            block_timestamp = EXCLUDED.block_timestamp,
            transaction_hash = EXCLUDED.transaction_hash, 
            transaction_index = EXCLUDED.transaction_index, 
            is_removed = EXCLUDED.is_removed,
            is_processed = EXCLUDED.is_processed,
            created_at = EXCLUDED.created_at;
    ";

    match client
        .execute(
            query,
            &[
                &address,
                &data,
                &topics,
                &block_hash,
                &block_number,
                &log_index,
                &block_timestamp,
                &transaction_hash,
                &transaction_index,
                &is_removed,
            ],
        )
        .await
    {
        Ok(_) => {}
        Err(e) => {
            println!(
                "Failed to insert block_number {} log_index {}. Error: {}",
                block_number.unwrap_or_default(),
                log_index.unwrap_or_default(),
                e
            );
            Box::pin(insert_raw_log(log, client)).await;
        }
    }
}

pub async fn fetch_raw_log(
    client: &Client,
    block_number: i64,
    log_index: i64,
) -> Vec<tokio_postgres::Row> {
    // IDEALLY we should do `>=` on private key
    let query = "
        SELECT address, data, topics, block_number, log_index, block_timestamp, transaction_hash
        FROM raw_logs
        WHERE (block_number, log_index) > ($1, $2) 
            AND is_removed IS FALSE
            AND is_processed IS FALSE
        ORDER BY
            block_number ASC, log_index ASC
        LIMIT 10;
    ";

    match client.query(query, &[&block_number, &log_index]).await {
        Ok(logs) => {
            return logs;
        }
        Err(e) => {
            println!(
                "Failed to fetch block_number {} log_index {}. Error: {}",
                block_number, log_index, e
            );
            Box::pin(fetch_raw_log(client, block_number, log_index)).await
        }
    };

    Vec::new()
}

pub async fn update_raw_log(client: &Client, block_number: i64, log_index: i64) {
    let query = "
        UPDATE raw_logs
        SET is_processed = true
        WHERE block_number = $1
        AND log_index = $2;
    ";

    match client.execute(query, &[&block_number, &log_index]).await {
        Ok(_) => {}
        Err(e) => {
            println!(
                "Failed to update block_number {} log_index {}. Error: {}",
                block_number, log_index, e
            );
            Box::pin(update_raw_log(client, block_number, log_index)).await;
        }
    }
}

pub async fn insert_processed_logs(
    client: &Client,
    block_number: i64,
    log_index: i64,
    transaction_hash: Vec<u8>,
    block_timestamp: i64,
    to: Vec<u8>,
    from: Vec<u8>,
    value: &str,
) {
    let query = "
        INSERT INTO processed_logs (block_number, log_index, block_timestamp, transaction_hash, \"to\", \"from\", \"value\") 
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (block_number, log_index)
        DO UPDATE SET
            block_number = EXCLUDED.block_number,
            log_index = EXCLUDED.log_index,
            block_timestamp = EXCLUDED.block_timestamp,
            transaction_hash = EXCLUDED.transaction_hash,
            \"to\" = EXCLUDED.to,
            \"from\" = EXCLUDED.from,
            \"value\" = EXCLUDED.value,
            created_at = EXCLUDED.created_at;
    ";

    match client
        .execute(
            query,
            &[
                &block_number,
                &log_index,
                &block_timestamp,
                &transaction_hash,
                &to,
                &from,
                &value,
            ],
        )
        .await
    {
        Ok(_) => {}
        Err(e) => {
            println!(
                "Failed to insert processed block_number {} log_index {}. Error: {}",
                block_number, log_index, e
            );
            Box::pin(insert_processed_logs(
                client,
                block_number,
                log_index,
                transaction_hash,
                block_timestamp,
                to,
                from,
                value,
            ))
            .await;
        }
    }
}

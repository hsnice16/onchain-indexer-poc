# DB Schema

```sql
CREATE TABLE raw_logs (
  address BYTEA,
  data BYTEA,
  topics BYTEA[],
  block_hash BYTEA,
  block_number BIGINT CHECK (block_number >= 0),
  log_index BIGINT CHECK (log_index >= 0),
  block_timestamp BIGINT CHECK (block_timestamp >= 0),
  transaction_hash BYTEA,
  transaction_index BIGINT CHECK (transaction_index >= 0),
  is_removed BOOLEAN NOT NULL,
  is_processed BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (block_number, log_index)
);
```

create database if not exists fraud;

create table fraud.transactions
(
    source_row UInt64,
    transaction_time DateTime,
    merch LowCardinality(String),
    cat_id LowCardinality(Nullable(String)),
    amount Decimal(18, 2),
    name_1 String,
    name_2 String,
    gender LowCardinality(String),
    street String,
    one_city LowCardinality(String),
    us_state LowCardinality(String),
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs LowCardinality(String),
    merchant_lat Float64,
    merchant_lon Float64,
    target Nullable(UInt8)
)
engine = MergeTree
order by (us_state, transaction_time, source_row);

create table fraud.kafka_transactions
(
    source_row UInt64,
    transaction_time DateTime,
    merch String,
    cat_id Nullable(String),
    amount Decimal(18, 2),
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target Nullable(UInt8)
)
engine = Kafka
settings
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'hw2-clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 10000,
    kafka_flush_interval_ms = 1000;

create materialized view fraud.kafka_to_transactions
to fraud.transactions
as select * from fraud.kafka_transactions;

create table if not exists fraud.baseline as fraud.transactions
engine = MergeTree
order by tuple();

alter table fraud.baseline
    modify column merch String,
    modify column cat_id Nullable(String),
    modify column gender String,
    modify column one_city String,
    modify column us_state String,
    modify column jobs String;

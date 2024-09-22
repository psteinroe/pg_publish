## About `pg_publish`

`pg_publish` is a highly experimental Rust crate to quickly forward the Postgres replication stream to a queue to run side-effects. It has build during the Supabase Launch Week XI Hackathon to learn about the Postgres Replication Protocol and gain more hands-on experience with async Rust.

Goals:
- at least once-delivery of replication data to designated endpoint
- automatically handle schema changes (start once, run forever)
- never crash (todo :D)
- later: fan-out to different queues with filters on event data
- later: consistent message ordering per queue

Non-Goals:
- persistence
- exactly-once delivery: such guarantees should be provided from the queue layer instead

At the moment, `pg_publish` is in no means better than just using `pg_net` other than higher possible throughput in theory! Only with fan-out and per-queue guaranteed message ordering this crate could become a good alternative if you have such requirements.

## Differences to `pg_replicate`

`pg_publish` draws a lot of inspiration (and code) from `pg_replicate`. I initially intended to just implement this as another Sink, but ended up with a fork due to different requrements. The main differences are:
- `pg_replicate` is much more mature
- different batching strategy: `pg_replicate` is batching events until a certain batch size and guarantees to end a batch on certain event types. This crate is batching per-transaction instead, so that during normal operations all changes made in a single transaction are always published together. There is still a max batch size, but its relatively high and mainly intended to not run out of memory when large migrations are run on the database.
- `pg_replicate` parses the cdc data into their actual type. We are just interested in their Json representation.*
- `pg_replicate` sends events one by one, while `pg_publish` publishes data batches concurrently.
- `pg_publish` requires an external store and does not use the sink to store the latest committed lsn. The store is flushed on a regular interval and not with every batch.
- `pg_publish` does not support backfill and will never support it
- `pg_publish` will have a tighter integration with queues as the Sink / Destination by providing an abstraction for subscriptions that apply filters on the stream and fan-out every event to 0-n queues.

\* I tried to use wal2json instead of the default pgoutput plugin to have the database return the data as json directly. Unfortunately, wal2json does not allow for manual LSN reporting which is mandatory for us to apply backpressure.

## Quickstart

To quickly try out `pg_publish`, you can run the `stdout` example, which will replicate the data to standard output. First, create a publication in Postgres which includes the tables you want to replicate:

```
create publication test_publication
for table table1, table2;
```

Then run the `stdout` example:

```
cargo run -p pg_publish --example stdout -- --db-host localhost --db-port 54322 --db-name postgres --db-username postgres --db-password postgres cdc test_publication test_slot
```

In the above example, `pg_publish` connects to a Postgres database named `postgres` running on `localhost:54322` with a username `postgres` and password `postgres`. The slot name `test_slot` will be created by `pg_replicate` automatically.

## ToDos

- [ ] refactoring to actor-based pipeline
- [ ]support more data types
- [ ]error handling + graceful shutdown + recovery
- [ ] fan out with subscriptions

## Design

Applications can use data sources, stores and destinations from `pg_publish` to build a data pipeline to continually publish data from the source to the destination. For example, a data pipeline to copy data from Postgres to SNS or SQS.

There are three components in a data pipeline:

1. A data source
2. A destination
3. A store

The data source is an object from where data will be copied. The destination is an object to which data will be published. The store is an object that stores the latest published transaction to report it back to postgres regularly.



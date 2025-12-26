# Sink Routing Integration Test

## Overview

This integration test verifies the sink routing feature for MySQL sinks, which allows CDC to route tables from source schemas/tables to different target schemas/tables during replication.

**Test Group**: G00 (`mysql_only`)
**Sink Type**: MySQL only (auto-skips for Kafka, Pulsar, Storage)

## Test Scenario

- **Source**: `source_db.{table}`
- **Target**: `target_db.{table}_routed`

The test configures a dispatch rule that:
1. Matches all tables in `source_db.*`
2. Routes them to the `target_db` schema
3. Appends `_routed` suffix to table names

## What is Tested

1. **Schema Routing**: Tables are replicated from `source_db` to `target_db`
2. **Table Routing**: Table names get `_routed` suffix (e.g., `users` → `users_routed`)
3. **DDL Routing**: DDL statements (CREATE TABLE, ALTER TABLE) are correctly routed
4. **DML Routing**: INSERT, UPDATE operations are routed to the correct target tables
5. **Data Integrity**: All data is correctly replicated to the routed tables
6. **Negative Checks**: Source tables do NOT exist in downstream (verifies routing replaced them)

## Test Flow

1. Create source database and tables (`source_db.users`, `source_db.orders`)
2. Start CDC with routing configuration
3. Insert initial data
4. Perform additional DML operations (INSERT, UPDATE)
5. Execute DDL operations (ALTER TABLE, CREATE TABLE)
6. Verify:
   - Tables exist in `target_db` with `_routed` suffix
   - Source tables do NOT exist in downstream
   - All data is correctly replicated
   - DDL changes are applied

## Configuration

See `conf/changefeed.toml` for the dispatch rule configuration:

```toml
[[sink.dispatch-rules]]
matcher = ['source_db.*']
schema = 'target_db'
table = '{table}_routed'
```

## How to Run

### Locally
```bash
# Run this specific test with MySQL sink
cd tests/integration_tests
./run.sh mysql sink_routing
```

### In CI
The test runs automatically as part of Group G00 in the MySQL integration test pipeline.

### Via Docker
```bash
CASE="sink_routing" make mysql_docker_integration_test_with_build
```

## Files

- `run.sh` - Main test script
- `conf/changefeed.toml` - Routing configuration
- `data/prepare.sql` - Initial setup (create tables, insert data)
- `data/test.sql` - Test operations (more DML, DDL, finish marker)

# Demo 2: ACID Transactions & Concurrency

## Overview

This demo explores Delta Lake's ACID (Atomicity, Consistency, Isolation, Durability) transaction capabilities and how it handles concurrent operations. You'll learn how Delta Lake ensures data integrity through its transaction log, isolation levels, and conflict resolution mechanisms using real UberEats data.

## Learning Objectives

By the end of this demo, you will be able to:

- ✅ Understand ACID properties in Delta Lake
- ✅ Handle concurrent write operations safely
- ✅ Explore and interpret Delta Lake transaction logs
- ✅ Configure isolation levels for different use cases
- ✅ Perform DDL operations on Delta tables

## Prerequisites

- Completed Demo 1 (Delta Lake Foundation)
- Docker environment with Spark cluster running
- Access to MinIO S3-compatible storage
- UberEats sample data loaded in MinIO

## File Structure

```
src/spark/mod-4/
├── demo-2.py
└── demo-2.md
```

## Demo Structure

### 1. ACID Transaction Properties

Delta Lake provides full ACID guarantees for all operations:

#### **Atomicity**
All operations in a transaction either complete successfully or fail entirely, with no partial updates.

```python
# Complex transaction with multiple operations
delta_table.update(
    condition=col("status") == "pending",
    set={"status": lit("processing")}
)

# If any operation fails, all changes are rolled back
audit_record.write.format("delta").mode("append").save(table_path)
```

#### **Consistency**
Data integrity constraints are maintained across all transactions.

#### **Isolation**
Concurrent operations don't interfere with each other - readers get consistent snapshots.

#### **Durability**
Once committed, changes are permanently stored and survive system failures.

### 2. Concurrent Write Operations

Delta Lake handles multiple writers through optimistic concurrency control:

```python
# Multiple writers can operate simultaneously
def writer_1():
    delta_table.update(condition=..., set=...)

def writer_2():
    new_data.write.format("delta").mode("append").save(path)

# Delta Lake automatically resolves conflicts
```

**Conflict Resolution:**
- **Compatible operations**: Both succeed (e.g., append + update different partitions)
- **Conflicting operations**: Latest writer wins, others retry
- **Automatic retry**: Failed operations can be retried with updated table state

### 3. Transaction Log (_delta_log)

The transaction log is Delta Lake's core innovation for ACID guarantees:

```
table_path/
├── _delta_log/
│   ├── 00000000000000000000.json  # Initial commit
│   ├── 00000000000000000001.json  # Transaction 1
│   ├── 00000000000000000002.json  # Transaction 2
│   └── _last_checkpoint             # Checkpoint metadata
├── part-00000-*.parquet            # Data files
└── part-00001-*.parquet
```

**Transaction Log Benefits:**
- **Atomic commits**: Single file write = atomic transaction
- **Version history**: Complete audit trail of all changes
- **Conflict detection**: Optimistic concurrency control
- **Fast metadata**: No expensive directory listings

### 4. Isolation Levels

Delta Lake provides snapshot isolation by default:

```python
# Reader gets consistent snapshot
reader_df = spark.read.format("delta").load(table_path)
initial_count = reader_df.count()

# Writer modifies table
new_data.write.format("delta").mode("append").save(table_path)

# Reader still sees original snapshot
final_count = reader_df.count()  # Same as initial_count
```

**Isolation Characteristics:**
- **Snapshot isolation**: Readers see consistent point-in-time view
- **No dirty reads**: Never see uncommitted changes
- **No phantom reads**: Consistent results within same transaction
- **Optimistic concurrency**: High performance for read-heavy workloads

### 5. DDL Operations

Delta Lake supports standard SQL DDL with transactional guarantees:

```sql
-- Create table with properties
CREATE OR REPLACE TABLE my_table (
    id BIGINT,
    name STRING,
    status STRING DEFAULT 'active'
)
USING DELTA
LOCATION 's3a://path/to/table'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)

-- Schema evolution
ALTER TABLE my_table ADD COLUMN (created_at TIMESTAMP)
ALTER TABLE my_table ALTER COLUMN name COMMENT 'User name'

-- Table properties
ALTER TABLE my_table SET TBLPROPERTIES ('key' = 'value')
```

## Running the Demo

### Step 1: Set Environment Variables (Optional)
```bash
export MINIO_ENDPOINT="http://24.144.65.249:80"
export MINIO_ACCESS_KEY="miniolake"
export MINIO_SECRET_KEY="LakE142536@@"
```

### Step 2: Execute the Demo
```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-2.py
```

### Step 3: Monitor Execution
Watch for:
- ✅ ACID transaction completions
- 🔄 Concurrent operation results
- 📜 Transaction log entries
- 🔐 Isolation level demonstrations
- 🏗️ DDL operation successes

## Expected Output

The demo will create/update several Delta Lake tables:

```
owshq-uber-eats-lakehouse/bronze/
├── payments_acid/
├── orders_concurrent/
├── users_isolation/
└── ddl_demo/
```

Each table will have transaction logs showing the operations performed.

## Key Concepts Learned

### Transaction Log Structure
```json
{
  "commitInfo": {
    "timestamp": 1640995200000,
    "operation": "WRITE",
    "operationParameters": {...},
    "readVersion": 0,
    "isolationLevel": "SnapshotIsolation"
  },
  "add": {
    "path": "part-00000-*.parquet",
    "partitionValues": {},
    "size": 1024,
    "modificationTime": 1640995200000,
    "dataChange": true,
    "stats": "{\"numRecords\":100,...}"
  }
}
```

### Optimistic Concurrency Control
1. **Read**: Get current table version
2. **Process**: Perform local operations
3. **Validate**: Check if table version changed
4. **Commit**: Write new transaction log entry
5. **Retry**: If conflict detected, retry with new version

### Performance Implications
- **Readers**: Never blocked by writers
- **Writers**: May need to retry on conflicts
- **Metadata**: Fast operations with transaction log
- **Scalability**: Handles thousands of concurrent readers

## Troubleshooting

### Common Issues and Solutions

1. **Concurrent Writer Conflicts**
   ```
   ConcurrentModificationException: Files were added to table by a concurrent update
   ```
   **Solution**: Implement retry logic or use partition-level operations

2. **Transaction Log Growth**
   ```
   Warning: Large number of transaction log files
   ```
   **Solution**: Run `OPTIMIZE` and checkpoint operations regularly

3. **Isolation Level Confusion**
   ```
   Inconsistent read results
   ```
   **Solution**: Understand snapshot isolation - create new DataFrame for fresh snapshot

4. **DDL Operation Failures**
   ```
   AnalysisException: Cannot add column with existing name
   ```
   **Solution**: Check existing schema before ALTER operations

## Exercises

### Exercise 1: ACID Transaction Testing
**Objective**: Test ACID properties with complex operations.

**Tasks**:
1. Create a Delta table with financial transaction data
2. Implement a complex transaction that:
   - Updates account balances
   - Inserts transaction records
   - Validates business rules
3. Intentionally cause a failure in the middle of the transaction
4. Verify that no partial updates occurred (Atomicity)
5. Check the transaction log to see the failed operation

**Expected Learning**:
- Understanding of atomic operations
- Transaction rollback behavior
- Error handling in Delta Lake

### Exercise 2: Concurrent Writer Simulation
**Objective**: Simulate real-world concurrent access patterns.

**Tasks**:
1. Create a table representing real-time order updates
2. Simulate 3 concurrent processes:
   - Process 1: Updates order status
   - Process 2: Adds new orders
   - Process 3: Deletes cancelled orders
3. Run all processes simultaneously
4. Analyze which operations succeeded and which were retried
5. Examine the transaction log to understand the conflict resolution

**Questions to Answer**:
- Which operations are compatible and can run concurrently?
- How does Delta Lake handle write conflicts?
- What's the performance impact of concurrent writes?

### Exercise 3: Transaction Log Analysis
**Objective**: Deep dive into Delta Lake's transaction log.

**Tasks**:
1. Perform a series of operations on a Delta table:
   - Initial load
   - Update operation
   - Delete operation
   - Schema change
   - Optimize operation
2. Examine each transaction log file in the `_delta_log` directory
3. Decode the JSON content and understand each field
4. Create a timeline of operations using `DESCRIBE HISTORY`
5. Practice using time travel to read specific versions

**Investigation Points**:
- What information is stored in each transaction log entry?
- How does Delta Lake track schema evolution?
- What are the performance characteristics of different operations?

### Exercise 4: Custom Isolation Testing
**Objective**: Understand isolation behavior in detail.

**Tasks**:
1. Create a long-running analytical query (simulate with `time.sleep()`)
2. While the query is "running", modify the underlying table
3. Test different scenarios:
   - Reader starts before writer commits
   - Reader starts after writer commits
   - Multiple readers with same start time
4. Compare results and explain the isolation behavior
5. Test with different DataFrame creation patterns

**Scenarios to Test**:
```python
# Scenario 1: Early reader
df1 = spark.read.format("delta").load(path)
# ... writer modifies table ...
result1 = df1.count()  # What does this return?

# Scenario 2: Late reader  
# ... writer modifies table ...
df2 = spark.read.format("delta").load(path)
result2 = df2.count()  # What does this return?
```

### Exercise 5: Advanced DDL Operations
**Objective**: Master schema evolution and table management.

**Tasks**:
1. Create a table with initial schema
2. Perform progressive schema changes:
   - Add nullable columns
   - Add columns with default values
   - Change column comments
   - Add table properties
3. Test data compatibility after each change
4. Practice column mapping for schema evolution
5. Implement a schema validation function

**Advanced Challenge**:
Create a schema migration script that:
- Validates current schema
- Applies changes safely
- Rolls back on failure
- Logs all changes for audit

**Schema Evolution Pattern**:
```sql
-- Check current schema
DESCRIBE EXTENDED my_table

-- Safe schema addition
ALTER TABLE my_table ADD COLUMN (
    new_column STRING COMMENT 'Added for feature X'
)

-- Verify change
DESCRIBE EXTENDED my_table

-- Test data compatibility
SELECT new_column FROM my_table LIMIT 5
```

## Best Practices

### 1. Transaction Design
- Keep transactions small and focused
- Avoid long-running transactions
- Design for retry-ability
- Use idempotent operations when possible

### 2. Concurrency Management
- Partition data to reduce conflicts
- Use append-only patterns where possible
- Implement exponential backoff for retries
- Monitor conflict rates

### 3. Transaction Log Maintenance
- Regular OPTIMIZE operations
- Set up automated checkpointing
- Monitor log file growth
- Plan for log retention policies

### 4. Schema Evolution
- Plan schema changes carefully
- Use backward-compatible changes
- Test with existing data
- Document schema versions

## Next Steps

After completing this ACID and concurrency demo, you'll be ready for:

- **Demo 3**: Time Travel & History Management - Explore Delta Lake's versioning capabilities
- **Demo 4**: Schema Management & Evolution - Handle changing data structures
- **Demo 5**: DML Operations & Data Management - Master data manipulation operations

## Additional Resources

- [Delta Lake Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Concurrency Control in Delta Lake](https://docs.delta.io/latest/concurrency-control.html)
- [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)

---

🎯 **Remember**: Delta Lake's ACID guarantees and concurrency control make it suitable for mission-critical applications where data consistency and reliability are paramount. The transaction log is the secret sauce that enables these capabilities while maintaining high performance.

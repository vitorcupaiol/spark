# Demo 3: Time Travel & History Management

## Overview

This demo explores Delta Lake's powerful time travel capabilities and history management features. You'll learn how to query historical versions of your data, analyze table evolution over time, restore tables to previous states, and manage storage with VACUUM operations using real UberEats order data.

## Learning Objectives

By the end of this demo, you will be able to:

- ✅ Query historical versions of Delta tables using time travel
- ✅ Analyze table history with DESCRIBE HISTORY and History API
- ✅ Restore tables to previous versions
- ✅ Manage storage and cleanup with VACUUM operations
- ✅ Perform advanced analytics across different table versions
- ✅ Understand the trade-offs between storage and time travel capabilities

## Prerequisites

- Completed Demo 1 (Delta Lake Foundation) and Demo 2 (ACID Transactions)
- Docker environment with Spark cluster running
- Access to MinIO S3-compatible storage
- UberEats sample data loaded in MinIO

## File Structure

```
src/spark/mod-4/
├── demo-3.py
└── demo-3.md
```

## Demo Structure

### 1. Time Travel Fundamentals

Delta Lake's time travel allows you to query historical versions of your data:

#### **Version-Based Time Travel**
```python
# Read specific version
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(table_path)

# Compare versions
print(f"Version 0: {df_v0.count()} records")
print(f"Version 1: {df_v1.count()} records")
```

#### **Timestamp-Based Time Travel**
```python
# Read data as of specific timestamp
df_historical = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01 10:00:00") \
    .load(table_path)

# Using SQL syntax
spark.sql(f"""
    SELECT * FROM delta.`{table_path}` 
    TIMESTAMP AS OF '2024-01-01 10:00:00'
""")
```

### 2. Table History Analysis

#### **DESCRIBE HISTORY Command**
```sql
-- SQL approach
DESCRIBE HISTORY delta.`s3a://path/to/table`

-- Shows: version, timestamp, operation, operationParameters, etc.
```

#### **Delta Table History API**
```python
# Programmatic approach
delta_table = DeltaTable.forPath(spark, table_path)
history_df = delta_table.history()

# Analyze operations
history_df.select("version", "timestamp", "operation").show()

# Get operation metrics
history_df.select("operationMetrics").show(truncate=False)
```

### 3. RESTORE TABLE Operations

Restore tables to previous versions when data issues occur:

```sql
-- Restore to specific version
RESTORE TABLE delta.`s3a://path/to/table` TO VERSION AS OF 2

-- Restore to specific timestamp
RESTORE TABLE delta.`s3a://path/to/table` 
TO TIMESTAMP AS OF '2024-01-01 10:00:00'
```

**Use Cases for RESTORE:**
- **Data corruption recovery**: Roll back to known good state
- **Bad deployment rollback**: Undo problematic data changes
- **Testing scenarios**: Reset to baseline for testing
- **Compliance requirements**: Restore to audit-required state

### 4. VACUUM Operations

Manage storage by cleaning up old data files:

```sql
-- Dry run (see what would be deleted)
VACUUM delta.`s3a://path/to/table` RETAIN 168 HOURS DRY RUN

-- Actual vacuum (default 7 days retention)
VACUUM delta.`s3a://path/to/table`

-- Custom retention period
VACUUM delta.`s3a://path/to/table` RETAIN 240 HOURS
```

**VACUUM Considerations:**
- **Default retention**: 7 days (168 hours)
- **Time travel impact**: Cannot time travel past vacuumed files
- **Concurrent safety**: Safe to run with concurrent reads/writes
- **Storage optimization**: Removes unused files to reduce costs

### 5. Advanced Time Travel Analytics

#### **Data Evolution Tracking**
```python
# Compare metrics across versions
v0_stats = df_v0.agg({"amount": "count", "amount": "avg"}).collect()[0]
v_current_stats = df_current.agg({"amount": "count", "amount": "avg"}).collect()[0]

growth_rate = ((v_current_stats[0] - v0_stats[0]) / v0_stats[0]) * 100
print(f"Data growth: {growth_rate:.1f}%")
```

#### **Schema Evolution Analysis**
```python
# Track schema changes
v0_columns = set(df_v0.columns)
current_columns = set(df_current.columns)

new_columns = current_columns - v0_columns
print(f"New columns added: {list(new_columns)}")
```

#### **Audit Trail Creation**
```python
# Create comprehensive audit log
audit_df = history_df.select(
    "version", "timestamp", "operation",
    col("operationMetrics.numTargetRowsUpdated").alias("rows_updated"),
    col("operationMetrics.numTargetRowsDeleted").alias("rows_deleted")
)
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
  /opt/bitnami/spark/jobs/spark/mod-4/demo-3.py
```

### Step 3: Monitor Execution
Watch for:
- ✅ Version creation progress (0 → 4)
- ⏰ Time travel query results
- 📜 History analysis output
- 🔄 Restore operation status
- 🧹 VACUUM operation results

## Expected Output

The demo creates a multi-version table showing data evolution:

```
Version 0: Initial load (100 records)
Version 1: Append operation (200 total records)  
Version 2: Update operation (price adjustments)
Version 3: Schema evolution (new column added)
Version 4: Delete operation (filtered records)
```

## Key Concepts Learned

### Time Travel Architecture
```
Table Versions:
├── Version 0: 00000000000000000000.json (Initial)
├── Version 1: 00000000000000000001.json (Append) 
├── Version 2: 00000000000000000002.json (Update)
└── Version 3: 00000000000000000003.json (Delete)

Data Files:
├── part-00000-v0.parquet (Version 0 data)
├── part-00001-v1.parquet (Version 1 new data)
├── part-00002-v2.parquet (Version 2 updated data)
└── [Previous files still available for time travel]
```

### Version Retention Strategy
- **Short-term**: Keep all versions for debugging (hours/days)
- **Medium-term**: Retain major versions for compliance (weeks)
- **Long-term**: Archive critical snapshots (months/years)
- **Cost optimization**: Balance storage costs vs. time travel needs

### Performance Considerations
- **Time travel overhead**: Reading old versions may be slower
- **Metadata growth**: More versions = larger transaction log
- **Storage costs**: Old files consume storage until vacuumed
- **Query planning**: Spark optimizes based on available statistics

## Troubleshooting

### Common Issues and Solutions

1. **Version Not Found**
   ```
   VersionNotFoundException: Version 5 not found
   ```
   **Solution**: Check available versions with `DESCRIBE HISTORY`

2. **Files Not Found After VACUUM**
   ```
   FileNotFoundException: File has been deleted
   ```
   **Solution**: Version vacuumed beyond retention period

3. **Concurrent Modification During RESTORE**
   ```
   ConcurrentModificationException: Table modified during restore
   ```
   **Solution**: Ensure no concurrent writes during restore operation

4. **VACUUM Permission Issues**
   ```
   AccessDeniedException: Cannot delete files
   ```
   **Solution**: Check S3 permissions for delete operations

## Exercises

### Exercise 1: Time Travel Comparison Analysis
**Objective**: Compare business metrics across different time periods.

**Tasks**:
1. Create a table with sales data over multiple days
2. Simulate daily batch updates (new sales + price changes)
3. Use time travel to compare:
   - Daily sales volumes
   - Average order values  
   - Customer behavior changes
4. Create a trend analysis showing evolution over time
5. Identify the version with highest revenue

**Expected Outcome**:
```
Day 1 (Version 0): 150 orders, $45.50 avg
Day 2 (Version 1): 280 orders, $52.30 avg  
Day 3 (Version 2): 310 orders, $48.90 avg
Peak performance: Version 2 with $15,159 total revenue
```

### Exercise 2: Data Recovery Simulation
**Objective**: Practice recovery from data corruption scenarios.

**Tasks**:
1. Create a stable baseline version (Version 0)
2. Apply several legitimate updates (Versions 1-3)
3. Simulate data corruption by applying bad updates (Version 4)
4. Use `DESCRIBE HISTORY` to identify the corruption point
5. Restore to the last known good version
6. Verify data integrity after restore
7. Document the recovery process

**Recovery Scenario**:
```sql
-- Identify corruption
DESCRIBE HISTORY delta.`table_path`

-- Restore to last good version
RESTORE TABLE delta.`table_path` TO VERSION AS OF 3

-- Verify restoration
SELECT COUNT(*) FROM delta.`table_path`
```

### Exercise 3: Storage Optimization Strategy
**Objective**: Develop an optimal VACUUM strategy.

**Tasks**:
1. Create a table with frequent updates over several versions
2. Analyze storage growth using `DESCRIBE DETAIL`
3. Run `VACUUM DRY RUN` with different retention periods:
   - 0 hours (aggressive cleanup)
   - 24 hours (daily retention)  
   - 168 hours (weekly retention)
4. Compare storage savings vs. time travel capabilities
5. Implement an optimal VACUUM schedule
6. Test time travel limitations after each VACUUM

**Analysis Points**:
- How much storage is saved with each retention period?
- What versions become inaccessible after VACUUM?
- What's the optimal balance for your use case?

### Exercise 4: Schema Evolution Audit
**Objective**: Track and document schema changes over time.

**Tasks**:
1. Start with a simple table schema
2. Apply progressive schema changes:
   - Add nullable columns
   - Add columns with default values
   - Rename columns (via recreate)
   - Change data types (where possible)
3. Use time travel to analyze schema at each version
4. Create a schema evolution report showing:
   - When each change occurred
   - What columns were added/modified
   - Impact on existing data
5. Test backward compatibility by reading old versions

**Schema Evolution Report**:
```
Version 0: [id, name, email] - baseline schema
Version 1: [id, name, email, phone] - added phone
Version 2: [id, name, email, phone, status] - added status with default
Version 3: [id, full_name, email, phone, status] - renamed name to full_name
```

### Exercise 5: Advanced Audit Trail Implementation
**Objective**: Build a comprehensive audit system using time travel.

**Tasks**:
1. Create a sensitive data table (customer information)
2. Implement audit logging for all operations:
   - Who made changes (user identification)
   - When changes occurred (timestamps)
   - What changed (operation details)
   - How many records affected (metrics)
3. Use the History API to build audit reports:
   - Daily change summary
   - User activity report
   - Data volume trends
   - Risk assessment (large deletions/updates)
4. Create alerts for suspicious activities
5. Demonstrate compliance reporting capabilities

**Audit Report Example**:
```python
def generate_audit_report(table_path, start_date, end_date):
    """Generate comprehensive audit report"""
    history_df = delta_table.history()
    
    # Filter by date range
    period_changes = history_df.filter(
        (col("timestamp") >= start_date) & 
        (col("timestamp") <= end_date)
    )
    
    # Summarize activities
    summary = period_changes.groupBy("operation", "userName") \
        .agg(count("*").alias("operation_count"))
    
    return summary
```

## Best Practices

### 1. Time Travel Strategy
- Plan retention periods based on business requirements
- Consider compliance and audit needs
- Balance storage costs with recovery capabilities
- Document version significance (releases, milestones)

### 2. VACUUM Management
- Schedule VACUUM during low-activity periods
- Use DRY RUN first to estimate impact
- Monitor storage costs and optimize retention
- Coordinate with backup and archival strategies

### 3. Version Documentation
- Tag significant versions with meaningful operations
- Maintain change logs for major schema evolution
- Document known good versions for recovery
- Establish rollback procedures and criteria

### 4. Performance Optimization
- Use predicate pushdown with time travel queries
- Consider partitioning for large historical datasets
- Monitor query performance across versions
- Optimize for common time travel patterns

## Next Steps

After completing this time travel and history management demo, you'll be ready for:

- **Demo 4**: Schema Management & Evolution - Handle changing data structures safely
- **Demo 5**: DML Operations & Data Management - Master data manipulation operations
- **Demo 6**: Performance Optimization - Optimize and tune Delta Lake tables

## Additional Resources

- [Delta Lake Time Travel Documentation](https://docs.delta.io/latest/delta-batch.html#deltatimetravel)
- [VACUUM Command Reference](https://docs.delta.io/latest/delta-utility.html#vacuum)
- [Table History and Versioning](https://docs.delta.io/latest/delta-batch.html#table-history)

---

🎯 **Remember**: Time travel is one of Delta Lake's most powerful features for data reliability and compliance. Use it wisely to balance recovery capabilities with storage costs, and always test your recovery procedures before you need them in production.

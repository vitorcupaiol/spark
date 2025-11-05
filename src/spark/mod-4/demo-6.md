# Demo 6: Performance Optimization

## Overview

This demo explores advanced performance optimization techniques in Delta Lake and Apache Spark. You'll learn how to optimize table layouts with Z-ORDER, leverage Adaptive Query Execution (AQE), design effective partition strategies, and use Bloom Filters for data skipping using real UberEats performance scenarios.

## Learning Objectives

By the end of this demo, you will be able to:

- ✅ Optimize Delta tables using OPTIMIZE and Z-ORDER operations
- ✅ Configure and leverage Adaptive Query Execution (AQE) features
- ✅ Design effective partition strategies for large datasets
- ✅ Implement Bloom Filters for efficient data skipping
- ✅ Measure and analyze query performance improvements
- ✅ Choose appropriate optimization techniques for different workloads

## Prerequisites

- Completed Demo 1-5 (Delta Lake Foundation through DML Operations)
- Docker environment with Spark cluster running
- Access to MinIO S3-compatible storage
- UberEats sample data loaded in MinIO
- Understanding of query execution plans and performance metrics

## File Structure

```
src/spark/mod-6/
├── demo-6.py
└── demo-6.md
```

## Demo Structure

### 1. OPTIMIZE & Z-ORDER Operations

Delta Lake's OPTIMIZE command compacts small files and Z-ORDER organizes data for efficient querying:

#### **Basic OPTIMIZE Operation**
```sql
-- Compact small files
OPTIMIZE delta.`s3a://path/to/table`

-- Check table details
DESCRIBE DETAIL delta.`s3a://path/to/table`
```

#### **Z-ORDER Optimization**
```sql
-- Optimize with Z-ORDER on frequently queried columns
OPTIMIZE delta.`s3a://path/to/orders` 
ZORDER BY (restaurant_id, order_date)

-- Multi-column Z-ORDER for complex queries
OPTIMIZE delta.`s3a://path/to/orders`
ZORDER BY (user_id, restaurant_id, order_status)
```

#### **When to Use Z-ORDER**
- **Point lookups**: Queries filtering on specific values
- **Range queries**: Date ranges, price ranges, etc.
- **Multi-dimensional queries**: Filtering on multiple columns
- **Join optimization**: Improve join performance on common keys

#### **Z-ORDER Benefits**
```python
# Before Z-ORDER: Random data layout
Files: [user1,user500,user234] [user2,user890,user45] [user3,user123,user678]

# After Z-ORDER by user_id: Co-located similar values  
Files: [user1,user2,user3] [user45,user123,user234] [user500,user678,user890]

# Result: Fewer files read for user-specific queries
```

### 2. Adaptive Query Execution (AQE)

AQE optimizes queries dynamically based on runtime statistics:

#### **AQE Configuration**
```python
# Enable AQE features
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

#### **AQE Optimizations**

**1. Coalesce Partitions**
```python
# Automatically reduces partition count for small datasets
# Before: 200 partitions with 1KB each
# After: 10 partitions with 20KB each
```

**2. Skew Join Optimization**
```python
# Detects and handles data skew in joins
# Splits large partitions, replicates small side
# Improves performance for skewed datasets
```

**3. Dynamic Partition Pruning**
```python
# Prunes partitions during query execution
# Uses runtime filter information
# Reduces data scanning significantly
```

**4. Join Strategy Conversion**
```python
# Converts sort-merge joins to broadcast joins
# When one side becomes small after filtering
# Eliminates shuffle operations
```

#### **AQE Performance Monitoring**
```python
# View AQE optimizations in query plans
df.explain("extended")

# Monitor AQE metrics in Spark UI
# - Partition coalescing events
# - Skew join handling
# - Join strategy changes
```

### 3. Partition Operations

Strategic partitioning improves query performance through partition pruning:

#### **Partition Strategy Design**
```python
# Date-based partitioning for time-series data
orders_df.write.format("delta") \
    .partitionBy("year", "month") \
    .save(orders_path)

# Multi-level partitioning
orders_df.write.format("delta") \
    .partitionBy("country", "year", "month") \
    .save(orders_path)
```

#### **Partition Pruning**
```sql
-- Efficient: Uses partition pruning
SELECT * FROM orders 
WHERE year = 2024 AND month = 6

-- Inefficient: Scans all partitions
SELECT * FROM orders 
WHERE order_date >= '2024-06-01'
```

#### **Partition Management**
```sql
-- View partition information
SHOW PARTITIONS delta.`s3a://path/to/orders`

-- Add new partitions
ALTER TABLE delta.`s3a://path/to/orders` 
ADD PARTITION (year=2024, month=12)

-- Drop old partitions
ALTER TABLE delta.`s3a://path/to/orders`
DROP PARTITION (year=2023, month=1)
```

#### **Partitioning Best Practices**
- **Cardinality**: Aim for 10MB-1GB per partition
- **Query patterns**: Partition by frequently filtered columns
- **Partition count**: Avoid too many small partitions
- **Evolution**: Plan for partition scheme changes

### 4. Bloom Filters

Bloom Filters enable efficient data skipping for point lookups:

#### **Configuring Bloom Filters**
```sql
-- Enable Bloom Filters on high-cardinality columns
ALTER TABLE delta.`s3a://path/to/orders`
SET TBLPROPERTIES (
    'delta.bloomFilter.user_id.enabled' = 'true',
    'delta.bloomFilter.user_id.fpp' = '0.1'
)
```

#### **Bloom Filter Parameters**
- **False Positive Probability (FPP)**: Balance between accuracy and size
  - `0.1` (10%): Smaller filter, more false positives
  - `0.01` (1%): Larger filter, fewer false positives
- **Indexed columns**: Limit to high-cardinality lookup columns
- **File-level filtering**: Skips entire files during queries

#### **Performance Impact**
```python
# Without Bloom Filter: Scan all files
SELECT * FROM orders WHERE user_id = 'USER12345'
# Files scanned: 100 files

# With Bloom Filter: Skip irrelevant files  
SELECT * FROM orders WHERE user_id = 'USER12345'
# Files scanned: 2 files (98% reduction)
```

#### **Bloom Filter Use Cases**
- **User lookups**: Finding specific user records
- **ID searches**: Searching by unique identifiers
- **Categorical filters**: Filtering by specific categories
- **Point queries**: Exact match queries on indexed columns

### 5. Performance Measurement & Analysis

#### **Query Performance Metrics**
```python
# Measure query execution time
import time

start_time = time.time()
result = spark.sql("SELECT * FROM table WHERE condition").collect()
execution_time = time.time() - start_time

print(f"Query executed in {execution_time:.3f} seconds")
```

#### **Table Statistics**
```sql
-- Analyze table statistics
ANALYZE TABLE delta.`s3a://path/to/table` COMPUTE STATISTICS

-- View detailed statistics
DESCRIBE DETAIL delta.`s3a://path/to/table`

-- Column-level statistics
DESCRIBE TABLE EXTENDED delta.`s3a://path/to/table`
```

#### **Optimization Impact Analysis**
```python
# Before optimization
files_before = spark.sql("DESCRIBE DETAIL table").collect()[0]["numFiles"]
size_before = spark.sql("DESCRIBE DETAIL table").collect()[0]["sizeInBytes"]

# After optimization  
spark.sql("OPTIMIZE table ZORDER BY (col1, col2)")

files_after = spark.sql("DESCRIBE DETAIL table").collect()[0]["numFiles"]
size_after = spark.sql("DESCRIBE DETAIL table").collect()[0]["sizeInBytes"]

print(f"File reduction: {files_before} → {files_after}")
print(f"Size change: {size_before} → {size_after}")
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
  /opt/bitnami/spark/jobs/spark/mod-6/demo-6.py
```

### Step 3: Monitor Performance
Watch for:
- ⚡ Query execution times before/after optimization
- 📊 File count and size changes
- 🎯 Partition pruning effectiveness  
- 🌸 Bloom Filter data skipping rates

## Expected Output

The demo demonstrates significant performance improvements:

```
📊 Table details before optimization:
+--------+------------+
|numFiles|sizeInBytes |
+--------+------------+
|     45 |  125847392 |
+--------+------------+

✅ OPTIMIZE completed in 3.24 seconds
✅ Z-ORDER completed in 5.67 seconds

📊 Table details after optimization:
+--------+------------+
|numFiles|sizeInBytes |
+--------+------------+
|      8 |  118203571 |
+--------+------------+

⚡ Testing query performance:
✅ Query completed in 0.234 seconds

🌸 Bloom Filter data skipping:
✅ Bloom Filter query completed in 0.089 seconds
📊 Found 3 matching records (98% file skip rate)
```

## Key Concepts Learned

### Optimization Hierarchy
```
Query Performance Optimization:

1. Physical Layout
   ├── Partitioning (coarse-grained)
   ├── Z-ORDER (fine-grained)  
   └── Liquid Clustering (automatic)

2. Runtime Optimization
   ├── AQE (adaptive execution)
   ├── Bloom Filters (data skipping)
   └── Statistics (cost-based optimization)

3. Configuration Tuning
   ├── Spark settings
   ├── Delta properties
   └── Storage optimization
```

### Optimization Decision Matrix
```
Workload Type          | Recommended Optimization
-----------------------|------------------------
Point lookups          | Bloom Filters + Z-ORDER
Range queries          | Partitioning + Z-ORDER  
Analytical queries     | AQE + Liquid Clustering
Mixed workloads        | Combination approach
Time-series data       | Date partitioning + Z-ORDER
High-cardinality joins | AQE + appropriate indexing
```

### Performance Trade-offs
```
Technique        | Write Cost | Storage | Query Speed | Maintenance
-----------------|------------|---------|-------------|------------
Partitioning     | Medium     | Low     | High        | Medium
Z-ORDER          | High       | Low     | High        | High  
Liquid Clustering| Low        | Low     | High        | Low
Bloom Filters    | Low        | Medium  | High        | Low
AQE              | None       | None    | Medium      | None
```

## Troubleshooting

### Common Issues and Solutions

1. **Z-ORDER Memory Issues**
   ```
   OutOfMemoryError during Z-ORDER operation
   ```
   **Solution**: Increase driver memory, reduce Z-ORDER columns, or use smaller datasets

2. **Partition Proliferation**
   ```
   Too many small partitions created
   ```
   **Solution**: Adjust partition column cardinality, use coarser granularity

3. **AQE Not Triggering**
   ```
   AQE optimizations not being applied
   ```
   **Solution**: Check AQE configuration, ensure sufficient data size thresholds

4. **Bloom Filter False Negatives**
   ```
   Expected records not found
   ```
   **Solution**: Check Bloom Filter configuration, verify FPP settings

## Exercises

### Exercise 1: Query Performance Optimization
**Objective**: Optimize a slow analytical query using multiple techniques.

**Tasks**:
1. Create a large orders table (100K+ records) with multiple dimensions
2. Write a complex analytical query with multiple joins and filters
3. Measure baseline performance without optimizations
4. Apply Z-ORDER on frequently queried columns
5. Enable AQE and measure improvements
6. Add Bloom Filters for point lookups
7. Compare performance gains from each optimization

### Exercise 2: Partitioning Strategy Design
**Objective**: Design optimal partitioning strategy for time-series data.

**Tasks**:
1. Create UberEats orders data spanning multiple months
2. Test different partitioning strategies:
   - No partitioning
   - By month only
   - By country and month
   - By restaurant_id and month
3. Measure query performance for different access patterns:
   - Single day queries
   - Monthly aggregations
   - Restaurant-specific analysis
4. Determine optimal partition strategy
5. Implement partition maintenance procedures

### Exercise 3: AQE Advanced Configuration
**Objective**: Fine-tune AQE for specific workload patterns.

**Tasks**:
1. Create datasets with known skew patterns
2. Configure AQE parameters:
   - Partition coalescing thresholds
   - Skew join detection settings
   - Local shuffle reader configuration
3. Test with various join scenarios:
   - Broadcast joins
   - Sort-merge joins
   - Skewed joins
4. Monitor AQE decisions in Spark UI
5. Create optimal AQE configuration for workload

### Exercise 4: Comprehensive Performance Benchmarking
**Objective**: Create complete performance optimization framework.

**Tasks**:
1. Design benchmark suite with representative queries
2. Implement automated performance testing
3. Test all optimization techniques:
   - Baseline (no optimization)
   - Partitioning only
   - Z-ORDER only
   - Bloom Filters only
   - AQE only
   - Combined optimizations
4. Create performance comparison matrix
5. Develop optimization recommendation system
6. Document best practices for different scenarios

## Best Practices

### 1. Optimization Strategy
- **Profile first**: Understand query patterns before optimizing
- **Measure impact**: Always benchmark before and after changes
- **Iterative approach**: Apply optimizations incrementally
- **Monitor continuously**: Performance can degrade over time

### 2. Z-ORDER Implementation
- **Column selection**: Choose most selective filter columns
- **Column order**: Put most selective columns first
- **Frequency**: Balance optimization cost with query improvement
- **Maintenance**: Schedule regular optimization runs

### 3. Partitioning Design
- **Cardinality planning**: Aim for optimal partition sizes
- **Query alignment**: Match partition keys to filter patterns
- **Evolution strategy**: Plan for changing requirements
- **Maintenance automation**: Automate partition lifecycle management

### 4. Performance Monitoring
- **Baseline establishment**: Document initial performance metrics
- **Continuous monitoring**: Track performance trends over time
- **Alert thresholds**: Set up alerts for performance degradation
- **Regular reviews**: Periodically review optimization effectiveness

## Next Steps

After completing this performance optimization demo, you'll be ready for:

- **Advanced Streaming**: Real-time performance optimization with Delta Lake
- **Production Monitoring**: Enterprise-grade performance monitoring systems
- **Auto-optimization**: Automated performance tuning systems

## Additional Resources

- [Delta Lake Optimization Guide](https://docs.delta.io/latest/optimizations-oss.html)
- [Z-ORDER Documentation](https://docs.delta.io/latest/delta-batch.html#z-ordering-multi-dimensional-clustering)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [Bloom Filters in Delta Lake](https://docs.delta.io/latest/delta-bloom-filters.html)

---

🎯 **Remember**: Performance optimization is an iterative process. Start with understanding your workload patterns, apply optimizations systematically, and always measure the impact. The best optimization strategy depends on your specific use case and query patterns.

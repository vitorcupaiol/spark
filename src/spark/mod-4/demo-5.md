# Demo 5: DML Operations & Data Management

## Overview

This demo explores Delta Lake's data manipulation language (DML) operations and advanced data management capabilities. You'll learn how to perform UPDATE, DELETE, and MERGE operations, implement Change Data Capture (CDC), manage Slowly Changing Dimensions (SCD), and execute multi-table ACID transactions using real UberEats operational data.

## Learning Objectives

By the end of this demo, you will be able to:

- ✅ Execute DML operations (UPDATE, DELETE, MERGE) on Delta tables
- ✅ Implement Change Data Capture using Delta Change Data Feed
- ✅ Manage Slowly Changing Dimensions with Delta MERGE operations
- ✅ Perform multi-table ACID transactions for complex business logic
- ✅ Handle real-time data modifications in production scenarios
- ✅ Understand performance implications of different DML patterns

## Prerequisites

- Completed Demo 1-4 (Delta Lake Foundation through Schema Management)
- Docker environment with Spark cluster running
- Access to MinIO S3-compatible storage
- UberEats sample data loaded in MinIO
- Understanding of ACID transaction concepts

## File Structure

```
src/spark/mod-5/
├── demo-5.py
└── demo-5.md
```

## Demo Structure

### 1. DML Operations Fundamentals

Delta Lake supports full DML operations with ACID guarantees:

#### **UPDATE Operations**
```python
# Update order status using Delta Table API
orders_table = DeltaTable.forPath(spark, table_path)

orders_table.update(
    condition=col("order_id") == "ORD001",
    set={"status": lit("preparing"), "updated_at": current_timestamp()}
)

# SQL syntax alternative
spark.sql("""
    UPDATE delta.`s3a://path/to/orders` 
    SET status = 'delivered' 
    WHERE order_id = 'ORD001'
""")
```

#### **DELETE Operations**
```python
# Delete cancelled orders
orders_table.delete(condition=col("status") == "cancelled")

# Conditional deletion with complex predicates
orders_table.delete(
    condition=(col("total_amount") < 5.0) & 
              (col("created_date") < "2024-01-01")
)
```

#### **MERGE Operations (UPSERT)**
```python
# Upsert pattern for order updates
orders_table.alias("target").merge(
    updates_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdate(set={
    "status": col("source.status"),
    "updated_at": current_timestamp()
}).whenNotMatchedInsert(values={
    "order_id": col("source.order_id"),
    "status": col("source.status"),
    "created_at": current_timestamp()
}).execute()
```

### 2. Change Data Capture (CDC)

Delta Lake's native Change Data Feed captures all data changes automatically:

#### **Enabling Change Data Feed**
```python
# Enable CDF on table creation
df.write.format("delta") \
  .option("delta.enableChangeDataFeed", "true") \
  .save(table_path)

# Enable on existing table
spark.sql(f"""
    ALTER TABLE delta.`{table_path}` 
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
```

#### **Reading Change Data Feed**
```python
# Read all changes from version 0
changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load(table_path)

# Change types: insert, update_preimage, update_postimage, delete
changes_df.select("order_id", "_change_type", "_commit_version").show()

# Time-based change reading
changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", "2024-01-01 00:00:00") \
    .load(table_path)
```

#### **CDC Use Cases**
- **Real-time data synchronization**: Stream changes to downstream systems
- **Audit trail creation**: Track all data modifications for compliance
- **Data lake to data warehouse**: Incrementally update analytical systems
- **Event-driven architectures**: Trigger workflows based on data changes

### 3. Slowly Changing Dimensions (SCD)

Implement SCD Type 2 patterns using Delta MERGE operations:

#### **SCD Type 2 Implementation**
```python
# Customer address changes with full history
customers_table.alias("target").merge(
    updates_df.alias("source"),
    "target.customer_id = source.customer_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.address != source.address",
    set={
        "is_current": "false",
        "end_date": "current_timestamp()"
    }
).whenNotMatchedInsert(values={
    "customer_id": "source.customer_id",
    "address": "source.address", 
    "is_current": "true",
    "start_date": "current_timestamp()",
    "end_date": "null"
}).execute()

# Insert new versions for changed records
new_versions_df.write.format("delta").mode("append").save(table_path)
```

#### **SCD Benefits with Delta Lake**
- **Single operation**: Handle complex SCD logic in one MERGE
- **ACID compliance**: Atomic updates ensure consistency
- **Performance**: Optimized for large-scale dimension management
- **Time travel**: Access any historical version when needed

### 4. Multi-Table Transactions

Coordinate changes across multiple related tables:

#### **Order Processing Transaction**
```python
# Atomic order processing: update inventory + create order
def process_order(order_data, inventory_updates):
    try:
        # Update inventory levels
        inventory_table.update(
            condition=col("product_id") == "PROD001",
            set={"quantity": col("quantity") - 2}
        )
        
        # Insert order record
        order_df.write.format("delta").mode("append").save(orders_path)
        
        # Insert order items
        items_df.write.format("delta").mode("append").save(items_path)
        
        print("✅ Order processed successfully")
        
    except Exception as e:
        print(f"❌ Transaction failed: {e}")
        # In production: implement rollback logic
```

#### **Transaction Patterns**
- **Order fulfillment**: Inventory, orders, payments coordination
- **Customer onboarding**: Profile, preferences, notifications setup
- **Data migration**: Moving data between systems atomically
- **Batch processing**: Coordinated updates across fact/dimension tables

### 5. Advanced DML Patterns

#### **Conditional MERGE with Complex Logic**
```python
# Restaurant menu updates with business rules
menu_table.alias("target").merge(
    new_menu_df.alias("source"),
    "target.restaurant_id = source.restaurant_id AND target.item_id = source.item_id"
).whenMatchedUpdate(
    condition="""
        source.price != target.price OR 
        source.availability != target.availability OR
        source.description != target.description
    """,
    set={
        "price": "source.price",
        "availability": "source.availability", 
        "description": "source.description",
        "last_updated": "current_timestamp()"
    }
).whenNotMatchedInsert(values={
    "restaurant_id": "source.restaurant_id",
    "item_id": "source.item_id",
    "price": "source.price",
    "availability": "source.availability",
    "created_at": "current_timestamp()"
}).execute()
```

#### **Bulk Operations Optimization**
```python
# Efficient bulk updates using broadcast joins
large_updates_df = updates_df.hint("broadcast")

target_table.alias("target").merge(
    large_updates_df.alias("source"),
    "target.key = source.key"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
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
  /opt/bitnami/spark/jobs/spark/mod-5/demo-5.py
```

### Step 3: Monitor Execution
Watch for:
- ✅ DML operation results (UPDATE/DELETE/MERGE)
- 📊 Change Data Feed captures
- 🏠 SCD Type 2 history tracking
- 🔗 Multi-table transaction coordination

## Expected Output

The demo demonstrates complete DML operation lifecycle:

```
✅ Created orders table with 4 orders
✅ Updated ORD001 to 'preparing'
✅ Deleted cancelled order ORD003
✅ MERGE completed: Updated ORD001, inserted ORD004

📊 Change Data Feed captured all operations:
+--------+-------+------------+---------------+
|order_id| status|_change_type|_commit_version|
+--------+-------+------------+---------------+
|  ORD001| placed|      insert|              0|
|  ORD001|preparing|update_postimage|        1|
|  ORD003|cancelled| delete|              2|
|  ORD004| placed|      insert|              3|
+--------+-------+------------+---------------+

🏠 CUST001 complete address history:
+--------+----------------+----------+
|customer_id|        address|is_current|
+--------+----------------+----------+
|  CUST001|    123 Main St|     false|
|  CUST001| 999 New Street|      true|
+--------+----------------+----------+
```

## Key Concepts Learned

### DML Operation Types
```
INSERT  → Add new records (append/overwrite modes)
UPDATE  → Modify existing records in-place  
DELETE  → Remove records based on conditions
MERGE   → Combine INSERT/UPDATE/DELETE (UPSERT)
```

### Change Data Capture Flow
```
Original Data → DML Operation → CDF Capture → Change Records
     ↓              ↓              ↓              ↓
   [A,B,C]    UPDATE A=A'    [A→A', INSERT]   Downstream
                              [DELETE C]        Systems
```

### SCD Type 2 Pattern
```
Initial State:
CUST001 | John Doe | 123 Main St | 2024-01-01 | NULL | true

After Address Change:
CUST001 | John Doe | 123 Main St | 2024-01-01 | 2024-02-01 | false
CUST001 | John Doe | 999 New St  | 2024-02-01 | NULL       | true
```

## Troubleshooting

### Common Issues and Solutions

1. **Concurrent Modification Exception**
   ```
   ConcurrentModificationException: Files were added to partition
   ```
   **Solution**: Implement retry logic or use optimistic concurrency

2. **MERGE Condition Ambiguity**
   ```
   AnalysisException: Column reference is ambiguous
   ```
   **Solution**: Use proper table aliases in MERGE conditions

3. **Change Data Feed Not Available**
   ```
   DeltaIllegalArgumentException: Change data feed not enabled
   ```
   **Solution**: Enable CDF on table with ALTER TABLE command

4. **Performance Issues with Large MERGE**
   ```
   Slow performance on large MERGE operations
   ```
   **Solution**: Use broadcast hints, optimize join conditions, consider partitioning

## Exercises

### Exercise 1: Order Lifecycle Management
**Objective**: Implement complete order status management system.

**Tasks**:
1. Create orders table with multiple status stages
2. Implement status transition rules using UPDATE operations
3. Handle order cancellations with DELETE operations
4. Use MERGE for order modifications (quantity, items)
5. Track all changes using Change Data Feed
6. Create status transition audit report

### Exercise 2: Customer 360 with SCD
**Objective**: Build comprehensive customer profile management.

**Tasks**:
1. Design customer dimension with SCD Type 2 for address changes
2. Implement profile updates (email, phone, preferences)
3. Handle customer lifecycle events (signup, activation, churn)
4. Create customer journey analysis using historical data
5. Implement data quality rules for customer updates

### Exercise 3: Inventory Management System
**Objective**: Build real-time inventory tracking with ACID transactions.

**Tasks**:
1. Create product inventory table with stock levels
2. Implement order processing that updates inventory atomically
3. Handle restocking operations with batch updates
4. Implement low-stock alerts using conditional logic
5. Create inventory audit trail with all stock movements

### Exercise 4: CDC-Based Data Pipeline
**Objective**: Build event-driven data pipeline using Change Data Feed.

**Tasks**:
1. Create source table with Change Data Feed enabled
2. Simulate various DML operations over time
3. Build downstream processor that consumes CDF changes
4. Implement change type filtering (inserts only, deletes only)
5. Create real-time dashboard showing change statistics

### Exercise 5: Complex Business Logic with MERGE
**Objective**: Implement sophisticated upsert patterns.

**Tasks**:
1. Design restaurant menu management system
2. Handle complex business rules:
   - Price changes require approval
   - Availability toggles are immediate
   - New items need validation
3. Implement conditional MERGE with multiple business rules
4. Add audit logging for all menu changes
5. Create rollback capability for erroneous updates

## Best Practices

### 1. DML Performance Optimization
- Use appropriate partitioning for large tables
- Leverage predicate pushdown in WHERE clauses
- Consider broadcast joins for small dimension updates
- Monitor operation metrics and optimize accordingly

### 2. Change Data Capture Strategy
- Enable CDF only when needed (storage overhead)
- Choose appropriate retention periods for change data
- Consider downstream system requirements
- Plan for change data volume growth

### 3. Transaction Design
- Keep transactions as short as possible
- Handle concurrent modification gracefully
- Implement proper error handling and rollback
- Consider isolation levels and locking implications

### 4. SCD Implementation
- Design proper surrogate key strategies
- Handle late-arriving data appropriately
- Consider storage implications of full history
- Implement efficient current record queries

## Next Steps

After completing this DML operations and data management demo, you'll be ready for:

- **Demo 6**: Performance Optimization - Tune Delta Lake for maximum performance

## Additional Resources

- [Delta Lake DML Reference](https://docs.delta.io/latest/delta-update.html)
- [Change Data Feed Documentation](https://docs.delta.io/latest/delta-change-data-feed.html)
- [MERGE Command Guide](https://docs.delta.io/latest/delta-merge.html)
- [Transaction Log Details](https://docs.delta.io/latest/delta-log.html)

---

🎯 **Remember**: DML operations are the foundation of production data systems. Master these patterns to build robust, scalable, and maintainable data pipelines that can handle complex business requirements while maintaining ACID guarantees.

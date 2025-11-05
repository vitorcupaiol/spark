# Demo 4: Schema Management & Evolution - Step-by-Step Guide

## Overview

This demo showcases Delta Lake's powerful schema management capabilities using real UberEats data. You'll learn how to enforce data integrity, evolve schemas safely, and implement business rules—all essential skills for production data pipelines.

**Duration:** ~15 minutes  
**Prerequisites:** Docker environment with Spark + Delta Lake  
**Data:** Real UberEats orders, products, and customer data

---

## Learning Objectives

By the end of this demo, you'll understand:

✅ **Schema Enforcement** - Protecting data integrity with strict validation  
✅ **Schema Evolution** - Adding columns without breaking existing applications  
✅ **Column Mapping** - Business-friendly column names without data movement  
✅ **Generated Columns** - Automated computed fields for analytics  
✅ **CHECK Constraints** - Enforcing business rules at the database level

---

## Setup & Execution

### Run the Demo

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-4.py
```

### Expected Output Structure

```
🚀 Delta Lake Schema Management & Evolution Demo
📊 UberEats Data Pipeline - Real-World Schema Scenarios

🛡️  Schema Enforcement - Protecting Data Integrity
🔄 Schema Evolution - Adding Business Context  
🗺️  Column Mapping - Business-Friendly Names
⚙️  Generated Columns - Automated Analytics
✅ CHECK Constraints - Business Rules Enforcement

🎯 DEMO COMPLETE: Schema Management Mastery
```

---

## Part 1: Schema Enforcement 🛡️

### What Happens
- Loads real UberEats orders data from S3
- Creates a Delta table with strict schema validation
- Tests what happens when invalid data is inserted

### Key Learning Points

**Schema Enforcement Prevents:**
- Wrong data types (e.g., string in numeric field)
- Missing required fields
- Data corruption from bad imports

**Business Value:**
- **Data Quality**: Prevents garbage data from entering your lakehouse
- **Debugging**: Issues caught early, not discovered in production
- **Compliance**: Ensures data meets business requirements

### What You'll See

```
📊 Loaded 20 UberEats orders
✅ Orders table created with schema enforcement
🔍 Testing schema enforcement:
✅ Schema enforcement blocked invalid data
📊 Final table: 20 valid records
```

### Expected Results

The demo will:
1. ✅ Successfully load real UberEats order data
2. ✅ Create a Delta table with enforced schema
3. ✅ Block invalid data from being inserted
4. ✅ Show only clean, valid records in the final table

---

## Part 2: Schema Evolution 🔄

### What Happens
- Starts with a basic orders table
- Adds new business context columns using `mergeSchema=true`
- Shows how old records handle new columns (NULL values)

### Key Learning Points

**Schema Evolution Enables:**
- Adding new columns without breaking existing queries
- Business context expansion (delivery fees, customer segments)
- Backward compatibility with existing applications

**Business Value:**
- **Agility**: Adapt data model as business requirements change
- **Zero Downtime**: Add columns without stopping production systems
- **Data Evolution**: Capture new business insights over time

### What You'll See

```
📊 Created base orders table
✅ Added delivery context columns with mergeSchema
📋 Evolved schema:
 |-- order_id: string
 |-- total_amount: double
 |-- delivery_fee: double (newly added)
 |-- customer_segment: string (newly added)
```

### Expected Results

The demo will:
1. ✅ Start with basic order columns
2. ✅ Add delivery context (fees, estimates, segments) 
3. ✅ Show schema evolution without data loss
4. ✅ Demonstrate NULL handling for existing records

---

## Part 3: Column Mapping 🗺️

### What Happens
- Creates a products table with technical column names
- Attempts to rename columns to business-friendly names
- Falls back to views if column mapping isn't supported

### Key Learning Points

**Column Mapping Provides:**
- Business-friendly column names (`price` → `unit_price`)
- Zero data movement (metadata-only changes)
- Improved data discoverability for business users

**Version Compatibility:**
- Requires Delta Lake writer version 5+
- Graceful fallback to business views
- Same end result with different approaches

### What You'll See

**If Supported:**
```
✅ Column mapping mode enabled
   • 'name' → 'product_name'
   • 'price' → 'unit_price'
💡 Zero data movement - only metadata changes
```

**If Not Supported (Most Common):**
```
⚠️  Column mapping failed: protocol version...
🔄 Using business view as alternative
✅ Created business-friendly view with renamed columns
```

### Expected Results

Either approach will:
1. ✅ Provide business-friendly column names
2. ✅ Show the same data with better naming
3. ✅ Demonstrate production-ready fallback patterns
4. ✅ Teach version compatibility strategies

---

## Part 4: Generated Columns ⚙️

### What Happens
- Attempts to create table with auto-computed columns
- Falls back to manual computation if not supported
- Shows customer analytics with derived fields

### Key Learning Points

**Generated Columns Automate:**
- Full name computation (`first_name + last_name`)
- Email domain extraction for analytics
- Consistent derived field calculations

**Production Patterns:**
- Manual computation achieves same results
- Fallback patterns for version compatibility
- Computed fields for business intelligence

### What You'll See

**Typical Output (Expected):**
```
⚠️  Generated columns failed: TABLE_OPERATION not supported
🔄 Using manual computation approach
✅ Manual computed columns created
📊 Customer analytics with computed fields:
|John      |Doe      |john@gmail.com    |John Doe     |gmail.com   |
```

### Expected Results

The demo will:
1. ⚠️ Show generated columns aren't supported (normal)
2. ✅ Create identical results with manual computation
3. ✅ Display customer analytics with derived fields
4. ✅ Demonstrate production-ready approaches

---

## Part 5: CHECK Constraints ✅

### What Happens
- Creates orders table with business rule validation
- Attempts to add constraints for order amounts and fees
- Tests constraint enforcement with invalid data

### Key Learning Points

**CHECK Constraints Enforce:**
- Business rules (positive order amounts)
- Data validity (reasonable delivery fees)
- Referential integrity across business domains

**Production Value:**
- **Data Quality**: Prevents invalid business data
- **Compliance**: Enforces regulatory requirements
- **Cost Savings**: Catches errors before processing

### What You'll See

**If Supported:**
```
✅ Business rules added:
   • total_amount > 0
   • delivery_fee between 0 and 15.99
✅ Valid orders inserted
✅ Business rule blocked invalid order amount
```

**If Not Supported (Common):**
```
⚠️  CHECK constraints not supported
✅ Manual business rules validation applied
📊 Orders following business rules:
```

### Expected Results

Either approach will:
1. ✅ Enforce positive order amounts
2. ✅ Validate delivery fee ranges
3. ✅ Block invalid business data
4. ✅ Show clean, rule-compliant data

---

## Troubleshooting Guide

### Common Issues & Solutions

#### 1. **Schema Evolution Fails**
```
Error: Cannot resolve column names
```
**Solution:** Check column name spelling and use `option("mergeSchema", "true")`

#### 2. **Column Mapping Not Supported**
```
Error: protocol version does not support
```
**Solution:** This is expected - the view fallback provides the same functionality

#### 3. **Generated Columns Fail**
```
Error: TABLE_OPERATION not supported
```
**Solution:** This is normal - manual computation achieves identical results

#### 4. **Data Loading Issues**
```
Error: Could not load UberEats data
```
**Solution:** Demo uses synthetic fallback data automatically

#### 5. **CHECK Constraints Not Working**
```
Error: CHECK constraints might not be supported
```
**Solution:** Manual validation filter provides same protection

### Performance Tips

1. **Use Small Datasets**: Demo uses `.limit(10-20)` for fast execution
2. **Enable AQE**: Adaptive Query Execution is configured automatically
3. **Partition Management**: Use `coalesce()` for small datasets
4. **Cache Strategy**: Results cached automatically between demos

---

## Business Applications

### Real-World Use Cases

#### **E-commerce Platforms**
- **Schema Evolution**: Add customer segments, loyalty tiers
- **Generated Columns**: Order totals, tax calculations
- **CHECK Constraints**: Valid price ranges, quantity limits

#### **Financial Services**
- **Schema Enforcement**: Transaction amount validation
- **Column Mapping**: Regulatory field naming requirements
- **CHECK Constraints**: Account balance rules, transfer limits

#### **Healthcare Data**
- **Schema Evolution**: Add new patient metrics over time
- **Generated Columns**: BMI calculations, age from birthdate
- **CHECK Constraints**: Valid vital sign ranges

#### **IoT & Manufacturing**
- **Schema Enforcement**: Sensor data type validation
- **Schema Evolution**: Add new sensor types without downtime
- **CHECK Constraints**: Operating parameter limits

---

## Next Steps

### Immediate Actions
1. **Review Logs**: Check which features worked in your environment
2. **Experiment**: Try different data types and constraints
3. **Documentation**: Note version compatibility for your use cases

### Advanced Exploration
1. **Custom Constraints**: Add complex business rules
2. **Performance Testing**: Measure schema evolution impact
3. **Integration**: Connect to BI tools using business-friendly names
4. **Automation**: Script schema changes for CI/CD pipelines

### Production Readiness
1. **Version Strategy**: Plan Delta Lake version upgrades
2. **Fallback Patterns**: Implement graceful degradation
3. **Monitoring**: Track schema evolution in production
4. **Documentation**: Maintain schema change logs

---

## Summary

This demo showcased Delta Lake's comprehensive schema management capabilities:

✅ **Schema Enforcement** protects data integrity from day one  
✅ **Schema Evolution** enables agile business requirement changes  
✅ **Column Mapping** improves data discoverability for business users  
✅ **Generated Columns** automate consistent derived field calculations  
✅ **CHECK Constraints** enforce business rules at the data layer

**Key Takeaway:** Delta Lake provides enterprise-grade schema management with graceful fallbacks for version compatibility, ensuring your data pipelines remain robust and maintainable as your business grows.

---

## Additional Resources

- [Delta Lake Schema Evolution Documentation](https://docs.delta.io/latest/delta-batch.html#schema-validation-and-evolution)
- [Column Mapping Guide](https://docs.delta.io/latest/delta-column-mapping.html)
- [Generated Columns Reference](https://docs.delta.io/latest/delta-batch.html#deltausegeneratedcolumns)
- [CHECK Constraints Documentation](https://docs.delta.io/latest/delta-constraints.html)
- [Schema Enforcement Best Practices](https://docs.delta.io/latest/best-practices.html)

# USE CASE 1: Real-Time Delivery Monitoring with Streaming

## 🎯 Business Context

**UberEats** processes **thousands of deliveries simultaneously** across multiple cities. Each delivery generates real-time events from multiple systems:

1. **Orders System** (Kafka)
   - New orders placed by customers
   - Order details: restaurant, driver, amount, timestamp

2. **Status Tracking System** (Kafka)
   - Delivery status updates (placed, preparing, picked up, delivered)
   - Real-time status changes throughout delivery lifecycle

3. **GPS System** (Kafka - future extension)
   - Driver location updates
   - Route tracking

4. **Payment System** (Kafka - future extension)
   - Payment confirmations
   - Transaction details

**The Problem**:
- **Operations** needs to monitor all deliveries in **real-time**
- **Delayed orders** must be detected in **< 3 seconds**
- **External systems** (alerting, monitoring) need critical events
- **Analytics** needs real-time KPIs (orders/min, avg delivery time)
- **Restaurant/Driver performance** must be tracked continuously

**The Challenge**:
- Process **10,000+ events/second** with low latency
- **Join** multiple streaming sources (orders + status)
- **Filter** critical events (delayed orders)
- **Publish** alerts to external Kafka topic (Sink)
- **Aggregate** metrics in real-time (windowed aggregations)
- Handle **late-arriving data** (watermarking)

---

## 🏗️ Solution Architecture

### **Lakeflow Declarative Pipelines + Medallion Architecture + Streaming + Sinks**

```
┌──────────────────────────────────────────────────────────────────────┐
│              REAL-TIME DELIVERY MONITORING SYSTEM                     │
│                                                                       │
│  Goal: Monitor deliveries in real-time and alert on critical events  │
└──────────────────────────────────────────────────────────────────────┘

📥 DATA SOURCES (Kafka Streams)
    ├─ kafka/orders/ (New orders)
    └─ kafka/status/ (Status updates)
    ↓
🟤 BRONZE LAYER (Raw Streaming Ingestion)
    ├─ bronze_orders (Streaming Table - Python Auto Loader)
    └─ bronze_status (Streaming Table - SQL read_files)
    ↓
🥈 SILVER LAYER (Stream Processing & Joins)
    ├─ silver_order_status (Stream-to-stream JOIN with watermarking)
    └─ silver_delayed_orders (Filtered critical events)
    ↓
    ├──→ 📤 SINK LAYER (External Integration)
    │    ├─ delivery_alerts_sink (Kafka endpoint)
    │    └─ sink_kafka_alerts (Formatted messages)
    │
    └──→ 🥇 GOLD LAYER (Real-Time Analytics)
         ├─ gold_restaurant_performance (Per-restaurant metrics)
         ├─ gold_driver_performance (Per-driver metrics)
         ├─ gold_delivery_time_distribution (Speed bucketing)
         └─ gold_system_health (Platform-wide KPIs)
    ↓
📊 CONSUMERS (Dashboards, Alerting Systems, ML Models)
```

---

## 📋 Detailed Use Case Plan

### **PHASE 1: Bronze Layer - Raw Streaming Ingestion**

**Duration**: 10 minutes

**What Happens**:
- Ingest raw streaming data from **Kafka topics** into Bronze layer
- Use **Streaming Tables** (continuous processing)
- Demonstrate **two approaches**: Python (Auto Loader) + SQL (read_files)
- Enable **Change Data Feed** for downstream lineage

**Why Streaming Tables?**
- Delivery events happen **in real-time** (not batch)
- Need **continuous processing** (not scheduled)
- **Sub-second latency** required for operations

**Tables Created**:

1. **bronze_orders** (Python with Auto Loader)
   - **Source**: Azure Blob Storage (`kafka/orders/*.json`)
   - **Method**: `spark.readStream.format("cloudFiles")`
   - **Features**:
     - Schema hints for type safety
     - Metadata enrichment (`_metadata.file_path`, `_metadata.file_modification_time`)
     - Change Data Feed enabled (`delta.enableChangeDataFeed = true`)
   - **Fields**: user_key, restaurant_key, driver_key, payment_id, order_id, total_amount, order_date, dt_current_timestamp
   - **Type**: STREAMING TABLE
   - **Language**: Python (PySpark)

2. **bronze_status** (SQL with read_files)
   - **Source**: Azure Blob Storage (`kafka/status/*.json`)
   - **Method**: `read_files('path', format => 'json')`
   - **Features**:
     - Nested field extraction (`status.status_name`)
     - Direct SQL syntax (simpler for SQL users)
     - Change Data Feed enabled
   - **Fields**: order_identifier, status_id, status_name, dt_current_timestamp
   - **Type**: STREAMING TABLE
   - **Language**: SQL

**Key Concepts Demonstrated**:
- ✅ Streaming Tables (continuous processing)
- ✅ Auto Loader (cloudFiles) vs read_files
- ✅ Python vs SQL syntax (same result, different approach)
- ✅ Schema hints for type safety
- ✅ Metadata enrichment
- ✅ Change Data Feed for lineage

---

### **PHASE 2: Silver Layer - Stream Processing & Joins**

**Duration**: 15 minutes

**What Happens**:
- **Join** two streaming sources (orders + status) in real-time
- Apply **watermarking** to handle late-arriving data
- Use **time-bounded join** to prevent unbounded state
- Add **Data Quality expectations** (3 rules)
- **Filter** critical events (delayed orders)

**The Challenge**:
Stream-to-stream joins are **complex**:
- Both sides are **unbounded** (infinite data)
- Events can arrive **out of order**
- Without watermarking, **state grows forever** (OOM)
- Need to define **how long to wait** for matching events

**Tables Created**:

1. **silver_order_status** (Stream-to-stream JOIN)
   - **Purpose**: Unified view of orders + status
   - **Join Type**: LEFT JOIN (keep all orders, even without status yet)
   - **Join Key**: `o.order_id = s.order_identifier`
   - **Watermarking**:
     - Orders: 10 minutes (`withWatermark("order_date", "10 minutes")`)
     - Status: 10 minutes (`withWatermark("dt_current_timestamp", "10 minutes")`)
     - **Meaning**: Wait up to 10 min for late events, then drop
   - **Time Constraint**: `s.dt_current_timestamp >= o.order_date - interval 2 hours`
     - **Meaning**: Only join status within 2 hours of order
     - **Why**: Prevents unbounded state growth
   - **Data Quality Expectations** (3 rules):
     ```python
     @dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
     @dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
     @dlt.expect_or_drop("valid_total_amount", "total_amount > 0")
     ```
   - **Fields**: order_id, order_date, restaurant_key, driver_key, user_key, total_amount, status_id, status_name, status_time
   - **Type**: STREAMING TABLE
   - **Language**: Python (PySpark)

2. **silver_delayed_orders** (Filtered critical events)
   - **Purpose**: Identify orders taking too long
   - **Filter Logic**:
     ```python
     (status_name == 'preparing' AND 
      unix_timestamp(status_time) - unix_timestamp(order_date) > 1800)
     ```
     - **Meaning**: Orders in "preparing" status for > 30 minutes
   - **Use Case**: Alert operations team about delayed orders
   - **Fields**: Same as silver_order_status (filtered subset)
   - **Type**: STREAMING TABLE
   - **Language**: Python (PySpark)

**Key Concepts Demonstrated**:
- ✅ Stream-to-stream JOIN (LEFT JOIN)
- ✅ Watermarking (handle late data)
- ✅ Time-bounded joins (prevent state explosion)
- ✅ Data Quality expectations (expect_or_drop)
- ✅ Stream filtering (critical events)
- ✅ Timestamp arithmetic (detect delays)

---

### **PHASE 3: Sink Layer - External Integration**

**Duration**: 10 minutes

**What Happens**:
- **Publish** critical events to **external Kafka topic**
- Use **Databricks Sink** (dp.create_sink)
- Format messages as **key-value pairs** (Kafka standard)
- Demonstrate **exactly-once** delivery guarantee

**Why Sinks?**
- **External systems** need to be notified (alerting, monitoring)
- **Kafka** is the standard for event streaming
- **Decoupling**: Analytics in Databricks, alerting in external system
- **Real-time**: Events published as they happen

**Objects Created**:

1. **delivery_alerts_sink** (Kafka endpoint)
   - **Purpose**: Define external Kafka destination
   - **Command**:
     ```python
     dp.create_sink(
         name="delivery_alerts_sink",
         endpoint="<kafka_bootstrap_servers>",
         topic="delivery-alerts",
         format="kafka"
     )
     ```
   - **Configuration**: Bootstrap servers, topic name, format
   - **Type**: SINK (not a table)

2. **sink_kafka_alerts** (Staging table for formatting)
   - **Purpose**: Format delayed orders as Kafka messages
   - **Structure**:
     ```python
     key: order_id (STRING)
     value: JSON({
         "order_id": order_id,
         "restaurant_key": restaurant_key,
         "driver_key": driver_key,
         "delay_minutes": delay_minutes,
         "alert_type": "DELAYED_ORDER",
         "timestamp": current_timestamp()
     })
     ```
   - **Type**: STREAMING TABLE
   - **Language**: Python (PySpark)

3. **publish_to_kafka** (Append flow to sink)
   - **Purpose**: Connect staging table to sink
   - **Command**:
     ```python
     @dp.append_flow(
         target="delivery_alerts_sink",
         source="sink_kafka_alerts"
     )
     ```
   - **Behavior**: Automatically publishes new records to Kafka
   - **Guarantee**: Exactly-once delivery

**Key Concepts Demonstrated**:
- ✅ Sinks (external integration)
- ✅ dp.create_sink (define Kafka endpoint)
- ✅ Key-value message format
- ✅ dp.append_flow (publish to sink)
- ✅ Exactly-once delivery
- ✅ JSON serialization

---

### **PHASE 4: Gold Layer - Real-Time Analytics**

**Duration**: 15 minutes

**What Happens**:
- Create **real-time KPI tables** from streaming data
- Use **windowed aggregations** (10-minute tumbling windows)
- Apply **streaming-compatible functions** (approx_count_distinct)
- Generate **multiple perspectives** (restaurant, driver, system)

**The Challenge**:
Streaming aggregations have **limitations**:
- Can't use **exact COUNT(DISTINCT)** (unbounded memory)
- Can't use **window functions** (OVER clause)
- Must use **tumbling/sliding windows** for time-based aggregations

**Tables Created**:

1. **gold_restaurant_performance** (Per-restaurant metrics)
   - **Purpose**: Track each restaurant's performance in real-time
   - **Aggregation Window**: 10-minute tumbling windows
   - **Metrics**:
     ```sql
     - total_orders: COUNT(*)
     - unique_drivers: approx_count_distinct(driver_key)
     - total_revenue: SUM(total_amount)
     - avg_order_value: AVG(total_amount)
     - delayed_orders: SUM(CASE WHEN delayed THEN 1 ELSE 0 END)
     - delay_rate: delayed_orders / total_orders
     ```
   - **Dimensions**: restaurant_key, window_start, window_end
   - **Type**: STREAMING TABLE
   - **Language**: SQL

2. **gold_driver_performance** (Per-driver metrics)
   - **Purpose**: Track each driver's performance in real-time
   - **Aggregation Window**: 10-minute tumbling windows
   - **Metrics**:
     ```sql
     - total_deliveries: COUNT(*)
     - unique_restaurants: approx_count_distinct(restaurant_key)
     - total_earnings: SUM(total_amount) * 0.15 (15% commission)
     - avg_delivery_value: AVG(total_amount)
     - completed_deliveries: COUNT(CASE WHEN status = 'delivered')
     - completion_rate: completed_deliveries / total_deliveries
     ```
   - **Dimensions**: driver_key, window_start, window_end
   - **Type**: STREAMING TABLE
   - **Language**: SQL

3. **gold_delivery_time_distribution** (Speed bucketing)
   - **Purpose**: Categorize deliveries by speed
   - **Buckets**:
     ```sql
     - fast: < 30 minutes
     - normal: 30-60 minutes
     - slow: 60-90 minutes
     - very_slow: > 90 minutes
     ```
   - **Aggregation Window**: 10-minute tumbling windows
   - **Metrics**:
     ```sql
     - order_count per bucket
     - percentage per bucket
     - avg_delivery_time per bucket
     ```
   - **Type**: STREAMING TABLE
   - **Language**: SQL

4. **gold_system_health** (Platform-wide KPIs)
   - **Purpose**: Overall system health monitoring
   - **Aggregation Window**: 10-minute tumbling windows
   - **Metrics**:
     ```sql
     - total_orders: COUNT(*)
     - total_revenue: SUM(total_amount)
     - unique_users: approx_count_distinct(user_key)
     - unique_restaurants: approx_count_distinct(restaurant_key)
     - unique_drivers: approx_count_distinct(driver_key)
     - avg_order_value: AVG(total_amount)
     - orders_per_minute: total_orders / 10
     - revenue_per_minute: total_revenue / 10
     ```
   - **Dimensions**: window_start, window_end
   - **Type**: STREAMING TABLE
   - **Language**: SQL

**Key Concepts Demonstrated**:
- ✅ Windowed aggregations (tumbling windows)
- ✅ approx_count_distinct (streaming-compatible)
- ✅ Multiple aggregation perspectives
- ✅ Real-time KPIs
- ✅ CASE expressions for conditional aggregations
- ✅ Time bucketing (CASE for speed categories)

---

## 🎨 Excalidraw Elements

### **1. Title Box**
```
┌─────────────────────────────────────────┐
│  USE CASE 1: REAL-TIME DELIVERY         │
│  MONITORING WITH STREAMING              │
│                                         │
│  Problem: Monitor 10,000+ deliveries/sec│
│           Detect delays in < 3 seconds  │
│  Solution: Streaming pipeline + Sinks   │
└─────────────────────────────────────────┘
```

---

### **2. Data Sources (Top)**
```
┌──────────────────────────────────────────┐
│         📥 DATA SOURCES (Kafka)          │
├──────────────────────────────────────────┤
│                                          │
│  🌊 kafka/orders/                        │
│  ├─ New orders from customers            │
│  ├─ Fields: order_id, restaurant_key,    │
│  │   driver_key, total_amount            │
│  ├─ Volume: ~5,000 events/sec            │
│  └─ Latency: < 100ms                     │
│                                          │
│  🌊 kafka/status/                        │
│  ├─ Delivery status updates              │
│  ├─ Fields: order_identifier, status_id, │
│  │   status_name                         │
│  ├─ Volume: ~5,000 events/sec            │
│  └─ Latency: < 100ms                     │
│                                          │
│  Join Key: order_id = order_identifier   │
│  Processing: Real-time streaming         │
└──────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Two boxes side by side (orders left, status right)
- Use streaming wave icon (🌊)
- Color: Blue (#4A90E2) - represents streaming
- Arrow pointing down to Bronze
- Annotate with "10,000+ events/sec"

---

### **3. Bronze Layer**
```
┌──────────────────────────────────────────┐
│   🟤 BRONZE LAYER (Streaming Ingestion)  │
├──────────────────────────────────────────┤
│                                          │
│  [STREAMING TABLE - Python]              │
│  bronze_orders                           │
│  ├─ Method: Auto Loader (cloudFiles)     │
│  ├─ Features: Schema hints, metadata     │
│  ├─ CDF: Enabled                         │
│  └─ Latency: < 1 second                  │
│                                          │
│  [STREAMING TABLE - SQL]                 │
│  bronze_status                           │
│  ├─ Method: read_files()                 │
│  ├─ Features: Nested extraction          │
│  ├─ CDF: Enabled                         │
│  └─ Latency: < 1 second                  │
│                                          │
│  Type: STREAMING TABLE (Continuous)      │
│  Processing: Event-by-event              │
└──────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Rectangle box
- Color: Brown (#8B5A2B)
- Two sub-boxes inside (one for each table)
- Label: "Python" and "SQL" to show different approaches
- Arrow pointing down to Silver
- Annotate: "Two approaches, same result"

---

### **4. Silver Layer (Stream Processing)**
```
┌──────────────────────────────────────────────────────────────┐
│   🥈 SILVER LAYER (Stream Joins & Filtering)                 │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  [STREAMING TABLE - Stream-to-stream JOIN]                  │
│  silver_order_status                                         │
│  ├─ Join: bronze_orders LEFT JOIN bronze_status             │
│  ├─ Watermark: 10 minutes on both sides                     │
│  ├─ Time Constraint: status within 2 hours of order         │
│  ├─ Data Quality: 3 expectations (expect_or_drop)           │
│  │   • valid_order_id: order_id IS NOT NULL                 │
│  │   • valid_order_date: order_date IS NOT NULL             │
│  │   • valid_total_amount: total_amount > 0                 │
│  └─ Latency: < 2 seconds                                    │
│                                                              │
│  [STREAMING TABLE - Filtered Events]                        │
│  silver_delayed_orders                                       │
│  ├─ Filter: status = 'preparing' AND delay > 30 min         │
│  ├─ Purpose: Critical events for alerting                   │
│  └─ Latency: < 2 seconds                                    │
│                                                              │
│  Key Concepts:                                               │
│  • Watermarking handles late data                           │
│  • Time-bounded join prevents state explosion               │
│  • Expectations drop invalid records automatically          │
└──────────────────────────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Large rectangle box
- Color: Gray/Silver (#C0C0C0)
- Two sub-boxes (stacked vertically)
- Top box: Emphasize "LEFT JOIN" and "Watermark: 10 min"
- Bottom box: Emphasize "Filter: delay > 30 min"
- Two arrows pointing down: one to Sink, one to Gold
- Annotate: "Stream-to-stream JOIN with watermarking"

---

### **5. Sink Layer (External Integration)**
```
┌──────────────────────────────────────────────────────────────┐
│   📤 SINK LAYER (External Kafka Publishing)                  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Step 1: Define Sink                                         │
│  ┌────────────────────────────────────────────────┐         │
│  │ delivery_alerts_sink                           │         │
│  │ ├─ Type: Kafka                                 │         │
│  │ ├─ Topic: delivery-alerts                      │         │
│  │ └─ Guarantee: Exactly-once                     │         │
│  └────────────────────────────────────────────────┘         │
│                                                              │
│  Step 2: Format Messages                                     │
│  ┌────────────────────────────────────────────────┐         │
│  │ sink_kafka_alerts (Staging)                    │         │
│  │ ├─ key: order_id                               │         │
│  │ ├─ value: JSON({order_id, restaurant_key,     │         │
│  │ │         driver_key, delay_minutes, ...})     │         │
│  │ └─ Source: silver_delayed_orders               │         │
│  └────────────────────────────────────────────────┘         │
│                                                              │
│  Step 3: Publish                                             │
│  ┌────────────────────────────────────────────────┐         │
│  │ @dp.append_flow(                               │         │
│  │   target="delivery_alerts_sink",               │         │
│  │   source="sink_kafka_alerts"                   │         │
│  │ )                                              │         │
│  └────────────────────────────────────────────────┘         │
│                                                              │
│  Latency: < 3 seconds (end-to-end)                          │
│  Consumers: Alerting systems, Monitoring tools               │
└──────────────────────────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Rectangle box (separate from main flow, to the right)
- Color: Orange (#F5A623) - represents external integration
- Three steps (stacked vertically)
- Arrow from silver_delayed_orders to sink
- Arrow from sink to external Kafka icon
- Annotate: "Exactly-once delivery to external Kafka"

---

### **6. Gold Layer (Real-Time Analytics)**
```
┌──────────────────────────────────────────────────────────────┐
│   🥇 GOLD LAYER (Real-Time KPIs)                             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  [STREAMING TABLE] gold_restaurant_performance               │
│  ├─ Window: 10-minute tumbling                              │
│  ├─ Metrics: total_orders, unique_drivers, revenue,         │
│  │   avg_order_value, delay_rate                            │
│  ├─ Group By: restaurant_key, window                        │
│  └─ Use: Restaurant dashboard                               │
│                                                              │
│  [STREAMING TABLE] gold_driver_performance                   │
│  ├─ Window: 10-minute tumbling                              │
│  ├─ Metrics: total_deliveries, unique_restaurants,          │
│  │   earnings, completion_rate                              │
│  ├─ Group By: driver_key, window                            │
│  └─ Use: Driver dashboard                                   │
│                                                              │
│  [STREAMING TABLE] gold_delivery_time_distribution           │
│  ├─ Window: 10-minute tumbling                              │
│  ├─ Buckets: fast (<30m), normal (30-60m), slow (60-90m),  │
│  │   very_slow (>90m)                                       │
│  ├─ Metrics: count, percentage per bucket                   │
│  └─ Use: Operations monitoring                              │
│                                                              │
│  [STREAMING TABLE] gold_system_health                        │
│  ├─ Window: 10-minute tumbling                              │
│  ├─ Metrics: total_orders, revenue, unique_users,           │
│  │   restaurants, drivers, orders_per_minute                │
│  ├─ Group By: window only (platform-wide)                   │
│  └─ Use: Executive dashboard                                │
│                                                              │
│  Key: approx_count_distinct (streaming-compatible)           │
│  Update Frequency: Every 10 minutes                          │
└──────────────────────────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Rectangle box
- Color: Gold/Yellow (#FFD700)
- Four sub-boxes (stacked vertically, or 2x2 grid)
- Each labeled "STREAMING TABLE"
- Emphasize "10-minute windows" and "approx_count_distinct"
- Arrows pointing down to Consumers
- Annotate: "Real-time KPIs updated every 10 minutes"

---

### **7. Consumers (Bottom)**
```
┌──────────────────────────────────────────┐
│         📊 CONSUMERS                     │
├──────────────────────────────────────────┤
│                                          │
│  Operations Team:                        │
│  ├─ Real-time delivery monitoring        │
│  ├─ Delayed order alerts                 │
│  └─ System health dashboard              │
│                                          │
│  Restaurant Partners:                    │
│  ├─ Performance metrics                  │
│  └─ Order volume tracking                │
│                                          │
│  Drivers:                                │
│  ├─ Earnings dashboard                   │
│  └─ Completion rate tracking             │
│                                          │
│  External Systems:                       │
│  ├─ Kafka consumers (alerting)           │
│  ├─ Monitoring tools (PagerDuty)         │
│  └─ ML models (demand forecasting)       │
│                                          │
│  Update Latency: < 3 seconds             │
└──────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Rectangle box at bottom
- Color: Blue (#4A90E2)
- Four sections (Operations, Restaurants, Drivers, External)
- Use icons: 📊 📈 🚨 👥
- Arrow from Gold layer
- Arrow from Sink layer to "External Systems"

---

### **8. Key Concepts Box (Side annotation)**
```
┌─────────────────────────────────────────┐
│  KEY CONCEPTS DEMONSTRATED              │
├─────────────────────────────────────────┤
│                                         │
│  ✅ STREAMING TABLES                    │
│     Continuous processing               │
│                                         │
│  ✅ AUTO LOADER (cloudFiles)            │
│     Python approach                     │
│                                         │
│  ✅ read_files()                        │
│     SQL approach                        │
│                                         │
│  ✅ STREAM-TO-STREAM JOIN               │
│     LEFT JOIN with watermarking         │
│                                         │
│  ✅ WATERMARKING                        │
│     Handle late-arriving data           │
│                                         │
│  ✅ TIME-BOUNDED JOIN                   │
│     Prevent state explosion             │
│                                         │
│  ✅ DATA QUALITY (Expectations)         │
│     expect_or_drop decorators           │
│                                         │
│  ✅ SINKS                               │
│     Publish to external Kafka           │
│                                         │
│  ✅ WINDOWED AGGREGATIONS               │
│     10-minute tumbling windows          │
│                                         │
│  ✅ approx_count_distinct               │
│     Streaming-compatible aggregation    │
│                                         │
│  ✅ MEDALLION ARCHITECTURE              │
│     Bronze → Silver → Gold + Sink       │
└─────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Place on right side of diagram
- Color: Light blue background
- Checkmarks (✅) for each concept
- Connect to relevant layers with dotted lines

---

### **9. Data Flow Arrows**

**Arrows to draw**:

1. **Sources → Bronze**: Two parallel arrows
   - Label: "Streaming ingestion (< 1s)"
   - Color: Blue (streaming)

2. **Bronze → Silver**: Two arrows converging
   - Label: "Stream-to-stream JOIN (LEFT JOIN)"
   - Annotate: "Watermark: 10 min, Time constraint: 2 hours"

3. **Silver → Sink**: One arrow to the right
   - Label: "Critical events only (delayed orders)"
   - Color: Orange

4. **Silver → Gold**: One arrow down
   - Label: "Windowed aggregations (10-min windows)"
   - Color: Yellow

5. **Sink → External Kafka**: Arrow to external system
   - Label: "Exactly-once delivery"
   - Dashed line (external boundary)

6. **Gold → Consumers**: Multiple arrows to different consumer boxes
   - Label: "Real-time dashboards (< 3s latency)"

---

### **10. Annotations (Callout boxes)**

**Annotation 1** (near Bronze):
```
💡 Python vs SQL
Same result, different syntax!
- Python: Auto Loader (cloudFiles)
- SQL: read_files()
Choose what fits your team!
```

**Annotation 2** (near Silver JOIN):
```
💡 Stream-to-stream JOIN
Challenge: Both sides are unbounded
Solution: Watermarking + Time constraint
- Watermark: Wait 10 min for late data
- Time constraint: Only join within 2 hours
Result: Bounded state, no OOM!
```

**Annotation 3** (near Expectations):
```
💡 Data Quality
3 expectations drop invalid records:
- valid_order_id (NOT NULL)
- valid_order_date (NOT NULL)
- valid_total_amount (> 0)
No manual filtering needed!
```

**Annotation 4** (near Sink):
```
💡 Why Sinks?
External systems need alerts!
- Kafka: Standard event streaming
- Exactly-once: No duplicates
- Decoupled: Analytics ≠ Alerting
```

**Annotation 5** (near Gold):
```
💡 Streaming Aggregations
Limitations:
- No exact COUNT(DISTINCT) → use approx
- No window functions (OVER clause)
- Must use tumbling/sliding windows
Result: 98% accurate, fixed memory!
```

**Annotation 6** (near end-to-end):
```
💡 End-to-End Latency
Event → Bronze: < 1s
Bronze → Silver: < 1s
Silver → Gold/Sink: < 1s
Total: < 3 seconds! ⚡
```

---

## 📊 Complete Excalidraw Layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  [TITLE BOX]                                                             │
│  USE CASE 1: REAL-TIME DELIVERY MONITORING                               │
│                                                                          │
│  ┌──────────────┐          ┌──────────────┐                             │
│  │  Kafka       │          │  Kafka       │                             │
│  │  orders/     │          │  status/     │                             │
│  └──────┬───────┘          └──────┬───────┘                             │
│         │                         │                                     │
│         └────────────┬────────────┘                                     │
│                      ↓                                                   │
│  ┌─────────────────────────────────────────────┐                        │
│  │  🟤 BRONZE LAYER                            │  [Annotation 1]        │
│  │  ├─ bronze_orders (Python Auto Loader)      │  Python vs SQL        │
│  │  └─ bronze_status (SQL read_files)          │                        │
│  └─────────────────┬───────────────────────────┘                        │
│                    ↓                                                     │
│  ┌─────────────────────────────────────────────┐                        │
│  │  🥈 SILVER LAYER                            │                        │
│  │  ┌─────────────────────────────────┐        │  [Annotation 2]        │
│  │  │ silver_order_status             │        │  Stream JOIN          │
│  │  │ LEFT JOIN + Watermarking        │        │                        │
│  │  │ 3 Expectations (expect_or_drop) │        │  [Annotation 3]        │
│  │  └────────────┬────────────────────┘        │  Data Quality         │
│  │               ↓                             │                        │
│  │  ┌─────────────────────────────────┐        │                        │
│  │  │ silver_delayed_orders           │        │                        │
│  │  │ Filter: delay > 30 min          │        │                        │
│  │  └────────┬──────────────┬─────────┘        │                        │
│  └───────────┼──────────────┼──────────────────┘                        │
│              │              │                                           │
│              │              └──────────────┐                            │
│              │                             ↓                            │
│              │              ┌──────────────────────────┐                │
│              │              │  📤 SINK LAYER           │ [Annotation 4] │
│              │              │  ├─ delivery_alerts_sink │ Why Sinks?    │
│              │              │  ├─ sink_kafka_alerts    │                │
│              │              │  └─ @dp.append_flow      │                │
│              │              └──────────┬───────────────┘                │
│              │                         │                                │
│              │                         ↓                                │
│              │              [External Kafka] 🌐                         │
│              │                                                          │
│              ↓                                                          │
│  ┌─────────────────────────────────────────────┐                        │
│  │  🥇 GOLD LAYER                              │  [Annotation 5]        │
│  │  ├─ gold_restaurant_performance             │  Streaming Aggs       │
│  │  ├─ gold_driver_performance                 │                        │
│  │  ├─ gold_delivery_time_distribution         │  [Annotation 6]        │
│  │  └─ gold_system_health                      │  Latency < 3s         │
│  │                                             │                        │
│  │  Window: 10-minute tumbling                 │                        │
│  │  Function: approx_count_distinct            │                        │
│  └─────────────────┬───────────────────────────┘                        │
│                    ↓                                                     │
│  ┌─────────────────────────────────────────────┐                        │
│  │  📊 CONSUMERS                               │                        │
│  │  ├─ Operations (Monitoring)                 │                        │
│  │  ├─ Restaurants (Performance)               │                        │
│  │  ├─ Drivers (Earnings)                      │                        │
│  │  └─ External Systems (Alerts)               │                        │
│  └─────────────────────────────────────────────┘                        │
│                                                                          │
│  [KEY CONCEPTS BOX - Right side]                                        │
│  ✅ Streaming Tables                                                     │
│  ✅ Auto Loader / read_files                                             │
│  ✅ Stream-to-stream JOIN                                                │
│  ✅ Watermarking                                                         │
│  ✅ Sinks (External Kafka)                                               │
│  ✅ Windowed Aggregations                                                │
│  ✅ approx_count_distinct                                                │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Phase Summary

### **Phase 1: Bronze (10 min)**
- **What**: Ingest streaming data from Kafka
- **How**: Streaming Tables (Python Auto Loader + SQL read_files)
- **Why**: Real-time delivery events need continuous processing
- **Output**: 2 Bronze tables (orders + status)

### **Phase 2: Silver (15 min)**
- **What**: Join streams + Filter critical events
- **How**: Stream-to-stream JOIN with watermarking + Expectations
- **Why**: Unified view of deliveries + Detect delays
- **Output**: 2 Silver tables (unified + delayed)

### **Phase 3: Sink (10 min)**
- **What**: Publish critical events to external Kafka
- **How**: dp.create_sink + dp.append_flow
- **Why**: External systems need real-time alerts
- **Output**: 1 Kafka sink (delivery-alerts topic)

### **Phase 4: Gold (15 min)**
- **What**: Real-time KPIs and analytics
- **How**: Windowed aggregations with approx_count_distinct
- **Why**: Operations, restaurants, drivers need live metrics
- **Output**: 4 Gold tables (restaurant, driver, distribution, system)

---

## 💡 Real-World Production Scenario

**Company**: UberEats (Food Delivery Platform)

**Problem**:
- **Operations** needs to monitor 10,000+ deliveries in real-time
- **Delayed orders** must be detected in < 3 seconds
- **External systems** (PagerDuty, Slack) need instant alerts
- **Restaurants/Drivers** need live performance dashboards

**Solution**:
Build a **Real-Time Streaming Pipeline** that:
1. ✅ Ingests Kafka streams continuously (orders + status)
2. ✅ Joins streams with watermarking (handle late data)
3. ✅ Filters critical events (delayed orders)
4. ✅ Publishes to external Kafka (Sinks)
5. ✅ Generates real-time KPIs (windowed aggregations)

**Business Impact**:
- 📈 **Operations**: 80% faster incident detection (< 3s vs 15s)
- 📈 **Customer Satisfaction**: 25% fewer complaints (proactive alerts)
- 📈 **Restaurant Partners**: 40% better planning (real-time metrics)
- 📈 **Drivers**: 30% higher earnings (optimized routing based on data)

---

## 🚀 Technical Highlights

### **Why This Use Case is Production-Grade**:

1. **Real Problem**: Monitor thousands of real-time deliveries (every delivery platform has this)
2. **Real Solution**: Streaming pipeline with Sinks (industry standard)
3. **Real Complexity**: Stream-to-stream joins, watermarking, late data handling
4. **Real Integration**: External Kafka for alerting (decoupled architecture)

### **What Students Learn**:

- ✅ When to use **Streaming Tables** vs Materialized Views
- ✅ How to **join** two streaming sources (watermarking + time constraints)
- ✅ How to handle **late-arriving data** (watermarking)
- ✅ How to apply **Data Quality** (expectations)
- ✅ How to **publish to external systems** (Sinks)
- ✅ How to create **real-time KPIs** (windowed aggregations)
- ✅ Why use **approx_count_distinct** (streaming limitations)
- ✅ How to design **low-latency pipelines** (< 3s end-to-end)

---

## 📋 File Mapping (Your Actual Implementation)

| Layer | File | Language | Table/Object |
|:------|:-----|:---------|:-------------|
| Bronze | 01-bronze-orders.py | Python | bronze_orders |
| Bronze | 01-bronze-status.sql | SQL | bronze_status |
| Silver | 02-silver-order-status.py | Python | silver_order_status |
| Silver | 02-silver-delayed-orders.py | Python | silver_delayed_orders |
| Sink | 04-sink-layer.py | Python | delivery_alerts_sink + sink_kafka_alerts |
| Gold | 03-gold-restaurant-performance.sql | SQL | gold_restaurant_performance |
| Gold | 03-gold-driver-performance.sql | SQL | gold_driver_performance |
| Gold | 03-gold-delivery-time-distribution.sql | SQL | gold_delivery_time_distribution |
| Gold | 03-gold-system-health.sql | SQL | gold_system_health |

**Total**: 9 files → 8 streaming tables + 1 Kafka sink

---

**This use case perfectly demonstrates Lakeflow's power for real-time streaming scenarios!** 🎓🚀


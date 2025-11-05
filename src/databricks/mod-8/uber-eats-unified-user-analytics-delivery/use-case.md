# USE CASE 2: Unified User Domain with CDC

## 🎯 Business Context

**UberEats** has user data scattered across **two different systems**:

1. **MongoDB** (Operational Database)
   - Stores delivery-related information
   - Updated when users place orders or change delivery preferences
   - Fields: email, delivery_address, city

2. **MSSQL** (User Management System / CRM)
   - Stores personal and professional information
   - Updated when users register or update their profile
   - Fields: first_name, last_name, birthday, job, company_name

**The Problem**:
- **Marketing** needs complete user profiles for segmentation
- **Customer Support** needs unified view to help customers
- **Analytics** needs combined data for user behavior analysis
- **Compliance** (LGPD/GDPR) requires tracking all changes to user data

**The Challenge**:
- Data changes **constantly** in both systems
- Need to **track history** of changes (audit trail)
- Must **unify** at the same grain (user_id level)
- Handle **out-of-order** updates (events arriving late)
- Support **SCD Type 2** (maintain full history)

---

## 🏗️ Solution Architecture

### **Lakeflow Declarative Pipelines + Medallion Architecture + AUTO CDC Flow**

```
┌──────────────────────────────────────────────────────────────────────┐
│                  UNIFIED USER DOMAIN WITH CDC                         │
│                                                                       │
│  Goal: Create a complete, historical user profile from 2 sources     │
└──────────────────────────────────────────────────────────────────────┘

📥 DATA SOURCES (2 systems)
    ↓
🟤 BRONZE LAYER (Raw Ingestion - Batch)
    ├─ bronze_mongodb_users (Materialized View)
    └─ bronze_mssql_users (Materialized View)
    ↓
🥈 SILVER LAYER (Unified Domain with CDC)
    ├─ silver_users_unified (AUTO CDC Flow - SCD Type 1)
    └─ silver_users_history (AUTO CDC Flow - SCD Type 2)
    ↓
🥇 GOLD LAYER (Analytics & Segmentation)
    ├─ gold_user_segments (Materialized View)
    ├─ gold_user_change_audit (Materialized View)
    └─ gold_user_demographics (Materialized View)
    ↓
📊 CONSUMERS (BI Tools, ML Models, Compliance Reports)
```

---

## 📋 Detailed Use Case Plan

### **PHASE 1: Bronze Layer - Raw Ingestion (Batch)**

**Duration**: 10 minutes

**What Happens**:
- Ingest raw data from **MongoDB** and **MSSQL** into Bronze layer
- Use **Materialized Views** (batch processing)
- No transformations, just raw data landing
- Schema inference from JSON files

**Why Materialized Views?**
- User data changes **slowly** (not real-time like orders)
- Batch processing is more **efficient**
- Scheduled updates (e.g., every hour or daily)

**Tables Created**:
1. **bronze_mongodb_users**
   - Source: Azure Blob Storage (`mongodb_users_*.json`)
   - Fields: user_id, email, delivery_address, city, cpf, phone_number, uuid, country, dt_current_timestamp
   - Type: MATERIALIZED VIEW

2. **bronze_mssql_users**
   - Source: Azure Blob Storage (`mssql_users_*.json`)
   - Fields: user_id, first_name, last_name, birthday, job, company_name, cpf, phone_number, uuid, country, dt_current_timestamp
   - Type: MATERIALIZED VIEW

**Key Concepts Demonstrated**:
- ✅ Materialized Views for batch ingestion
- ✅ Multiple data sources (MongoDB + MSSQL)
- ✅ Schema inference from JSON
- ✅ Incremental file detection

---

### **PHASE 2: Silver Layer - Unified Domain with AUTO CDC**

**Duration**: 15 minutes

**What Happens**:
- **Merge** MongoDB and MSSQL data at the **user_id** grain
- Apply **AUTO CDC Flow** with `APPLY CHANGES INTO`
- Create **two tables**:
  - **SCD Type 1**: Current state only (for operations)
  - **SCD Type 2**: Full history with effective dates (for audit/compliance)
- Handle **INSERT**, **UPDATE**, **DELETE** operations automatically
- Handle **out-of-order** events with `SEQUENCE BY` timestamp

**The Unified Schema**:
```
user_id (key)
├─ From MongoDB:
│  ├─ email
│  ├─ delivery_address
│  └─ city
├─ From MSSQL:
│  ├─ first_name
│  ├─ last_name
│  ├─ birthday
│  ├─ job
│  └─ company_name
└─ Common:
   ├─ cpf
   ├─ phone_number
   ├─ uuid
   ├─ country
   └─ dt_current_timestamp (for CDC sequencing)
```

**Tables Created**:

1. **silver_users_staging** (Intermediate)
   - **Purpose**: Combine MongoDB + MSSQL before CDC
   - **Logic**: FULL OUTER JOIN on user_id
   - **Conflict Resolution**: COALESCE(mongodb.field, mssql.field)
   - **Type**: MATERIALIZED VIEW

2. **silver_users_unified** (SCD Type 1)
   - **Purpose**: Current state of each user
   - **CDC Strategy**: UPDATE overwrites previous values
   - **Use Case**: Operational queries (current user info)
   - **Type**: STREAMING TABLE with AUTO CDC Flow
   - **Command**: `APPLY CHANGES INTO ... STORED AS SCD TYPE 1`

3. **silver_users_history** (SCD Type 2)
   - **Purpose**: Full history of all changes
   - **CDC Strategy**: UPDATE closes old record, creates new record
   - **Adds**: `__START_AT`, `__END_AT`, `__CURRENT` columns
   - **Use Case**: Audit trail, compliance, historical analysis
   - **Type**: STREAMING TABLE with AUTO CDC Flow
   - **Command**: `APPLY CHANGES INTO ... STORED AS SCD TYPE 2`

**Key Concepts Demonstrated**:
- ✅ AUTO CDC Flow with `APPLY CHANGES INTO`
- ✅ SCD Type 1 vs Type 2
- ✅ FULL OUTER JOIN to unify sources
- ✅ Conflict resolution with COALESCE
- ✅ SEQUENCE BY for out-of-order handling
- ✅ Automatic INSERT/UPDATE/DELETE handling

---

### **PHASE 3: Gold Layer - Analytics & Segmentation**

**Duration**: 10 minutes

**What Happens**:
- Create **business-ready** analytics tables
- Segment users by demographics, location, behavior
- Track change frequency for compliance
- Aggregate user statistics

**Tables Created**:

1. **gold_user_segments** (Materialized View)
   - **Source**: silver_users_unified (current state)
   - **Purpose**: User segmentation for marketing
   - **Dimensions**: city, age_group, job_category
   - **Metrics**: user_count, avg_orders, avg_spend
   - **Type**: MATERIALIZED VIEW

2. **gold_user_change_audit** (Materialized View)
   - **Source**: silver_users_history (full history)
   - **Purpose**: Compliance and audit reports
   - **Tracks**: email_changes, address_changes, profile_updates
   - **Metrics**: change_count, last_change_date, change_frequency
   - **Type**: MATERIALIZED VIEW

3. **gold_user_demographics** (Materialized View)
   - **Source**: silver_users_unified (current state)
   - **Purpose**: Demographic analysis
   - **Dimensions**: age_group, city, state
   - **Metrics**: user_count, gender_distribution, job_distribution
   - **Type**: MATERIALIZED VIEW

**Key Concepts Demonstrated**:
- ✅ Materialized Views for aggregations
- ✅ Business segmentation
- ✅ Audit trail analysis
- ✅ Demographic analytics

---

## 🎨 Excalidraw Elements

### **1. Title Box**
```
┌─────────────────────────────────────────┐
│  USE CASE 2: UNIFIED USER DOMAIN        │
│  WITH CHANGE DATA CAPTURE (CDC)         │
│                                         │
│  Problem: User data scattered across    │
│           2 systems (MongoDB + MSSQL)   │
│  Solution: Unified domain with history  │
└─────────────────────────────────────────┘
```

---

### **2. Data Sources (Top)**
```
┌──────────────────────────────────────────┐
│         📥 DATA SOURCES                  │
├──────────────────────────────────────────┤
│                                          │
│  🗄️ MongoDB (Operational DB)            │
│  ├─ email, delivery_address, city       │
│  ├─ Updated: When orders placed         │
│  └─ Change Frequency: High              │
│                                          │
│  🗄️ MSSQL (User Management)             │
│  ├─ name, birthday, job, company        │
│  ├─ Updated: Profile changes            │
│  └─ Change Frequency: Low               │
│                                          │
│  Common Key: user_id                    │
│  Common Fields: cpf, phone, uuid        │
└──────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Two boxes side by side (MongoDB left, MSSQL right)
- Use database icon (🗄️)
- Color: Green (#7ED321)
- Arrow pointing down to Bronze

---

### **3. Bronze Layer**
```
┌──────────────────────────────────────────┐
│      🟤 BRONZE LAYER (Batch Ingestion)   │
├──────────────────────────────────────────┤
│                                          │
│  [MATERIALIZED VIEW]                     │
│  bronze_mongodb_users                    │
│  ├─ Source: Azure Blob Storage           │
│  ├─ Format: JSON                         │
│  ├─ Schema: Auto-inferred                │
│  └─ Schedule: Hourly                     │
│                                          │
│  [MATERIALIZED VIEW]                     │
│  bronze_mssql_users                      │
│  ├─ Source: Azure Blob Storage           │
│  ├─ Format: JSON                         │
│  ├─ Schema: Auto-inferred                │
│  └─ Schedule: Hourly                     │
│                                          │
│  Type: MATERIALIZED VIEW (Batch)         │
│  Processing: Incremental file detection  │
└──────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Rectangle box
- Color: Brown (#8B5A2B)
- Two sub-boxes inside (one for each source)
- Label: "MATERIALIZED VIEW" in each
- Arrow pointing down to Silver

---

### **4. Silver Layer (CDC)**
```
┌──────────────────────────────────────────────────────────────┐
│      🥈 SILVER LAYER (Unified Domain with CDC)               │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Step 1: Staging (Combine sources)                          │
│  ┌────────────────────────────────────────────────┐         │
│  │ [MATERIALIZED VIEW]                            │         │
│  │ silver_users_staging                           │         │
│  │ ├─ FULL OUTER JOIN on user_id                 │         │
│  │ ├─ COALESCE(mongodb.field, mssql.field)       │         │
│  │ └─ Unified schema (14 fields)                 │         │
│  └────────────────────────────────────────────────┘         │
│                        ↓                                     │
│  Step 2: Apply CDC (Two strategies)                         │
│  ┌──────────────────────┐  ┌──────────────────────┐         │
│  │ [AUTO CDC - Type 1]  │  │ [AUTO CDC - Type 2]  │         │
│  │ silver_users_unified │  │ silver_users_history │         │
│  │ ├─ Current state     │  │ ├─ Full history      │         │
│  │ ├─ UPDATE overwrites │  │ ├─ UPDATE = close +  │         │
│  │ │                    │  │ │   new record        │         │
│  │ ├─ DELETE removes    │  │ ├─ DELETE = soft     │         │
│  │ └─ Use: Operations   │  │ ├─ Adds: __START_AT, │         │
│  │                      │  │ │   __END_AT, __CURRENT│        │
│  │                      │  │ └─ Use: Audit/Compliance│       │
│  └──────────────────────┘  └──────────────────────┘         │
│                                                              │
│  Command: APPLY CHANGES INTO ... KEYS (user_id)             │
│           SEQUENCE BY dt_current_timestamp                  │
│           STORED AS SCD TYPE 1 / TYPE 2                     │
└──────────────────────────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Large rectangle box
- Color: Gray/Silver (#C0C0C0)
- Three sub-sections:
  1. Top: Staging box (single)
  2. Bottom: Two boxes side by side (Type 1 left, Type 2 right)
- Arrow from staging to both CDC boxes
- Label "AUTO CDC FLOW" prominently
- Arrows pointing down to Gold

---

### **5. Gold Layer**
```
┌──────────────────────────────────────────────────────────────┐
│      🥇 GOLD LAYER (Analytics & Segmentation)                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  [MATERIALIZED VIEW] gold_user_segments                      │
│  ├─ Source: silver_users_unified (current)                   │
│  ├─ Dimensions: city, age_group, job_category               │
│  ├─ Metrics: user_count, avg_orders                         │
│  └─ Use: Marketing segmentation                             │
│                                                              │
│  [MATERIALIZED VIEW] gold_user_change_audit                  │
│  ├─ Source: silver_users_history (history)                   │
│  ├─ Tracks: email_changes, address_changes                  │
│  ├─ Metrics: change_count, change_frequency                 │
│  └─ Use: Compliance (LGPD/GDPR)                             │
│                                                              │
│  [MATERIALIZED VIEW] gold_user_demographics                  │
│  ├─ Source: silver_users_unified (current)                   │
│  ├─ Dimensions: age_group, city, state                      │
│  ├─ Metrics: user_count, distribution                       │
│  └─ Use: Business intelligence                              │
│                                                              │
│  Type: MATERIALIZED VIEW (Batch aggregations)                │
└──────────────────────────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Rectangle box
- Color: Gold/Yellow (#FFD700)
- Three sub-boxes (stacked vertically)
- Each labeled "MATERIALIZED VIEW"
- Arrows pointing down to Consumers

---

### **6. Consumers (Bottom)**
```
┌──────────────────────────────────────────┐
│         📊 CONSUMERS                     │
├──────────────────────────────────────────┤
│                                          │
│  Marketing Team:                         │
│  ├─ User segmentation                    │
│  └─ Campaign targeting                   │
│                                          │
│  Customer Support:                       │
│  ├─ Unified user view                    │
│  └─ 360° customer profile                │
│                                          │
│  Compliance Team:                        │
│  ├─ Audit trail (LGPD/GDPR)              │
│  └─ Change tracking                      │
│                                          │
│  Analytics Team:                         │
│  ├─ User behavior analysis               │
│  └─ Demographic insights                 │
└──────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Rectangle box at bottom
- Color: Blue (#4A90E2)
- Four sections (Marketing, Support, Compliance, Analytics)
- Use icons: 📊 📈 🔍 👥

---

### **7. Key Concepts Box (Side annotation)**
```
┌─────────────────────────────────────────┐
│  KEY CONCEPTS DEMONSTRATED              │
├─────────────────────────────────────────┤
│                                         │
│  ✅ MATERIALIZED VIEWS                  │
│     Bronze & Gold layers                │
│                                         │
│  ✅ AUTO CDC FLOW                       │
│     APPLY CHANGES INTO                  │
│                                         │
│  ✅ SCD TYPE 1                          │
│     Current state only                  │
│                                         │
│  ✅ SCD TYPE 2                          │
│     Full history with dates             │
│                                         │
│  ✅ FULL OUTER JOIN                     │
│     Unify two sources                   │
│                                         │
│  ✅ SEQUENCE BY                         │
│     Out-of-order handling               │
│                                         │
│  ✅ MEDALLION ARCHITECTURE              │
│     Bronze → Silver → Gold              │
│                                         │
│  ✅ BATCH PROCESSING                    │
│     Efficient for slowly changing data  │
└─────────────────────────────────────────┘
```

**Excalidraw Instructions**:
- Place on right side of diagram
- Color: Light blue background
- Checkmarks (✅) for each concept
- Connect to relevant layers with dotted lines

---

### **8. Data Flow Arrows**

**Arrows to draw**:
1. **Sources → Bronze**: Two arrows (one from each source)
   - Label: "Batch ingestion (hourly)"
   
2. **Bronze → Silver Staging**: Two arrows converging
   - Label: "FULL OUTER JOIN on user_id"
   
3. **Staging → CDC Tables**: One arrow splitting into two
   - Label: "APPLY CHANGES INTO"
   
4. **Silver → Gold**: Two arrows (one from each CDC table)
   - Label: "Aggregations & Analytics"
   
5. **Gold → Consumers**: Multiple arrows to different consumer boxes
   - Label: "BI Tools, Reports, Dashboards"

---

### **9. Annotations (Callout boxes)**

**Annotation 1** (near Bronze):
```
💡 Why Materialized Views?
User data changes slowly (not real-time)
Batch processing is more efficient
Scheduled updates (hourly/daily)
```

**Annotation 2** (near Silver CDC):
```
💡 SCD Type 1 vs Type 2
Type 1: Only current state (operations)
Type 2: Full history (audit/compliance)
Both created from same source!
```

**Annotation 3** (near APPLY CHANGES):
```
💡 AUTO CDC Magic
Handles INSERT/UPDATE/DELETE automatically
Out-of-order events? SEQUENCE BY handles it
No manual merge logic needed!
```

**Annotation 4** (near Gold):
```
💡 Business Value
Marketing: Segment users precisely
Support: 360° customer view
Compliance: Complete audit trail
Analytics: Historical insights
```

---

## 📊 Complete Excalidraw Layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  [TITLE BOX]                                                             │
│  USE CASE 2: UNIFIED USER DOMAIN WITH CDC                                │
│                                                                          │
│  ┌──────────────┐          ┌──────────────┐                             │
│  │  MongoDB     │          │  MSSQL       │                             │
│  │  (Operational│          │  (User Mgmt) │                             │
│  └──────┬───────┘          └──────┬───────┘                             │
│         │                         │                                     │
│         └────────────┬────────────┘                                     │
│                      ↓                                                   │
│  ┌─────────────────────────────────────────────┐                        │
│  │  🟤 BRONZE LAYER                            │  [Annotation 1]        │
│  │  ├─ bronze_mongodb_users (MAT VIEW)         │  Why Materialized     │
│  │  └─ bronze_mssql_users (MAT VIEW)           │  Views?               │
│  └─────────────────┬───────────────────────────┘                        │
│                    ↓                                                     │
│  ┌─────────────────────────────────────────────┐                        │
│  │  🥈 SILVER LAYER                            │                        │
│  │  ┌─────────────────────────────────┐        │                        │
│  │  │ silver_users_staging (MAT VIEW) │        │  [Annotation 2]        │
│  │  │ FULL OUTER JOIN                 │        │  SCD Type 1 vs 2      │
│  │  └────────────┬────────────────────┘        │                        │
│  │               ↓                             │  [Annotation 3]        │
│  │  ┌──────────────────┐  ┌──────────────────┐ │  AUTO CDC Magic       │
│  │  │ silver_users_    │  │ silver_users_    │ │                        │
│  │  │ unified (Type 1) │  │ history (Type 2) │ │                        │
│  │  │ AUTO CDC FLOW    │  │ AUTO CDC FLOW    │ │                        │
│  │  └────────┬─────────┘  └────────┬─────────┘ │                        │
│  └───────────┼────────────────────┼─────────────┘                        │
│              │                    │                                     │
│              └──────────┬─────────┘                                     │
│                         ↓                                                │
│  ┌─────────────────────────────────────────────┐                        │
│  │  🥇 GOLD LAYER                              │  [Annotation 4]        │
│  │  ├─ gold_user_segments (MAT VIEW)           │  Business Value       │
│  │  ├─ gold_user_change_audit (MAT VIEW)       │                        │
│  │  └─ gold_user_demographics (MAT VIEW)       │                        │
│  └─────────────────┬───────────────────────────┘                        │
│                    ↓                                                     │
│  ┌─────────────────────────────────────────────┐                        │
│  │  📊 CONSUMERS                               │                        │
│  │  ├─ Marketing (Segmentation)                │                        │
│  │  ├─ Support (360° view)                     │                        │
│  │  ├─ Compliance (Audit trail)                │                        │
│  │  └─ Analytics (Insights)                    │                        │
│  └─────────────────────────────────────────────┘                        │
│                                                                          │
│  [KEY CONCEPTS BOX - Right side]                                        │
│  ✅ Materialized Views                                                   │
│  ✅ AUTO CDC Flow                                                        │
│  ✅ SCD Type 1 & 2                                                       │
│  ✅ FULL OUTER JOIN                                                      │
│  ✅ Medallion Architecture                                               │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Phase Summary

### **Phase 1: Bronze (10 min)**
- **What**: Ingest raw data from MongoDB and MSSQL
- **How**: Materialized Views with batch processing
- **Why**: User data changes slowly, batch is efficient
- **Output**: 2 Bronze tables (raw data)

### **Phase 2: Silver (15 min)**
- **What**: Unify two sources + Apply CDC
- **How**: FULL OUTER JOIN + APPLY CHANGES INTO
- **Why**: Create complete user profile with history
- **Output**: 3 Silver tables (staging + 2 CDC tables)

### **Phase 3: Gold (10 min)**
- **What**: Create business analytics tables
- **How**: Materialized Views with aggregations
- **Why**: Enable marketing, compliance, analytics use cases
- **Output**: 3 Gold tables (segments, audit, demographics)

---

## 💡 Real-World Production Scenario

**Company**: UberEats (Food Delivery Platform)

**Problem**:
- **Marketing** can't segment users effectively (missing delivery addresses)
- **Support** can't see complete user profile (data in 2 systems)
- **Compliance** (LGPD/GDPR) requires tracking all user data changes
- **Analytics** can't analyze user behavior (incomplete data)

**Solution**:
Create a **Unified User Domain** that:
1. ✅ Combines MongoDB (operational) + MSSQL (user management)
2. ✅ Maintains **current state** (SCD Type 1) for operations
3. ✅ Maintains **full history** (SCD Type 2) for compliance
4. ✅ Handles **out-of-order** updates automatically
5. ✅ Provides **business-ready** analytics tables

**Business Impact**:
- 📈 Marketing: 35% better campaign targeting (complete profiles)
- 📈 Support: 50% faster resolution (360° customer view)
- 📈 Compliance: 100% audit trail (LGPD/GDPR compliant)
- 📈 Analytics: 3x more insights (historical analysis enabled)

---

## 🚀 Technical Highlights

### **Why This Use Case is Production-Grade**:

1. **Real Problem**: Data scattered across systems (common in enterprises)
2. **Real Solution**: Unified domain with CDC (industry best practice)
3. **Real Compliance**: LGPD/GDPR audit trail (legal requirement)
4. **Real Efficiency**: Batch processing for slowly changing data (cost-effective)

### **What Students Learn**:

- ✅ When to use **Materialized Views** vs Streaming Tables
- ✅ How to **unify** data from multiple sources (FULL OUTER JOIN)
- ✅ How to apply **AUTO CDC Flow** (APPLY CHANGES INTO)
- ✅ Difference between **SCD Type 1** and **Type 2**
- ✅ How to handle **out-of-order** events (SEQUENCE BY)
- ✅ How to design **business-ready** analytics tables
- ✅ How to meet **compliance** requirements (audit trail)

---

**This use case perfectly demonstrates Lakeflow's power for batch CDC scenarios!** 🎓


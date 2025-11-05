# Auto CDC API Demo - Simplified Pipeline

**Purpose:** Demonstrates Databricks Auto CDC API capabilities with minimal configuration.

**Official Documentation:** https://docs.databricks.com/aws/en/ldp/cdc?language=Python

---

## Architecture

```
MongoDB CDC Events ──┐
                     ├─→ [BRONZE] ─→ [SILVER CDC] ─→ [GOLD Analytics]
MSSQL CDC Events ────┘   Streaming    apply_changes()   Live Tables
                         Tables       Auto CDC API
```

---

## Pipeline Files

### Bronze Layer (Raw CDC Ingestion)
- **01-bronze-mongodb-users.sql** - MongoDB CDC events via Auto Loader
- **01-bronze-mssql-users.sql** - MSSQL CDC events via Auto Loader

### Silver Layer (Auto CDC Processing)
- **02-silver-users-cdc.py** - Auto CDC with `apply_changes()`
  - Creates: `silver_users_current` (SCD Type 1)
  - Creates: `silver_users_history` (SCD Type 2)

### Gold Layer (Analytics)
- **03-gold-user-analytics.sql** - Business analytics on CDC data
  - Demographics by city
  - Change audit trail

---

## Auto CDC API Key Features

### 1. Automatic Operation Handling
```python
dlt.apply_changes(
    target="silver_users_current",
    source="silver_users_staging",  # Contains 'operation' column
    keys=["cpf"],
    apply_as_deletes="operation = 'DELETE'"
)
```
**What it does:**
- Reads `operation` column automatically (INSERT/UPDATE/DELETE)
- No manual logic needed for CDC operations
- Handles out-of-order events with `sequence_by`

### 2. SCD Type 1 - Current State Only
```python
stored_as_scd_type=1
```
**Behavior:**
- One row per key (`cpf`)
- UPDATE overwrites existing record
- DELETE removes record
- **Use case:** Operational queries, real-time dashboards

### 3. SCD Type 2 - Full History
```python
stored_as_scd_type=2,
track_history_column_list=["email", "city", "first_name"]
```
**Behavior:**
- Multiple rows per key (version history)
- UPDATE creates new version, closes old version
- DELETE soft-deletes by setting `__END_AT`
- Adds: `__START_AT`, `__END_AT`, `__CURRENT` columns
- **Use case:** Audit trails, LGPD/GDPR compliance

### 4. Out-of-Order Event Handling
```python
sequence_by=col("sequenceNum")
```
**What it does:**
- Events processed in `sequenceNum` order (not arrival order)
- Prevents data inconsistency from network delays
- Critical for distributed CDC streams

---

## Comparison: Simplified vs Full Pipeline

| Feature | Simplified (`stream-cdc-simple`) | Full (`stream-cdc`) |
|---------|----------------------------------|---------------------|
| **Files** | 4 files | 8 files |
| **Bronze Tables** | 2 (MongoDB + MSSQL) | 2 (MongoDB + MSSQL) |
| **Silver Tables** | 3 (staging + 2 CDC) | 5 (staging + staging_latest + 2 CDC + monitoring) |
| **Gold Tables** | 2 (demographics + audit) | 3 (demographics + segments + audit) |
| **Documentation** | Inline comments only | Extensive inline + production notes |
| **Monitoring** | None | Streaming metrics table |
| **Complexity** | **Minimal** - Demo focus | **Production-ready** - Full features |
| **Purpose** | Learn Auto CDC API | Production deployment |

---

## Quick Start

### Option 1: Databricks CLI
```bash
databricks bundle deploy --target dev
databricks bundle run auto_cdc_demo
```

### Option 2: Databricks UI
1. Create new Delta Live Tables pipeline
2. Add files:
   - `01-bronze-mongodb-users.sql`
   - `01-bronze-mssql-users.sql`
   - `02-silver-users-cdc.py`
   - `03-gold-user-analytics.sql`
3. Configure:
   - **Serverless:** Enabled
   - **Continuous:** Enabled (for streaming)
   - **Target:** `main.uber_eats_auto_cdc`
4. Click "Start"

---

## Query Examples

### Current User State (SCD Type 1)
```sql
SELECT * FROM main.uber_eats_auto_cdc.silver_users_current
WHERE cpf = '12345678900';
```

### Current Version (SCD Type 2)
```sql
SELECT * FROM main.uber_eats_auto_cdc.silver_users_history
WHERE cpf = '12345678900' AND __END_AT IS NULL;
```

### All Versions (SCD Type 2)
```sql
SELECT
  cpf,
  email,
  city,
  __START_AT,
  __END_AT,
  CASE WHEN __END_AT IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END AS version_type
FROM main.uber_eats_auto_cdc.silver_users_history
WHERE cpf = '12345678900'
ORDER BY __START_AT DESC;
```

### Version at Specific Time
```sql
SELECT * FROM main.uber_eats_auto_cdc.silver_users_history
WHERE cpf = '12345678900'
  AND __START_AT <= '2025-01-01'
  AND (__END_AT > '2025-01-01' OR __END_AT IS NULL);
```

---

## Auto CDC API Benefits

✅ **Minimal Code:** No manual INSERT/UPDATE/DELETE logic
✅ **Out-of-Order Handling:** Automatic with `sequence_by`
✅ **SCD Patterns:** Built-in Type 1 and Type 2 support
✅ **Production-Ready:** Handles edge cases automatically
✅ **Streaming Native:** Works with continuous data

---

## Learning Path

1. **Start here:** Study simplified pipeline (4 files)
2. **Understand:** Auto CDC API key concepts
3. **Practice:** Run queries on SCD Type 1 and Type 2
4. **Advanced:** Explore full pipeline (`stream-cdc/`) for production patterns

---

## Next Steps

- [ ] Deploy simplified pipeline to Databricks
- [ ] Test INSERT/UPDATE/DELETE operations
- [ ] Query SCD Type 1 and Type 2 tables
- [ ] Compare simplified vs full pipeline
- [ ] Review official docs: https://docs.databricks.com/aws/en/ldp/cdc?language=Python

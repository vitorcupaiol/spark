"""
Apache Iceberg Demo 7: Branching, Tagging & WAP
===============================================

This demo covers:
- Apache Iceberg: CREATE BRANCH
- Apache Iceberg: CREATE TAG
- Apache Iceberg: Writing to Branches
- Apache Iceberg: Branch Retention Policies
- Apache Iceberg: WAP (Write-Audit-Publish)

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-13.py
"""

import base64
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def spark_session():
    """Create Spark Session with Apache Iceberg and MinIO support"""

    encoded_access_key = "bWluaW9sYWtl"
    encoded_secret_key = "TGFrRTE0MjUzNkBA"
    access_key = base64.b64decode(encoded_access_key).decode("utf-8")
    secret_key = base64.b64decode(encoded_secret_key).decode("utf-8")

    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3a://owshq-catalog/warehouse") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Apache Iceberg Spark Session Created Successfully!")
    print(f"🚀 Spark Version: {spark.version}")

    return spark


def setup_namespace(spark):
    """Setup namespace for demo"""

    print("\n=== Setting Up Demo Namespace ===")

    # TODO create namespace
    print("📁 creating namespace...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.ubereats")

    # TODO set catalog context
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats")

    print("✅ namespace ready!")


def create_branch(spark):
    """Demonstrate CREATE BRANCH

    Main Branch (orders):
    ├── Snapshot 1: Initial data
    ├── Snapshot 2: Additional records
    └── Snapshot 3: Current state

    Development Branch (orders.branch_development):
    ├── Snapshot 1: Same as main (shared)
    ├── Snapshot 2: Same as main (shared)
    ├── Snapshot 3: Same as main (shared)
    └── Snapshot 4: Development changes (isolated)

    Feature Branch (orders.branch_feature):
    ├── Snapshot 1: Same as main (shared)
    ├── Snapshot 2: Same as main (shared)
    ├── Snapshot 3: Same as main (shared)
    └── Snapshot 5: Feature changes (isolated)
    """

    print("\n=== Apache Iceberg: CREATE BRANCH ===")

    table_fq = "hadoop_catalog.ubereats.orders"

    # TODO create base table
    print("🏗️ creating base table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  user_id INT,
                  amount DOUBLE,
                  status STRING
              ) USING iceberg
              """)

    # TODO insert initial data
    print("💾 inserting initial data...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 1001, 25.50, 'completed'),
              ('ORD-002', 1002, 18.75, 'pending')
              """)

    # TODO create branches
    print("🌿 creating development branch...")
    spark.sql(f"ALTER TABLE {table_fq} CREATE BRANCH development")

    print("🌿 creating feature branch...")
    spark.sql(f"ALTER TABLE {table_fq} CREATE BRANCH feature")

    # TODO show branches
    print("🔍 showing branches...")
    spark.sql(f"SELECT name, type FROM {table_fq}.refs").show()

    print("✅ branches created!")


def create_tag(spark):
    """Demonstrate CREATE TAG"""

    print("\n=== Apache Iceberg: CREATE TAG ===")

    table_fq = "hadoop_catalog.ubereats.orders"

    # TODO create tag
    print("🏷️ creating release tag...")
    spark.sql(f"ALTER TABLE {table_fq} CREATE TAG v1_0")

    # TODO add more data
    print("💾 adding more data...")
    spark.sql(f"INSERT INTO {table_fq} VALUES ('ORD-003', 1003, 32.00, 'processing')")

    # TODO create another tag
    print("🏷️ creating v1.1 tag...")
    spark.sql(f"ALTER TABLE {table_fq} CREATE TAG v1_1")

    # TODO show all refs
    print("🔍 showing all refs...")
    spark.sql(f"SELECT name, type FROM {table_fq}.refs ORDER BY type, name").show()

    print("✅ tags created!")


def writing_to_branches(spark):
    """Demonstrate Writing to Branches"""

    print("\n=== Apache Iceberg: Writing to Branches ===")

    table_fq = "hadoop_catalog.ubereats.orders"

    # TODO write to development branch
    print("💾 writing to development branch...")
    spark.sql(f"""
              INSERT INTO {table_fq}.branch_development VALUES
              ('ORD-DEV-001', 2001, 35.75, 'testing')
              """)

    # TODO write to feature branch
    print("💾 writing to feature branch...")
    spark.sql(f"""
              INSERT INTO {table_fq}.branch_feature VALUES
              ('ORD-FEAT-001', 3001, 15.50, 'discounted')
              """)

    # TODO compare counts
    print("🔍 comparing branch data...")
    print("   📊 Main branch:")
    spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}").show()

    print("   📊 Development branch:")
    spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}.branch_development").show()

    print("   📊 Feature branch:")
    spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}.branch_feature").show()

    print("✅ branch writing demonstrated!")


def branch_retention_policies(spark):
    """
    Demonstrate branch retention policies in Apache Iceberg (Spark 3.5, Iceberg 1.9.1).
    - Drops the branch if it exists, then creates it with a retention policy.
    - Lists branch retention policies using supported columns.
    """

    print("\n=== Apache Iceberg: Branch Retention Policies ===")

    table_fq = "hadoop_catalog.ubereats.orders"
    branch_name = "temp"
    retention_days = 7

    # Drop the branch if it exists
    print(f"🧹 Dropping branch '{branch_name}' if it exists...")
    try:
        spark.sql(f"ALTER TABLE {table_fq} DROP BRANCH {branch_name}")
        print(f"✅ Branch '{branch_name}' dropped.")
    except Exception as ex:
        if "not found" in str(ex).lower() or "does not exist" in str(ex).lower():
            print(f"ℹ️ Branch '{branch_name}' does not exist, skipping drop.")
        else:
            print(f"⚠️ Unexpected error while dropping branch: {ex}")

    # Create the branch with retention
    print(f"🌿 Creating branch '{branch_name}' with {retention_days} day retention...")
    spark.sql(f"ALTER TABLE {table_fq} CREATE BRANCH {branch_name} RETAIN {retention_days} DAYS")

    # Show all branch retention policies using supported columns
    print("🔍 Showing branch retention policies...")
    spark.sql(f"""
        SELECT name, type, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms
        FROM {table_fq}.refs
        WHERE type = 'BRANCH'
    """).show(truncate=False)

    print("✅ Branch retention policies demonstration complete!")



def write_audit_publish(spark):
    """Demonstrate WAP (Write-Audit-Publish)

    WAP Pattern Overview:
    Write Phase:   Write data to staging branch
        ↓
    Audit Phase:   Validate data quality and business rules
        ↓
    Publish Phase: Promote validated data to production

    WAP Implementation Architecture:
    Production Table (orders):
    ├── Main Branch: Production data (validated, stable)
    └── Staging Branch: New data (unvalidated, testing)

    WAP Workflow:
    1. Write → staging branch (isolated)
    2. Audit → data quality checks on staging
    3. Publish → merge staging to main (if valid)
    """

    print("\n=== Apache Iceberg: WAP (Write-Audit-Publish) ===")

    table_fq = "hadoop_catalog.ubereats.orders_wap"

    # TODO create WAP table
    print("🏗️ creating WAP table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  amount DOUBLE,
                  status STRING
              ) USING iceberg
              """)

    # TODO insert production data
    print("💾 inserting production data...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-PROD-001', 25.50, 'completed'),
              ('ORD-PROD-002', 18.75, 'pending')
              """)

    # TODO WRITE phase: create staging branch
    print("✍️ WAP WRITE: creating staging branch...")
    spark.sql(f"ALTER TABLE {table_fq} CREATE BRANCH staging")

    print("✍️ WAP WRITE: writing to staging...")
    spark.sql(f"""
              INSERT INTO {table_fq}.branch_staging VALUES
              ('ORD-STAGE-001', 45.90, 'pending'),
              ('ORD-STAGE-002', -5.00, 'invalid')
              """)

    # TODO AUDIT phase: validate data
    print("🔍 WAP AUDIT: validating data...")

    negative_count = spark.sql(f"""
                              SELECT COUNT(*) as count 
                              FROM {table_fq}.branch_staging 
                              WHERE amount < 0
                              """).collect()[0]['count']

    print(f"   ❌ Negative amounts found: {negative_count}")
    is_valid = negative_count == 0
    print(f"   ✅ Validation: {'PASSED' if is_valid else 'FAILED'}")

    # TODO PUBLISH phase: conditional merge
    if is_valid:
        print("📤 WAP PUBLISH: publishing to production...")
        spark.sql(f"""
                  INSERT INTO {table_fq}
                  SELECT * FROM {table_fq}.branch_staging WHERE amount > 0
                  """)
        print("   ✅ Data published successfully!")
    else:
        print("   ❌ Data NOT published - validation failed")

    # TODO show final state
    print("🔍 final production data...")
    spark.sql(f"SELECT * FROM {table_fq} ORDER BY order_id").show()

    # TODO cleanup
    print("🧹 cleaning up staging branch...")
    spark.sql(f"ALTER TABLE {table_fq} DROP BRANCH staging")

    print("✅ WAP demonstrated!")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables
        tables = [
            'hadoop_catalog.ubereats.orders',
            'hadoop_catalog.ubereats.orders_wap'
        ]

        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")

        # TODO drop namespace
        spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.ubereats CASCADE")

        print("✅ demo resources cleaned up successfully!")

    except Exception as e:
        print(f"⚠️ cleanup warning: {e}")


def main():
    """Main demo execution"""

    print("🚀 Starting Apache Iceberg Demo 7: Branching, Tagging & WAP")
    print("=" * 70)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_namespace(spark)
        create_branch(spark)
        create_tag(spark)
        writing_to_branches(spark)
        branch_retention_policies(spark)
        write_audit_publish(spark)

        print("\n" + "=" * 70)
        print("🎉 Demo 7 completed successfully!")
        print("📚 Key concepts covered:")
        print("   ✓ Creating branches for development")
        print("   ✓ Creating tags for versioning")
        print("   ✓ Writing to specific branches")
        print("   ✓ Branch retention policies")
        print("   ✓ Write-Audit-Publish pattern")

        print("\n🔗 What's Next:")
        print("   → Demo 8: Performance & Maintenance")

    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # TODO cleanup
        # cleanup_resources(spark)
        spark.stop()
        print("🔒 Spark session stopped")


if __name__ == "__main__":
    main()

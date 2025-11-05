"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-20-postgres-integration.py

-------------------
-------------------
JDBC & Partitioning
column = "total_deliveries"
lowerBound = 0
upperBound = 1000
numPartitions = 4

SELECT * FROM drivers WHERE total_deliveries >= 0 AND total_deliveries < 250
SELECT * FROM drivers WHERE total_deliveries >= 250 AND total_deliveries < 500
SELECT * FROM drivers WHERE total_deliveries >= 500 AND total_deliveries < 750
SELECT * FROM drivers WHERE total_deliveries >= 750 AND total_deliveries <= 1000
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import col, round
from pyspark.sql import Row

spark = SparkSession.builder \
    .getOrCreate()

# TODO configs
spark.sparkContext.setLogLevel("ERROR")

# TODO configure PostgreSQL connection
pg_host = "159.203.159.29"
pg_port = "5432"
pg_database = "postgres"
pg_user = "postgres"
pg_password = "6e8e5979-25c5-44e2-ad76-7a4e8ee68c6f"

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

connection_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# TODO test PostgreSQL connection
try:
    test_df = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT 1 as test) AS test",
        properties=connection_properties
    )
    test_df.show()
    print("PostgreSQL connection successfully established!")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {str(e)}")


# TODO set schema
drivers_schema = StructType([
    StructField("license_plate", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("last_login", DateType(), True),
    StructField("email", StringType(), True),
    StructField("average_rating", FloatType(), True),
    StructField("name", StringType(), True),
    StructField("total_earnings", FloatType(), True),
    StructField("status", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("driver_id", StringType(), False),
    StructField("total_deliveries", IntegerType(), True),
    StructField("vehicle_type", StringType(), True)
])

print("Schema of the 'drivers' table:")
for field in drivers_schema:
    print(f"  - {field.name}: {field.dataType}")

# TODO 5 = complex queries
drivers_data = [
    ("ABC1234", "(11) 98765-4321", "2023-01-10", "carlos.silva@email.com", 4.8, "Carlos Silva",
     5200.50, "active", "2022-03-15", "DRV-001", 342, "Motorcycle"),
    ("DEF5678", "(11) 91234-5678", "2023-01-12", "maria.santos@email.com", 4.9, "Maria Santos",
     6150.75, "active", "2022-02-20", "DRV-002", 415, "Car"),
    ("GHI9012", "(21) 98765-4321", "2023-01-08", "joao.oliveira@email.com", 4.7, "João Oliveira",
     4850.25, "inactive", "2022-04-05", "DRV-003", 278, "Motorcycle"),
    ("JKL3456", "(21) 91234-5678", "2023-01-11", "ana.pereira@email.com", 4.6, "Ana Pereira",
     5600.00, "active", "2022-05-10", "DRV-004", 320, "Bicycle"),
    ("MNO7890", "(31) 98765-4321", "2023-01-09", "pedro.souza@email.com", 4.8, "Pedro Souza",
     5950.50, "active", "2022-01-25", "DRV-005", 380, "Car")
]

drivers_df = spark.createDataFrame([
    Row(license_plate=d[0], phone_number=d[1], last_login=d[2], email=d[3],
        average_rating=d[4], name=d[5], total_earnings=d[6], status=d[7],
        registration_date=d[8], driver_id=d[9], total_deliveries=d[10], vehicle_type=d[11])
    for d in drivers_data
])


query = """
    (SELECT 
        d.name, 
        d.vehicle_type, 
        d.average_rating,
        d.total_deliveries,
        d.total_earnings,
        (d.total_earnings / NULLIF(d.total_deliveries, 0)) as avg_earning_per_delivery
    FROM 
        drivers d
    WHERE 
        d.status = 'active'
    ORDER BY 
        avg_earning_per_delivery DESC
    LIMIT 10) AS top_drivers
"""

try:
    top_earning_drivers_df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=connection_properties
    )

    print("Top drivers by average earning per delivery:")
    top_earning_drivers_df.show()
except Exception as e:
    print(f"Error executing complex query: {str(e)}")
    print("Demonstrating with local data...")

    top_earning_drivers_df = (drivers_df
        .filter(col("status") == "active")
        .withColumn("avg_earning_per_delivery", round(col("total_earnings") / col("total_deliveries"), 2))
        .select("name", "vehicle_type", "average_rating", "total_deliveries", "total_earnings", "avg_earning_per_delivery")
        .orderBy(col("avg_earning_per_delivery").desc())
        .limit(10))

    print("Top drivers by average earning per delivery (calculated locally):")
    top_earning_drivers_df.show()


# TODO 6 = parallel reading
def read_table_in_parallel(table_name, partition_column, lower_bound, upper_bound, num_partitions):
    """
    Read a table in parallel using partitioning

    Args:
        table_name: PostgreSQL table name
        partition_column: Column to use for partitioning (must be numeric)
        lower_bound: Minimum value of partition column
        upper_bound: Maximum value of partition column
        num_partitions: Number of partitions to create

    Returns:
        DataFrame with the table data
    """
    return spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=connection_properties,
        column=partition_column,
        lowerBound=lower_bound,
        upperBound=upper_bound,
        numPartitions=num_partitions
    )


try:
    bounds_df = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT MIN(total_deliveries) as min_deliveries, MAX(total_deliveries) as max_deliveries FROM drivers) AS bounds",
        properties=connection_properties
    )

    min_deliveries = bounds_df.first()["min_deliveries"]
    max_deliveries = bounds_df.first()["max_deliveries"]

    if min_deliveries is not None and max_deliveries is not None:
        drivers_parallel_df = read_table_in_parallel(
            table_name="drivers",
            partition_column="total_deliveries",
            lower_bound=min_deliveries,
            upper_bound=max_deliveries + 1,
            num_partitions=4
        )

        print(f"Read {drivers_parallel_df.count()} records in parallel")
        print(f"Number of partitions: {drivers_parallel_df.rdd.getNumPartitions()}")
        drivers_parallel_df.show(5)
    else:
        print("Could not determine partition bounds")
except Exception as e:
    print(f"Error reading in parallel: {str(e)}")
    print("Continuing with the next examples...")


spark.stop()

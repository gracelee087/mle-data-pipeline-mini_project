import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True, help='Input path for Green Taxi data')
parser.add_argument('--output', required=True, help='Output path for revenue report')

args = parser.parse_args()

input_green = args.input_green
output = args.output

# Create Spark Session (without .master for Dataproc)
spark = SparkSession.builder \
    .appName('Green Taxi Revenue Report') \
    .getOrCreate()

print(f"Reading Green Taxi data from {input_green}...")
df_green = spark.read.parquet(input_green)

print(f"Total records: {df_green.count()}")

# Rename columns to match common naming
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

# Common columns
common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type'
]

# Create temporary view for SQL queries
df_green.createOrReplaceTempView('trips_data')

print("Calculating daily revenue by location...")

# Calculate daily revenue with detailed breakdown
df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('day', pickup_datetime) AS revenue_day,

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_daily_fare,
    SUM(extra) AS revenue_daily_extra,
    SUM(mta_tax) AS revenue_daily_mta_tax,
    SUM(tip_amount) AS revenue_daily_tip_amount,
    SUM(tolls_amount) AS revenue_daily_tolls_amount,
    SUM(improvement_surcharge) AS revenue_daily_improvement_surcharge,
    SUM(total_amount) AS revenue_daily_total_amount,
    
    -- Additional calculations
    COUNT(*) AS trip_count,
    AVG(passenger_count) AS avg_daily_passenger_count,
    AVG(trip_distance) AS avg_daily_trip_distance
FROM
    trips_data
GROUP BY
    1, 2
ORDER BY
    revenue_day, revenue_zone
""")

# Show sample results
print("\nSample of daily revenue by location:")
df_result.show(20, truncate=False)

# Write results to output path
print(f"\nWriting results to {output}...")
df_result.coalesce(1).write.parquet(output, mode='overwrite')

print("Revenue report completed successfully!")

# Stop Spark session
spark.stop()
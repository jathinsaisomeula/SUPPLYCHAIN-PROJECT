# spark_processor.py (Corrected Schema and Transformations)
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType, IntegerType, FloatType

# 1. Initialize SparkSession
# Make sure to specify the Kafka package for Spark
# spark-sql-kafka-0-10_2.12:3.5.1 is for Spark 3.5.x and Scala 2.12
spark = SparkSession.builder \
    .appName("SupplyChainKafkaProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Reduce verbosity of Spark logs for cleaner output
spark.sparkContext.setLogLevel("WARN")

print("SparkSession initialized and connected to Kafka package.")

# 2. Define the Schema for your incoming JSON messages
# THIS SCHEMA MUST NOW MATCH THE COLUMNS IN YOUR supply_chain_data.csv
# AND THE 'id' FIELD ADDED BY YOUR PRODUCER.
# Adjust data types based on your CSV's actual content.
input_schema = StructType([
    StructField("id", StringType(), True), # Added by producer
    StructField("Product type", StringType(), True),
    StructField("Stock levels", IntegerType(), True), # Changed to IntegerType
    StructField("Location", StringType(), True),
    StructField("Shipping costs", FloatType(), True), # Changed to FloatType
    StructField("Lead time", IntegerType(), True), # Changed to IntegerType
    StructField("Order quantities", IntegerType(), True), # Changed to IntegerType
    StructField("Shipping carriers", StringType(), True),
    StructField("Manufacturing costs", FloatType(), True), # Changed to FloatType
    StructField("Inspection results", StringType(), True),
    StructField("Defect rates", FloatType(), True), # Changed to FloatType
    StructField("Transportation modes", StringType(), True),
    StructField("Routes", StringType(), True),
    StructField("SKUs", StringType(), True),
    StructField("Manufacturing plants", StringType(), True),
    StructField("Production volumes", IntegerType(), True), # Changed to IntegerType
    StructField("Customer demographics", StringType(), True),
    StructField("Sales channels", StringType(), True),
    StructField("Demand fluctuations", StringType(), True),
    StructField("Supplier performance metrics", StringType(), True),
    StructField("Supplier risks", StringType(), True),
    StructField("Compliance issues", StringType(), True),
    StructField("Storage conditions", StringType(), True),
    StructField("Safety stock levels", IntegerType(), True) # Changed to IntegerType
])

# 3. Read from Kafka topic as a streaming DataFrame
# Ensure 'kafka:29092' points to your Dockerized Kafka broker
kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "supply_chain_data"

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

print(f"Reading stream from Kafka topic: {kafka_topic}")

# 4. Process the Kafka messages
# The 'value' column from Kafka is binary, so cast it to STRING, then parse as JSON
parsed_df = df_kafka.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_ingestion_timestamp") \
                     .withColumn("data", from_json(col("json_payload"), input_schema)) \
                     .select("data.*", "kafka_ingestion_timestamp")

# Example Transformation (using fields from your CSV data)
# For instance, calculate total cost from order quantities and manufacturing costs
transformed_df = parsed_df \
    .withColumn("total_cost", col("Order quantities") * col("Manufacturing costs")) \
    .withColumn("processing_timestamp", current_timestamp())

# Select desired columns for final output, using actual CSV column names
final_df = transformed_df.select(
    "id",
    col("Product type").alias("product_type"), # Renaming for consistency
    col("Stock levels").alias("stock_levels"),
    col("Location").alias("location"),
    col("Shipping costs").alias("shipping_costs"),
    col("Lead time").alias("lead_time"),
    col("Order quantities").alias("order_quantities"),
    col("Shipping carriers").alias("shipping_carriers"),
    col("Manufacturing costs").alias("manufacturing_costs"),
    col("Inspection results").alias("inspection_results"),
    col("Defect rates").alias("defect_rates"),
    col("Transportation modes").alias("transportation_modes"),
    col("Routes").alias("routes"),
    col("SKUs").alias("skus"),
    col("Manufacturing plants").alias("manufacturing_plants"),
    col("Production volumes").alias("production_volumes"),
    col("Customer demographics").alias("customer_demographics"),
    col("Sales channels").alias("sales_channels"),
    col("Demand fluctuations").alias("demand_fluctuations"),
    col("Supplier performance metrics").alias("supplier_performance_metrics"),
    col("Supplier risks").alias("supplier_risks"),
    col("Compliance issues").alias("compliance_issues"),
    col("Storage conditions").alias("storage_conditions"),
    col("Safety stock levels").alias("safety_stock_levels"),
    "total_cost", # The newly calculated column
    "processing_timestamp",
    "kafka_ingestion_timestamp"
)

print("Applying transformations...")

# 5. Write the results (e.g., to console for testing)
# In a real project, you'd write to BigQuery, another Kafka topic, or a data lake (like GCS)
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) # Set to False to see full column contents
    .start()

print("Spark streaming query started. Waiting for data...")

query.awaitTermination() # Keep the script running to process continuous streams
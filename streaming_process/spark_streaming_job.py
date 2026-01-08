"""
Spark Streaming Job - Real-time event processing
Processes events from Kafka and writes to Redis and PostgreSQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import redis
import psycopg2
from datetime import datetime

# ======================
# CONFIGURATION
# ======================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = "page_views,cart_events,purchases,sessions"
CHECKPOINT_DIR = "./checkpoint"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_analytics',
    'user': 'ecommerce',
    'password': 'ecommerce123'
}

# ======================
# EVENT SCHEMA
# ======================
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("user_name", StringType(), True),
    StructField("user_segment", StringType(), True),
    StructField("device", StringType(), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ]), True),
    # Product fields (for product_view, add_to_cart)
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_category", StringType(), True),
    # Purchase fields
    StructField("order_id", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    # Session fields
    StructField("session_duration_seconds", IntegerType(), True),
    StructField("reason", StringType(), True)
])

# ======================
# SPARK SESSION
# ======================
def create_spark_session():
    """Create Spark session with required configurations"""
    import os
    
    # Set Spark configs for local mode
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'
    
    return SparkSession.builder \
        .appName("EcommerceStreamProcessing") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

# ======================
# REDIS CLIENT
# ======================
class RedisClient:
    """Redis client for storing real-time metrics"""
    
    def __init__(self):
        self.client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
    
    def set_metric(self, key, value, expire=300):
        """Set a metric with expiration (default 5 minutes)"""
        self.client.setex(key, expire, json.dumps(value))
    
    def get_metric(self, key):
        """Get a metric from Redis"""
        value = self.client.get(key)
        return json.loads(value) if value else None
    
    def increment(self, key, amount=1):
        """Increment a counter"""
        return self.client.incrby(key, amount)

# ======================
# STREAMING TRANSFORMATIONS
# ======================
def process_streaming_data(spark):
    """Main streaming processing logic"""
    
    # Read from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
    
    # Parse JSON events
    events = kafka_stream \
        .select(
            from_json(col("value").cast("string"), event_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic")
        ) \
        .select("data.*", "kafka_timestamp", "topic")
    
    # Add processing timestamp and watermark
    events_with_watermark = events \
        .withColumn("processing_time", current_timestamp()) \
        .withWatermark("processing_time", "10 seconds")
    
    return events_with_watermark

# ======================
# REAL-TIME AGGREGATIONS
# ======================
def calculate_realtime_metrics(events):
    """Calculate real-time metrics for dashboard"""
    
    # 1. Active users in last 5 minutes
    active_users = events \
        .filter(col("processing_time") >= current_timestamp() - expr("INTERVAL 5 MINUTES")) \
        .groupBy(window(col("processing_time"), "1 minute")) \
        .agg(
            countDistinct("user_id").alias("active_users"),
            count("*").alias("total_events")
        )
    
    # 2. Revenue per minute
    purchases = events.filter(col("event_type") == "purchase")
    revenue_per_minute = purchases \
        .groupBy(window(col("processing_time"), "1 minute")) \
        .agg(
            sum("total_amount").alias("revenue"),
            count("*").alias("purchase_count"),
            avg("total_amount").alias("avg_order_value")
        )
    
    # 3. Top products (last 5 minutes)
    product_views = events.filter(col("event_type") == "product_view")
    top_products = product_views \
        .filter(col("processing_time") >= current_timestamp() - expr("INTERVAL 5 MINUTES")) \
        .groupBy("product_id", "product_name", "product_category") \
        .agg(count("*").alias("view_count")) \
        .orderBy(col("view_count").desc()) \
        .limit(10)
    
    # 4. Cart abandonment rate
    cart_adds = events.filter(col("event_type") == "add_to_cart") \
        .groupBy(window(col("processing_time"), "5 minutes")) \
        .agg(countDistinct("session_id").alias("carts"))
    
    purchases_by_session = purchases \
        .groupBy(window(col("processing_time"), "5 minutes")) \
        .agg(countDistinct("session_id").alias("purchases"))
    
    # 5. Events per second
    events_per_second = events \
        .groupBy(window(col("processing_time"), "10 seconds")) \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .withColumn("events_per_second", 
                   col("event_count") / 10.0)
    
    return {
        'active_users': active_users,
        'revenue': revenue_per_minute,
        'top_products': top_products,
        'events_rate': events_per_second
    }

# ======================
# WRITE TO REDIS
# ======================
def write_to_redis(df, epoch_id):
    """Write aggregated metrics to Redis"""
    redis_client = RedisClient()
    
    # Convert to pandas and write to Redis
    pandas_df = df.toPandas()
    
    for _, row in pandas_df.iterrows():
        metric_data = row.to_dict()
        
        # Determine metric type and key
        if 'active_users' in metric_data:
            redis_client.set_metric('realtime:active_users', 
                                   metric_data['active_users'])
        
        elif 'revenue' in metric_data:
            redis_client.set_metric('realtime:revenue_last_minute', 
                                   metric_data['revenue'])
            redis_client.set_metric('realtime:avg_order_value', 
                                   metric_data.get('avg_order_value', 0))
        
        elif 'view_count' in metric_data:
            # Store top products as sorted set
            redis_client.client.zadd(
                'realtime:top_products',
                {metric_data['product_name']: metric_data['view_count']}
            )
        
        elif 'events_per_second' in metric_data:
            redis_client.set_metric('realtime:events_per_second', 
                                   metric_data['events_per_second'])

# ======================
# ANOMALY DETECTION
# ======================
def detect_anomalies(events):
    """Detect anomalies in real-time"""
    
    # Calculate baseline metrics (you'd normally load from historical data)
    current_metrics = events \
        .groupBy(window(col("processing_time"), "1 minute")) \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("user_count")
        )
    
    # Simple threshold-based anomaly detection
    anomalies = current_metrics \
        .filter((col("event_count") < 10) | (col("event_count") > 10000))
    
    return anomalies

# ======================
# WRITE TO CONSOLE (for debugging)
# ======================
def write_to_console(df, output_mode="update"):
    """Write stream to console for debugging"""
    return df.writeStream \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", False) \
        .start()

# ======================
# WRITE TO POSTGRES
# ======================
def write_to_postgres(df, epoch_id, table_name):
    """Write batch to PostgreSQL"""
    
    # Convert to pandas
    pandas_df = df.toPandas()
    
    if pandas_df.empty:
        return
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    try:
        for _, row in pandas_df.iterrows():
            # Prepare insert statement (customize based on table)
            if table_name == 'realtime_metrics':
                cursor.execute("""
                    INSERT INTO realtime_metrics 
                    (timestamp, metric_name, metric_value)
                    VALUES (%s, %s, %s)
                """, (row['window']['start'], 'active_users', row.get('active_users', 0)))
        
        conn.commit()
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ======================
# MAIN EXECUTION
# ======================
def main():
    """Main execution function"""
    
    print("\n" + "="*60)
    print("SPARK STREAMING JOB - STARTING")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Process streaming data
    events = process_streaming_data(spark)
    
    # Calculate real-time metrics
    metrics = calculate_realtime_metrics(events)
    
    # Start streaming queries
    queries = []
    
    # 1. Write active users to Redis
    query1 = metrics['active_users'].writeStream \
        .foreachBatch(write_to_redis) \
        .outputMode("update") \
        .start()
    queries.append(query1)
    
    # 2. Write revenue metrics to Redis
    query2 = metrics['revenue'].writeStream \
        .foreachBatch(write_to_redis) \
        .outputMode("update") \
        .start()
    queries.append(query2)
    
    # 3. Write top products to Redis
    query3 = metrics['top_products'].writeStream \
        .foreachBatch(write_to_redis) \
        .outputMode("complete") \
        .trigger(processingTime='10 seconds') \
        .start()
    queries.append(query3)
    
    # 4. Console output for monitoring
    query4 = events \
        .groupBy("event_type") \
        .agg(count("*").alias("count")) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='30 seconds') \
        .start()
    queries.append(query4)
    
    print("\n✅ All streaming queries started")
    print("📊 Processing events in real-time...")
    print("Press Ctrl+C to stop\n")
    
    # Wait for all queries to finish
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠️  Stopping streaming queries...")
        for query in queries:
            query.stop()
        spark.stop()
        print("✅ Stopped gracefully")

if __name__ == '__main__':
    main()
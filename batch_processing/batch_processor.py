"""
Batch Processor - Stores events from Kafka to PostgreSQL
Runs periodically to persist data for historical analysis
"""

import json
import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ======================
# CONFIGURATION
# ======================
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPICS = ['page_views', 'cart_events', 'purchases', 'sessions']
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_analytics',
    'user': 'ecommerce',
    'password': 'ecommerce123'
}
BATCH_SIZE = 1000  # Insert 1000 records at a time

# ======================
# DATABASE CONNECTION
# ======================
class PostgresWriter:
    """Handle PostgreSQL connections and writes"""
    
    def __init__(self):
        self.conn = None
        self.connect()
    
    def connect(self):
        """Connect to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**POSTGRES_CONFIG)
            self.conn.autocommit = False
            logger.info("✅ Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            raise
    
    def ensure_connection(self):
        """Ensure connection is alive"""
        if self.conn is None or self.conn.closed:
            self.connect()
    
    def insert_events_batch(self, events):
        """Insert batch of events into raw_events table"""
        if not events:
            return 0
        
        self.ensure_connection()
        cursor = self.conn.cursor()
        
        try:
            # Prepare insert query
            insert_query = """
                INSERT INTO raw_events (
                    event_id, timestamp, event_type, session_id, user_id,
                    user_name, user_segment, device, city, country,
                    product_id, product_name, product_price, product_category,
                    order_id, total_amount, payment_method,
                    session_duration_seconds, exit_reason
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (event_id) DO NOTHING
            """
            
            # Prepare data tuples
            data = []
            for event in events:
                location = event.get('location', {})
                data.append((
                    event.get('event_id'),
                    event.get('timestamp'),
                    event.get('event_type'),
                    event.get('session_id'),
                    event.get('user_id'),
                    event.get('user_name'),
                    event.get('user_segment'),
                    event.get('device'),
                    location.get('city'),
                    location.get('country'),
                    event.get('product_id'),
                    event.get('product_name'),
                    event.get('product_price'),
                    event.get('product_category'),
                    event.get('order_id'),
                    event.get('total_amount'),
                    event.get('payment_method'),
                    event.get('session_duration_seconds'),
                    event.get('reason')
                ))
            
            # Execute batch insert
            execute_batch(cursor, insert_query, data, page_size=500)
            self.conn.commit()
            
            logger.info(f"✅ Inserted {len(events)} events into PostgreSQL")
            return len(events)
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Error inserting events: {e}")
            return 0
        finally:
            cursor.close()
    
    def get_stats(self):
        """Get current database stats"""
        self.ensure_connection()
        cursor = self.conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT session_id) as unique_sessions,
                    MIN(timestamp) as first_event,
                    MAX(timestamp) as last_event
                FROM raw_events
            """)
            
            result = cursor.fetchone()
            return {
                'total_events': result[0],
                'unique_users': result[1],
                'unique_sessions': result[2],
                'first_event': result[3],
                'last_event': result[4]
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return None
        finally:
            cursor.close()
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")

# ======================
# KAFKA TO POSTGRES PIPELINE
# ======================
def process_kafka_to_postgres():
    """Read from Kafka and write to PostgreSQL"""
    
    logger.info("🚀 Starting Batch Processor (Kafka → PostgreSQL)")
    
    # Initialize consumer
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='postgres-batch-processor',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # 10 second timeout
    )
    
    logger.info("✅ Kafka consumer initialized")
    
    # Initialize database writer
    db_writer = PostgresWriter()
    
    # Batch processing
    event_batch = []
    total_processed = 0
    
    try:
        logger.info("📖 Reading events from Kafka...\n")
        
        for message in consumer:
            event = message.value
            event_batch.append(event)
            
            # Insert when batch is full
            if len(event_batch) >= BATCH_SIZE:
                inserted = db_writer.insert_events_batch(event_batch)
                total_processed += inserted
                event_batch = []
                
                # Print progress
                if total_processed % 5000 == 0:
                    stats = db_writer.get_stats()
                    if stats:
                        logger.info(f"\n📊 Progress Update:")
                        logger.info(f"  Events in DB: {stats['total_events']:,}")
                        logger.info(f"  Unique Users: {stats['unique_users']:,}")
                        logger.info(f"  Sessions: {stats['unique_sessions']:,}")
        
        # Insert remaining events
        if event_batch:
            inserted = db_writer.insert_events_batch(event_batch)
            total_processed += inserted
        
        # Final statistics
        logger.info("\n" + "="*60)
        logger.info("✅ BATCH PROCESSING COMPLETE")
        logger.info("="*60)
        logger.info(f"Total events processed: {total_processed:,}")
        
        stats = db_writer.get_stats()
        if stats:
            logger.info(f"\n📊 Database Statistics:")
            logger.info(f"  Total Events: {stats['total_events']:,}")
            logger.info(f"  Unique Users: {stats['unique_users']:,}")
            logger.info(f"  Unique Sessions: {stats['unique_sessions']:,}")
            logger.info(f"  First Event: {stats['first_event']}")
            logger.info(f"  Last Event: {stats['last_event']}")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        if event_batch:
            db_writer.insert_events_batch(event_batch)
    
    finally:
        consumer.close()
        db_writer.close()
        logger.info("✅ Cleanup complete")

# ======================
# MAIN
# ======================
if __name__ == '__main__':
    print("\n" + "="*60)
    print("BATCH PROCESSOR - KAFKA TO POSTGRESQL")
    print("="*60)
    print("This process reads events from Kafka and stores them")
    print("in PostgreSQL for historical analysis and reporting.")
    print("="*60 + "\n")
    
    process_kafka_to_postgres()
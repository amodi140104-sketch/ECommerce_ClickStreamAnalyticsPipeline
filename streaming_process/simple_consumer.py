"""
Simple Kafka Consumer - Processes events without Spark
For quick testing and development
"""

import json
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import redis
import logging

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
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# ======================
# METRICS TRACKER
# ======================
class RealTimeMetrics:
    """Track real-time metrics in memory"""
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )
        
        # Sliding windows for metrics
        self.window_5min = deque(maxlen=300)  # 5 minutes of seconds
        self.active_users = set()
        self.product_views = defaultdict(int)
        
        # Counters
        self.total_events = 0
        self.event_types = defaultdict(int)
        
        # Revenue tracking
        self.revenue_last_minute = 0
        self.last_revenue_reset = time.time()
        
        logger.info("✅ Metrics tracker initialized")
    
    def process_event(self, event):
        """Process a single event and update metrics"""
        
        self.total_events += 1
        event_type = event.get('event_type', 'unknown')
        self.event_types[event_type] += 1
        
        # Track active users
        user_id = event.get('user_id')
        if user_id:
            self.active_users.add(user_id)
        
        # Track product views
        if event_type == 'product_view':
            product_name = event.get('product_name', 'Unknown')
            self.product_views[product_name] += 1
        
        # Track revenue
        if event_type == 'purchase':
            amount = event.get('total_amount', 0)
            self.revenue_last_minute += amount
        
        # Reset revenue counter every minute
        if time.time() - self.last_revenue_reset > 60:
            self.last_revenue_reset = time.time()
            self.revenue_last_minute = 0
    
    def calculate_and_store_metrics(self):
        """Calculate metrics and store in Redis"""
        
        try:
            # Active users
            active_count = len(self.active_users)
            self.redis_client.setex(
                'realtime:active_users',
                300,  # 5 minute expiry
                active_count
            )
            
            # Events per second (approximate)
            events_per_sec = self.total_events / max(1, time.time() - start_time)
            self.redis_client.setex(
                'realtime:events_per_second',
                60,
                f"{events_per_sec:.2f}"
            )
            
            # Revenue last minute
            self.redis_client.setex(
                'realtime:revenue_last_minute',
                60,
                f"{self.revenue_last_minute:.2f}"
            )
            
            # Top 10 products
            top_products = sorted(
                self.product_views.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            self.redis_client.setex(
                'realtime:top_products',
                300,
                json.dumps([
                    {'name': name, 'count': count}
                    for name, count in top_products
                ])
            )
            
            # Event type breakdown
            self.redis_client.setex(
                'realtime:event_types',
                60,
                json.dumps(dict(self.event_types))
            )
            
        except Exception as e:
            logger.error(f"Error storing metrics in Redis: {e}")
    
    def print_stats(self):
        """Print current statistics"""
        elapsed = time.time() - start_time
        rate = self.total_events / elapsed if elapsed > 0 else 0
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 REAL-TIME METRICS")
        logger.info(f"{'='*60}")
        logger.info(f"Total Events:    {self.total_events:,}")
        logger.info(f"Active Users:    {len(self.active_users):,}")
        logger.info(f"Events/sec:      {rate:.1f}")
        logger.info(f"Revenue (1min):  ${self.revenue_last_minute:,.2f}")
        
        logger.info(f"\n📈 Event Breakdown:")
        for event_type, count in sorted(self.event_types.items()):
            percentage = (count / self.total_events) * 100
            logger.info(f"  {event_type:20s}: {count:6,} ({percentage:5.1f}%)")
        
        if self.product_views:
            logger.info(f"\n🔥 Top 5 Products:")
            for product, count in sorted(
                self.product_views.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]:
                logger.info(f"  {product:30s}: {count:4,} views")
        
        logger.info(f"{'='*60}\n")

# ======================
# KAFKA CONSUMER
# ======================
def consume_and_process():
    """Main consumer loop"""
    
    logger.info("🚀 Starting Kafka Consumer")
    logger.info(f"📡 Subscribing to topics: {KAFKA_TOPICS}")
    
    # Create consumer
    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # Start from beginning (read all messages)
        enable_auto_commit=True,
        group_id='realtime-processor',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000  # 5 second timeout for stats printing
    )
    
    logger.info("✅ Consumer connected")
    
    # Initialize metrics
    metrics = RealTimeMetrics()
    
    global start_time
    start_time = time.time()
    
    last_stats_print = time.time()
    last_redis_update = time.time()
    
    try:
        logger.info("📖 Waiting for messages... (Press Ctrl+C to stop)\n")
        
        for message in consumer:
            try:
                event = message.value
                
                # Process event
                metrics.process_event(event)
                
                # Update Redis every 5 seconds
                if time.time() - last_redis_update > 5:
                    metrics.calculate_and_store_metrics()
                    last_redis_update = time.time()
                
                # Print stats every 10 seconds
                if time.time() - last_stats_print > 10:
                    metrics.print_stats()
                    last_stats_print = time.time()
                
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding message: {e}")
            except Exception as e:
                logger.error(f"Error processing event: {e}")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Stopping consumer...")
    finally:
        # Final stats
        metrics.print_stats()
        consumer.close()
        logger.info("✅ Consumer closed")

# ======================
# MAIN
# ======================
if __name__ == '__main__':
    print("\n" + "="*60)
    print("SIMPLE KAFKA CONSUMER - REAL-TIME PROCESSING")
    print("="*60)
    print("This is a simplified version without Spark")
    print("For production, use the Spark Streaming job")
    print("="*60 + "\n")
    
    consume_and_process()
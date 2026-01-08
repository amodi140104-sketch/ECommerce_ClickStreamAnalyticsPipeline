"""
Kafka Producer - Streams events from event generator to Kafka topics
Handles retries, error handling, and performance monitoring
"""

import json
import time
import sys
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
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
TOPICS = {
    'page_views': {'partitions': 3, 'replication': 1},
    'cart_events': {'partitions': 2, 'replication': 1},
    'purchases': {'partitions': 2, 'replication': 1},
    'sessions': {'partitions': 2, 'replication': 1}
}

# ======================
# TOPIC MANAGEMENT
# ======================
def create_topics():
    """Create Kafka topics if they don't exist"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='topic-creator'
        )
        
        # Get existing topics
        existing_topics = admin_client.list_topics()
        
        # Create new topics
        new_topics = []
        for topic_name, config in TOPICS.items():
            if topic_name not in existing_topics:
                new_topics.append(
                    NewTopic(
                        name=topic_name,
                        num_partitions=config['partitions'],
                        replication_factor=config['replication']
                    )
                )
        
        if new_topics:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            logger.info(f"✅ Created {len(new_topics)} topics: {[t.name for t in new_topics]}")
        else:
            logger.info("✅ All topics already exist")
        
        admin_client.close()
        
    except Exception as e:
        logger.error(f"❌ Error creating topics: {e}")
        raise

# ======================
# PRODUCER CLASS
# ======================
class EventProducer:
    """
    Kafka producer for streaming e-commerce events
    Features:
    - Automatic retries
    - Performance monitoring
    - Event routing by type
    - Error handling
    """
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            
            # Serialization
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            
            # Performance tuning
            compression_type=None,      # No compression (simpler for Windows)
            batch_size=16384,           # Batch size in bytes
            linger_ms=10,               # Wait 10ms to batch messages
            buffer_memory=33554432,     # 32MB buffer
            
            # Reliability
            acks='all',                 # Wait for all replicas
            retries=3,                  # Retry failed sends
            max_in_flight_requests_per_connection=1,  # Required for idempotence
            
            # Timeouts
            request_timeout_ms=30000,
            
            # Idempotence (exactly-once semantics)
            enable_idempotence=True
        )
        
        self.stats = {
            'sent': 0,
            'failed': 0,
            'bytes_sent': 0,
            'start_time': time.time()
        }
        
        logger.info("✅ Kafka Producer initialized")
    
    def route_event(self, event):
        """
        Route event to appropriate topic based on event type
        Returns: (topic_name, partition_key)
        """
        event_type = event.get('event_type', '')
        user_id = event.get('user_id', '')
        
        # Route based on event type
        if event_type in ['product_view', 'search']:
            return 'page_views', user_id
        elif event_type in ['add_to_cart', 'remove_from_cart']:
            return 'cart_events', user_id
        elif event_type == 'purchase':
            return 'purchases', user_id
        elif event_type in ['session_start', 'session_end']:
            return 'sessions', event.get('session_id', user_id)
        else:
            return 'page_views', user_id  # Default topic
    
    def send_event(self, event):
        """
        Send single event to Kafka
        Returns: True if successful, False otherwise
        """
        try:
            # Route to appropriate topic
            topic, key = self.route_event(event)
            
            # Send asynchronously with callback
            future = self.producer.send(
                topic=topic,
                value=event,
                key=key
            )
            
            # Add callback for success/failure
            future.add_callback(self.on_send_success, event, topic)
            future.add_errback(self.on_send_error, event, topic)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending event: {e}")
            self.stats['failed'] += 1
            return False
    
    def on_send_success(self, record_metadata, event, topic):
        """Callback for successful send"""
        self.stats['sent'] += 1
        self.stats['bytes_sent'] += len(json.dumps(event))
        
        # Log every 100 events
        if self.stats['sent'] % 100 == 0:
            self.print_stats()
    
    def on_send_error(self, exception, event, topic):
        """Callback for failed send"""
        self.stats['failed'] += 1
        logger.error(f"❌ Failed to send event to {topic}: {exception}")
    
    def print_stats(self):
        """Print producer statistics"""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['sent'] / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"📊 Stats: {self.stats['sent']:,} sent | "
            f"{self.stats['failed']} failed | "
            f"{rate:.1f} events/sec | "
            f"{self.stats['bytes_sent'] / 1024 / 1024:.2f} MB sent"
        )
    
    def flush_and_close(self):
        """Flush pending messages and close producer"""
        logger.info("🔄 Flushing pending messages...")
        self.producer.flush()
        self.producer.close()
        self.print_stats()
        logger.info("✅ Producer closed")

# ======================
# STREAMING FROM FILE
# ======================
def stream_from_file(file_path, events_per_second=100, producer=None):
    """
    Stream events from historical data file
    Simulates real-time event flow
    """
    if producer is None:
        producer = EventProducer()
    
    logger.info(f"📖 Reading events from {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            events = [json.loads(line) for line in f]
        
        logger.info(f"✅ Loaded {len(events):,} events")
        logger.info(f"🚀 Streaming at {events_per_second} events/second")
        
        delay = 1.0 / events_per_second  # Delay between events
        
        for i, event in enumerate(events):
            producer.send_event(event)
            
            # Rate limiting
            time.sleep(delay)
            
            # Progress update
            if (i + 1) % 1000 == 0:
                logger.info(f"Progress: {i+1:,}/{len(events):,} events")
        
        # Flush remaining messages
        producer.flush_and_close()
        
        logger.info(f"✅ Completed streaming {len(events):,} events")
        
    except FileNotFoundError:
        logger.error(f"❌ File not found: {file_path}")
        logger.error("💡 Run event_generator.py first to create the file")
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        producer.flush_and_close()
    except Exception as e:
        logger.error(f"❌ Error streaming events: {e}")
        producer.flush_and_close()

# ======================
# STREAMING FROM GENERATOR
# ======================
def stream_from_generator(producer=None):
    """
    Stream events from live event generator
    Generates events in real-time
    """
    if producer is None:
        producer = EventProducer()
    
    # Import event generator
    sys.path.append('../1_data_generation')
    from event_generator import ShoppingSession, USERS
    import random
    from datetime import datetime
    
    logger.info("🚀 Starting real-time event generation and streaming")
    
    try:
        session_count = 0
        
        while True:
            # Generate session
            user = random.choice(USERS)
            session = ShoppingSession(user, datetime.now())
            events = session.simulate_complete_journey()
            
            # Send all events from session
            for event in events:
                producer.send_event(event)
            
            session_count += 1
            
            if session_count % 10 == 0:
                logger.info(f"Generated {session_count} sessions")
            
            # Wait before next session
            time.sleep(random.uniform(1, 5))
    
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        producer.flush_and_close()
    except Exception as e:
        logger.error(f"❌ Error in real-time streaming: {e}")
        producer.flush_and_close()

# ======================
# MAIN EXECUTION
# ======================
def main():
    """Main execution function"""
    
    print("\n" + "="*60)
    print("KAFKA EVENT PRODUCER")
    print("="*60)
    
    # Create topics first
    logger.info("📋 Setting up Kafka topics...")
    create_topics()
    
    # Wait for Kafka to be ready
    time.sleep(2)
    
    # Choose mode
    if len(sys.argv) > 1:
        if sys.argv[1] == 'file':
            # Stream from file
            file_path = sys.argv[2] if len(sys.argv) > 2 else '../1_data_generation/historical_events.jsonl'
            events_per_sec = int(sys.argv[3]) if len(sys.argv) > 3 else 100
            
            stream_from_file(file_path, events_per_sec)
            
        elif sys.argv[1] == 'live':
            # Stream from generator
            stream_from_generator()
        else:
            print("Usage:")
            print("  python kafka_producer.py file [filepath] [events_per_sec]")
            print("  python kafka_producer.py live")
    else:
        # Default: stream from file
        stream_from_file('../data_generation/historical_events.jsonl', events_per_second=1000)

if __name__ == '__main__':
    main()

    
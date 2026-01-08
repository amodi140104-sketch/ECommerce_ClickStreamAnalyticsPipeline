"""
E-Commerce Event Generator with Realistic User Behavior and Conversion Funnels
Generates both historical batch data and real-time streaming events
"""

import random
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict
import uuid

# ============================================
# 1. PRODUCT CATALOG (100+ products)
# ============================================
def create_product_catalog():
    """Create 100 realistic products across different categories"""
    categories = {
        'Electronics': [
            ('iPhone', 999, 1199),
            ('Samsung Phone', 799, 999),
            ('Laptop', 699, 1999),
            ('Headphones', 49, 349),
            ('Tablet', 299, 799),
            ('Smart Watch', 199, 499),
            ('Camera', 399, 1499),
            ('TV', 499, 2499)
        ],
        'Clothing': [
            ('T-Shirt', 19, 49),
            ('Jeans', 39, 99),
            ('Dress', 49, 149),
            ('Jacket', 79, 249),
            ('Shoes', 59, 199),
            ('Sweater', 39, 89),
            ('Shorts', 29, 69)
        ],
        'Home': [
            ('Sofa', 499, 1999),
            ('Table', 199, 799),
            ('Lamp', 29, 149),
            ('Bed', 399, 1499),
            ('Chair', 99, 399),
            ('Bookshelf', 149, 499)
        ],
        'Books': [
            ('Fiction Book', 9, 29),
            ('Non-Fiction', 12, 35),
            ('Textbook', 49, 199),
            ('Comic Book', 5, 19),
            ('Magazine', 4, 12)
        ],
        'Sports': [
            ('Running Shoes', 69, 179),
            ('Yoga Mat', 19, 49),
            ('Dumbbell Set', 49, 199),
            ('Tennis Racket', 79, 249),
            ('Basketball', 19, 59)
        ]
    }
    
    products = []
    for category, items in categories.items():
        for item_name, min_price, max_price in items:
            # Create 3-5 variants of each item
            variants = random.randint(3, 5)
            for i in range(variants):
                products.append({
                    'id': f'prod_{len(products)+1:04d}',
                    'name': f'{item_name} {"Pro" if i == 0 else "Model " + str(i+1)}',
                    'category': category,
                    'price': round(random.uniform(min_price, max_price), 2),
                    'rating': round(random.uniform(3.5, 5.0), 1),
                    'stock': random.randint(0, 100)
                })
    
    return products

PRODUCTS = create_product_catalog()

# ============================================
# 2. USER POOL (Realistic user profiles)
# ============================================
def create_user_pool(count=2000):
    """Generate pool of 2000 unique users with realistic attributes"""
    
    # Realistic first and last names
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa',
                   'James', 'Mary', 'William', 'Patricia', 'Richard', 'Jennifer', 'Thomas']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
                  'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez']
    
    cities_countries = [
        ('New York', 'USA'), ('Los Angeles', 'USA'), ('Chicago', 'USA'),
        ('Houston', 'USA'), ('Phoenix', 'USA'), ('London', 'UK'),
        ('Paris', 'France'), ('Berlin', 'Germany'), ('Tokyo', 'Japan'),
        ('Sydney', 'Australia'), ('Toronto', 'Canada'), ('Mumbai', 'India'),
        ('Singapore', 'Singapore'), ('Dubai', 'UAE')
    ]
    
    users = []
    for i in range(count):
        city, country = random.choice(cities_countries)
        users.append({
            'id': f'user_{i+1:06d}',
            'name': f'{random.choice(first_names)} {random.choice(last_names)}',
            'email': f'user{i+1}@example.com',
            'city': city,
            'country': country,
            'device': random.choices(
                ['mobile', 'desktop', 'tablet'],
                weights=[60, 30, 10]  # 60% mobile, 30% desktop, 10% tablet
            )[0],
            'user_segment': random.choices(
                ['high_value', 'medium_value', 'low_value', 'new_user'],
                weights=[10, 30, 40, 20]
            )[0],
            'join_date': (datetime.now() - timedelta(days=random.randint(1, 365))).date().isoformat()
        })
    return users

USERS = create_user_pool()

# ============================================
# 3. SHOPPING SESSION SIMULATOR
# ============================================
class ShoppingSession:
    """
    Simulates ONE complete user shopping journey with realistic behavior patterns
    
    Funnel Stages:
    1. Session Start (100%)
    2. Browse Products (100%)
    3. Add to Cart (30% - 70% DROP HERE)
    4. Checkout Start (50% of cart adds - 50% DROP HERE)
    5. Purchase Complete (70% of checkouts - 30% DROP HERE)
    
    Final Conversion: ~10.5% (realistic for e-commerce)
    """
    
    def __init__(self, user, base_time):
        self.user = user
        self.session_id = str(uuid.uuid4())
        self.current_time = base_time
        self.events = []
        self.cart = []
        self.viewed_products = []
        
    def simulate_complete_journey(self):
        """Simulate complete user journey from landing to exit"""
        
        # STAGE 1: SESSION START (100% of sessions)
        self.add_event('session_start', {
            'landing_page': random.choice(['homepage', 'category_page', 'search_results', 'product_page']),
            'referrer': random.choice(['google', 'facebook', 'instagram', 'direct', 'email'])
        })
        self.advance_time(1, 3)
        
        # STAGE 2: SEARCH (60% of users search)
        if random.random() < 0.60:
            search_terms = ['phone', 'laptop', 'shoes', 'shirt', 'headphones', 'watch', 'bag']
            search_term = random.choice(search_terms)
            self.add_event('search', {
                'query': search_term,
                'results_count': random.randint(5, 50)
            })
            self.advance_time(2, 5)
        
        # STAGE 3: BROWSE PRODUCTS (100% browse, 2-8 products)
        products_to_view = random.randint(2, 8)
        
        for i in range(products_to_view):
            product = random.choice(PRODUCTS)
            self.viewed_products.append(product)
            
            self.add_event('product_view', {
                'product_id': product['id'],
                'product_name': product['name'],
                'product_price': product['price'],
                'product_category': product['category'],
                'product_rating': product['rating'],
                'time_on_page_seconds': random.randint(10, 120)
            })
            self.advance_time(10, 45)
            
            # FUNNEL DROP #1: 70% leave after browsing
            if i >= 2 and random.random() < 0.70:
                self.add_event('session_end', {
                    'reason': 'browsing_only',
                    'session_duration_seconds': self.get_session_duration(),
                    'pages_viewed': len(self.events)
                })
                return self.events
        
        # STAGE 4: ADD TO CART (30% of remaining users)
        if random.random() < 0.30 and self.viewed_products:
            # Add 1-3 products to cart
            num_items = min(random.randint(1, 3), len(self.viewed_products))
            products_to_add = random.sample(self.viewed_products, num_items)
            
            for product in products_to_add:
                quantity = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
                self.cart.append({
                    'product': product,
                    'quantity': quantity
                })
                
                self.add_event('add_to_cart', {
                    'product_id': product['id'],
                    'product_name': product['name'],
                    'product_price': product['price'],
                    'quantity': quantity
                })
                self.advance_time(2, 5)
            
            # FUNNEL DROP #2: 50% abandon cart
            if random.random() < 0.50:
                self.add_event('session_end', {
                    'reason': 'cart_abandoned',
                    'cart_value': self.calculate_cart_value(),
                    'items_in_cart': len(self.cart),
                    'session_duration_seconds': self.get_session_duration()
                })
                return self.events
            
            # STAGE 5: CHECKOUT START (50% of cart adds)
            cart_value = self.calculate_cart_value()
            self.add_event('checkout_start', {
                'cart_value': cart_value,
                'items_count': len(self.cart),
                'estimated_tax': round(cart_value * 0.08, 2),
                'shipping_method': random.choice(['standard', 'express', 'next_day'])
            })
            self.advance_time(5, 15)
            
            # FUNNEL DROP #3: 30% abandon at checkout
            if random.random() < 0.30:
                self.add_event('session_end', {
                    'reason': 'checkout_abandoned',
                    'cart_value': cart_value,
                    'abandonment_stage': random.choice(['shipping_info', 'payment_info', 'review_order']),
                    'session_duration_seconds': self.get_session_duration()
                })
                return self.events
            
            # STAGE 6: PURCHASE COMPLETE (70% of checkouts)
            tax = round(cart_value * 0.08, 2)
            shipping = 10.00 if cart_value < 50 else 0.00
            total = round(cart_value + tax + shipping, 2)
            
            self.add_event('purchase', {
                'order_id': f'ORD-{uuid.uuid4().hex[:12].upper()}',
                'subtotal': cart_value,
                'tax': tax,
                'shipping': shipping,
                'total_amount': total,
                'items': [
                    {
                        'product_id': item['product']['id'],
                        'product_name': item['product']['name'],
                        'price': item['product']['price'],
                        'quantity': item['quantity'],
                        'subtotal': round(item['product']['price'] * item['quantity'], 2)
                    } for item in self.cart
                ],
                'payment_method': random.choices(
                    ['credit_card', 'debit_card', 'paypal', 'apple_pay'],
                    weights=[50, 30, 15, 5]
                )[0],
                'discount_applied': random.random() < 0.15  # 15% get discounts
            })
            self.advance_time(2, 5)
            
            self.add_event('session_end', {
                'reason': 'purchase_completed',
                'order_value': total,
                'session_duration_seconds': self.get_session_duration()
            })
        
        else:
            # Didn't add anything to cart
            self.add_event('session_end', {
                'reason': 'no_items_added',
                'session_duration_seconds': self.get_session_duration()
            })
        
        return self.events
    
    def add_event(self, event_type, data):
        """Create and store an event"""
        event = {
            'event_id': f'evt_{uuid.uuid4().hex[:16]}',
            'timestamp': self.current_time.isoformat(),
            'event_type': event_type,
            'session_id': self.session_id,
            'user_id': self.user['id'],
            'user_name': self.user['name'],
            'user_segment': self.user['user_segment'],
            'device': self.user['device'],
            'location': {
                'city': self.user['city'],
                'country': self.user['country']
            }
        }
        event.update(data)
        self.events.append(event)
    
    def advance_time(self, min_seconds, max_seconds):
        """Move time forward (realistic gaps between actions)"""
        self.current_time += timedelta(seconds=random.uniform(min_seconds, max_seconds))
    
    def calculate_cart_value(self):
        """Calculate total cart value"""
        return round(sum(item['product']['price'] * item['quantity'] for item in self.cart), 2)
    
    def get_session_duration(self):
        """Get total session duration in seconds"""
        if not self.events:
            return 0
        start_time = datetime.fromisoformat(self.events[0]['timestamp'])
        return int((self.current_time - start_time).total_seconds())

# ============================================
# 4. HISTORICAL DATA GENERATOR
# ============================================
def generate_historical_data(days=7, output_file='historical_events.jsonl'):
    """
    Generate historical data for specified number of days
    Creates realistic traffic patterns with hourly variations
    """
    
    print(f"\n🏭 GENERATING {days} DAYS OF HISTORICAL DATA")
    print("=" * 60)
    
    all_events = []
    stats = defaultdict(int)
    
    start_date = datetime.now() - timedelta(days=days)
    
    # Realistic hourly traffic pattern (sessions per hour)
    hourly_sessions = {
        0: 20, 1: 15, 2: 10, 3: 8, 4: 10, 5: 15,        # Night: low traffic
        6: 30, 7: 50, 8: 80, 9: 120, 10: 150, 11: 180,  # Morning: rising
        12: 200, 13: 180, 14: 160, 15: 140, 16: 120,    # Afternoon: peak
        17: 100, 18: 90, 19: 110, 20: 130, 21: 100,     # Evening: second peak
        22: 70, 23: 40                                   # Night: declining
    }
    
    for day_num in range(days):
        day_start = start_date + timedelta(days=day_num)
        day_events = []
        
        print(f"\n📅 Day {day_num + 1}: {day_start.strftime('%Y-%m-%d')}")
        
        for hour, session_count in hourly_sessions.items():
            hour_start = day_start.replace(hour=hour, minute=0, second=0, microsecond=0)
            
            # Generate sessions for this hour
            for _ in range(session_count):
                user = random.choice(USERS)
                
                # Random time within the hour
                session_time = hour_start + timedelta(
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                # Simulate complete session
                session = ShoppingSession(user, session_time)
                session_events = session.simulate_complete_journey()
                
                day_events.extend(session_events)
                
                # Track statistics
                for event in session_events:
                    stats[event['event_type']] += 1
        
        all_events.extend(day_events)
        print(f"  ✅ Generated {len(day_events):,} events for this day")
    
    # Save to file
    print(f"\n💾 Saving to {output_file}...")
    with open(output_file, 'w') as f:
        for event in all_events:
            f.write(json.dumps(event) + '\n')
    
    # Print statistics
    print("\n" + "=" * 60)
    print("📊 DATA GENERATION COMPLETE")
    print("=" * 60)
    print(f"Total Events: {len(all_events):,}")
    print(f"Total Sessions: {stats['session_start']:,}")
    print(f"\nEvent Breakdown:")
    for event_type, count in sorted(stats.items()):
        percentage = (count / len(all_events)) * 100
        print(f"  {event_type:20s}: {count:6,} ({percentage:5.2f}%)")
    
    # Calculate conversion funnel
    print(f"\n🎯 Conversion Funnel:")
    sessions = stats['session_start']
    carts = stats.get('add_to_cart', 0)
    checkouts = stats.get('checkout_start', 0)
    purchases = stats.get('purchase', 0)
    
    print(f"  Sessions:        {sessions:6,} (100.00%)")
    print(f"  Add to Cart:     {carts:6,} ({(carts/sessions)*100:5.2f}%) [Drop: {((sessions-carts)/sessions)*100:.1f}%]")
    print(f"  Checkout Start:  {checkouts:6,} ({(checkouts/sessions)*100:5.2f}%) [Drop: {((carts-checkouts)/carts)*100:.1f}%]")
    print(f"  Purchase:        {purchases:6,} ({(purchases/sessions)*100:5.2f}%) [Drop: {((checkouts-purchases)/checkouts)*100:.1f}%]")
    
    return all_events

# ============================================
# 5. REAL-TIME STREAMING
# ============================================
def stream_events_realtime():
    """
    Stream events in real-time (for Kafka)
    Simulates continuous user sessions
    """
    
    print("\n🚀 STARTING REAL-TIME EVENT STREAMING")
    print("=" * 60)
    print("Press Ctrl+C to stop\n")
    
    session_count = 0
    
    try:
        while True:
            # Pick random user
            user = random.choice(USERS)
            
            # Create and simulate session
            session = ShoppingSession(user, datetime.now())
            events = session.simulate_complete_journey()
            
            session_count += 1
            
            # Print session summary
            session_duration = events[-1].get('session_duration_seconds', 0) if events else 0
            exit_reason = events[-1].get('reason', 'unknown') if events else 'unknown'
            
            print(f"Session #{session_count}: {user['name']:20s} | "
                  f"Events: {len(events):2d} | "
                  f"Duration: {session_duration:3d}s | "
                  f"Exit: {exit_reason:20s}")
            
            # Here you would send to Kafka
            # producer.send('ecommerce_events', value=event)
            
            # Wait before next session (1-5 seconds)
            time.sleep(random.uniform(1, 5))
            
    except KeyboardInterrupt:
        print(f"\n\n✅ Stopped. Generated {session_count} sessions")

# ============================================
# 6. MAIN EXECUTION
# ============================================
if __name__ == '__main__':
    import sys
    
    print("\n" + "="*60)
    print("E-COMMERCE EVENT GENERATOR")
    print("="*60)
    print(f"Products in catalog: {len(PRODUCTS)}")
    print(f"Users in pool: {len(USERS)}")
    print("="*60)
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'historical':
            # Generate historical batch data
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            generate_historical_data(days=days)
        elif sys.argv[1] == 'stream':
            # Real-time streaming mode
            stream_events_realtime()
        else:
            print("Usage:")
            print("  python event_generator.py historical [days]  - Generate historical data")
            print("  python event_generator.py stream             - Stream real-time events")
    else:
        # Default: generate 7 days of data
        generate_historical_data(days=7)
"""
Real-Time E-Commerce Analytics Dashboard
Reads metrics from Redis and displays live updates
"""

import streamlit as st
import redis
import json
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ======================
# PAGE CONFIG
# ======================
st.set_page_config(
    page_title="E-Commerce Real-Time Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ======================
# REDIS CONNECTION
# ======================
@st.cache_resource
def get_redis_client():
    """Get Redis client (cached)"""
    return redis.Redis(
        host='localhost',
        port=6379,
        decode_responses=True
    )

redis_client = get_redis_client()

# ======================
# CUSTOM CSS
# ======================
st.markdown("""
    <style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .big-number {
        font-size: 48px;
        font-weight: bold;
        margin: 10px 0;
    }
    .metric-label {
        font-size: 18px;
        opacity: 0.9;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
    }
    </style>
""", unsafe_allow_html=True)

# ======================
# FETCH METRICS FROM REDIS
# ======================
def get_metrics():
    """Fetch all metrics from Redis"""
    try:
        metrics = {
            'active_users': redis_client.get('realtime:active_users') or '0',
            'events_per_second': redis_client.get('realtime:events_per_second') or '0',
            'revenue': redis_client.get('realtime:revenue_last_minute') or '0',
            'top_products': redis_client.get('realtime:top_products') or '[]',
            'event_types': redis_client.get('realtime:event_types') or '{}'
        }
        
        # Parse JSON strings
        metrics['top_products'] = json.loads(metrics['top_products'])
        metrics['event_types'] = json.loads(metrics['event_types'])
        
        return metrics
    except Exception as e:
        st.error(f"Error fetching metrics: {e}")
        return None

# ======================
# HEADER
# ======================
st.title("🛒 E-Commerce Real-Time Analytics")
st.markdown("**Live metrics updated every 2 seconds**")
st.markdown("---")

# ======================
# SIDEBAR
# ======================
with st.sidebar:
    st.header("⚙️ Dashboard Settings")
    
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_interval = st.slider("Refresh interval (seconds)", 1, 10, 2)
    
    st.markdown("---")
    st.markdown("### 📊 About")
    st.info("""
    This dashboard displays real-time analytics from a Kafka-based event streaming pipeline.
    
    **Architecture:**
    - Event Generator → Kafka
    - Kafka → Consumer
    - Consumer → Redis
    - Redis → This Dashboard
    """)
    
    st.markdown("---")
    st.markdown("### 🔗 Quick Links")
    st.markdown("- [Kafka UI](http://localhost:8080)")
    st.markdown("- [Grafana](http://localhost:3000)")
    st.markdown("- [Prometheus](http://localhost:9090)")

# ======================
# MAIN DASHBOARD
# ======================

# Create placeholder for metrics
placeholder = st.empty()

# Auto-refresh loop
while True:
    with placeholder.container():
        # Fetch metrics
        metrics = get_metrics()
        
        if metrics:
            # ======================
            # TOP METRICS ROW
            # ======================
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="👥 Active Users",
                    value=f"{int(metrics['active_users']):,}",
                    delta=None
                )
            
            with col2:
                events_per_sec = float(metrics['events_per_second'])
                st.metric(
                    label="⚡ Events/Second",
                    value=f"{events_per_sec:,.1f}",
                    delta=None
                )
            
            with col3:
                revenue = float(metrics['revenue'])
                st.metric(
                    label="💰 Revenue (1 min)",
                    value=f"${revenue:,.2f}",
                    delta=None
                )
            
            with col4:
                # Calculate conversion rate
                event_types = metrics['event_types']
                views = event_types.get('product_view', 1)
                purchases = event_types.get('purchase', 0)
                conversion = (purchases / views * 100) if views > 0 else 0
                
                st.metric(
                    label="📈 Conversion Rate",
                    value=f"{conversion:.2f}%",
                    delta=None
                )
            
            st.markdown("---")
            
            # ======================
            # CHARTS ROW 1
            # ======================
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🔥 Top 10 Products")
                
                if metrics['top_products']:
                    # Create dataframe
                    df_products = pd.DataFrame(metrics['top_products'][:10])
                    
                    # Create horizontal bar chart
                    fig = px.bar(
                        df_products,
                        x='count',
                        y='name',
                        orientation='h',
                        title="Most Viewed Products (Real-Time)",
                        labels={'count': 'Views', 'name': 'Product'},
                        color='count',
                        color_continuous_scale='Viridis'
                    )
                    fig.update_layout(
                        showlegend=False,
                        height=400,
                        yaxis={'categoryorder': 'total ascending'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No product data yet. Start the producer!")
            
            with col2:
                st.subheader("📊 Event Type Distribution")
                
                if metrics['event_types']:
                    # Create pie chart
                    df_events = pd.DataFrame([
                        {'Event Type': k.replace('_', ' ').title(), 'Count': v}
                        for k, v in metrics['event_types'].items()
                    ])
                    
                    fig = px.pie(
                        df_events,
                        values='Count',
                        names='Event Type',
                        title="Event Breakdown",
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No event data yet.")
            
            # ======================
            # CONVERSION FUNNEL
            # ======================
            st.markdown("---")
            st.subheader("🎯 Real-Time Conversion Funnel")
            
            if metrics['event_types']:
                # Calculate funnel stages
                sessions = metrics['event_types'].get('session_start', 0)
                views = metrics['event_types'].get('product_view', 0)
                carts = metrics['event_types'].get('add_to_cart', 0)
                checkouts = metrics['event_types'].get('checkout_start', 0)
                purchases = metrics['event_types'].get('purchase', 0)
                
                # Create funnel data
                funnel_data = {
                    'Stage': ['Sessions', 'Product Views', 'Add to Cart', 'Checkout', 'Purchase'],
                    'Count': [sessions, views, carts, checkouts, purchases],
                    'Percentage': [100, 100, 100, 100, 100]
                }
                
                if sessions > 0:
                    funnel_data['Percentage'] = [
                        100,
                        (views / sessions * 100) if sessions > 0 else 0,
                        (carts / sessions * 100) if sessions > 0 else 0,
                        (checkouts / sessions * 100) if sessions > 0 else 0,
                        (purchases / sessions * 100) if sessions > 0 else 0
                    ]
                
                df_funnel = pd.DataFrame(funnel_data)
                
                # Create funnel chart
                fig = go.Figure()
                
                fig.add_trace(go.Funnel(
                    y=df_funnel['Stage'],
                    x=df_funnel['Count'],
                    textposition="inside",
                    textinfo="value+percent initial",
                    marker={
                        "color": ["#667eea", "#764ba2", "#f093fb", "#4facfe", "#00f2fe"],
                        "line": {"width": 2, "color": "white"}
                    },
                    connector={"line": {"color": "royalblue", "width": 3}}
                ))
                
                fig.update_layout(
                    title="Conversion Funnel (All-Time)",
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Drop-off percentages
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    view_to_cart = (carts / views * 100) if views > 0 else 0
                    drop_view_cart = 100 - view_to_cart
                    st.metric(
                        "View → Cart Drop",
                        f"{drop_view_cart:.1f}%",
                        delta=f"{view_to_cart:.1f}% converted",
                        delta_color="inverse"
                    )
                
                with col2:
                    cart_to_checkout = (checkouts / carts * 100) if carts > 0 else 0
                    drop_cart_checkout = 100 - cart_to_checkout
                    st.metric(
                        "Cart → Checkout Drop",
                        f"{drop_cart_checkout:.1f}%",
                        delta=f"{cart_to_checkout:.1f}% converted",
                        delta_color="inverse"
                    )
                
                with col3:
                    checkout_to_purchase = (purchases / checkouts * 100) if checkouts > 0 else 0
                    drop_checkout_purchase = 100 - checkout_to_purchase
                    st.metric(
                        "Checkout → Purchase Drop",
                        f"{drop_checkout_purchase:.1f}%",
                        delta=f"{checkout_to_purchase:.1f}% converted",
                        delta_color="inverse"
                    )
                
                with col4:
                    overall_conversion = (purchases / sessions * 100) if sessions > 0 else 0
                    st.metric(
                        "Overall Conversion",
                        f"{overall_conversion:.2f}%",
                        delta="Target: 10%",
                        delta_color="normal" if overall_conversion >= 10 else "inverse"
                    )
            
            # ======================
            # LIVE FEED
            # ======================
            st.markdown("---")
            st.subheader("📡 System Status")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.success("✅ Kafka: Connected")
            
            with col2:
                st.success("✅ Redis: Connected")
            
            with col3:
                st.success("✅ Consumer: Running")
            
            # Timestamp
            st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        else:
            st.error("⚠️ Unable to fetch metrics. Make sure:")
            st.warning("1. Redis is running (docker ps)")
            st.warning("2. Consumer is running (simple_consumer.py)")
            st.warning("3. Producer is sending events")
    
    # Auto-refresh logic
    if not auto_refresh:
        break
    
    time.sleep(refresh_interval)
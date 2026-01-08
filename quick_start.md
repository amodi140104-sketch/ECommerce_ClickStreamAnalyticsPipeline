# 1. Start infrastructure
docker-compose up -d
# 2. Generate data
cd 1_data_generation
python event_generator.py historical 7
# 3. Setup PostgreSQL
cd ..
docker exec -i postgres psql -U ecommerce -d ecommerce_analytics < postgres_schema.sql

# 4. Run pipeline (open 4 terminals)
# Terminal 1: python 2_ingestion/kafka_producer.py live
# Terminal 2: python 3_streaming_process/simple_consumer.py
# Terminal 3: python 4_batch_processing/batch_processor.py
# Terminal 4: streamlit run 7_dashboard/streamlit_dashboard.py

# 5. Open browser: http://localhost:8501

Project Structure
C:\Your\Path\ecommerce-analytics\
├── data_generation/
│   └── event_generator.py
│
├── ingestion/
│   └── kafka_producer.py
│cd 
├── streaming_process/
│   └── simple_consumer.py
│
├── 4_batch_processing/
│   └── batch_processor.py
│
├── 7_dashboard/
│   └── streamlit_dashboard.py
├── docker-compose.yml
├── postgres_schema.sql
└── requirements.txt


### Generate Historical Data

cd 1_data_generation

# Generate 7 days of historical data
python event_generator.py historical 7

# You should see:
# ✅ Generated 105,234 events
# 💾 Saved to historical_events.jsonl
```

**Output file:** `data_generation/historical_events.jsonl` (~50MB)

---

### STEP 4: Setup PostgreSQL Database

```bash
# Go back to root directory
cd ..

# Initialize database schema
docker exec -i postgres psql -U ecommerce -d ecommerce_analytics < postgres_schema.sql

# Verify tables were created
docker exec -it postgres psql -U ecommerce -d ecommerce_analytics -c "\dt"
```

**Expected output:**
```
              List of relations
 Schema |          Name           | Type  |   Owner
--------+-------------------------+-------+-----------
 public | raw_events              | table | ecommerce
 public | daily_user_metrics      | table | ecommerce
 public | daily_product_metrics   | table | ecommerce
 ...
```

---

### STEP 5: Run the Pipeline

**Open 4 separate terminals:**

#### Terminal 1: Kafka Producer (Generates Events)

```bash
cd 2_ingestion
python kafka_producer.py live

#### Terminal 2: Stream Processor (Real-Time Analytics)

cd 3_streaming_process
python simple_consumer.py

#### Terminal 3: Batch Processor (PostgreSQL Storage)

```bash
cd 4_batch_processing
python batch_processor.py
```

#### Terminal 4: Dashboard (Visualization)

```bash
cd dashboard
streamlit run streamlit_dashboard.py
```

```bash
docker exec -it postgres psql -U ecommerce -d ecommerce_analytics
```

**Query 1: Today's summary**
```sql
SELECT * FROM v_today_summary;
```

**Query 2: Top products**
```sql
SELECT * FROM v_top_products LIMIT 10;
```

**Query 3: Conversion funnel**
```sql
SELECT * FROM calculate_conversion_rate(CURRENT_DATE);
```
### Export PostgreSQL Data
```bash
docker exec postgres pg_dump -U ecommerce ecommerce_analytics > backup.sql
```
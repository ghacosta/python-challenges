# Memory-Efficient Data Pipeline

A high-performance, memory-efficient data processing pipeline designed to handle streaming JSON data from webhooks with constant memory usage regardless of input volume.

## 📋 Stack

- Python 3.8+
- Redis server (optional, for message queue functionality)
- SQLite (included with Python)

## 🛠️ Installation & Quick Start

0. (Optional) create venv:
```bash
python3 -m venv venv
source venv/bin/activate
```

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start Redis server:
```bash
redis-server
```

3. Start the data pipeline:
```bash
python data_pipeline.py
```
The pipeline will start listening for webhooks on `http://localhost:8080/webhook`

4. Send test data:
```bash
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d '{"user_id": "123", "event_type": "click", "value": 1.5}'
```

5. Run tests:
```bash
python test_data_generator.py
```

## 📊 Testing

The `test_data_generator.py` provides several testing scenarios:

- **Steady Stream**: Consistent message rate for sustained testing
- **Burst Traffic**: High-volume bursts to test resilience
- **Variable Rate**: Real-world simulation with changing rates
- **Single Message**: Quick functionality verification
- **Custom Test**: User-defined parameters

## 🏗️ Architecture

### Core Components

1. **DataPipeline**: Main orchestrator managing the entire data flow
2. **DataTransformer**: Handles JSON parsing and windowed aggregation
3. **OutputSink**: Abstract interface with concrete implementations:
   - `DatabaseSink`: SQLite persistence
   - `MessageQueueSink`: Redis queue integration
4. **WebhookServer**: HTTP server for receiving JSON data

### Data Flow

```
Webhook → Input Queue → JSON Parser → Window Aggregator → Output Sinks
                                                        ├── Database
                                                        └── Message Queue
```
## 🔧 Configuration

### Pipeline Settings
```python
pipeline = DataPipeline(
    window_size=60,        # Aggregation window in seconds
    max_queue_size=1000    # Maximum input queue size
)
```

### Database Configuration
```python
db_sink = DatabaseSink(db_path="pipeline_data.db")
```

### Message Queue Configuration
```python
mq_sink = MessageQueueSink(
    redis_url="redis://localhost:6379",
    queue_name="pipeline_output"
)
```

## 📝 Data Format

### Input Format
```json
{
    "user_id": "user_123",
    "event_type": "click",
    "value": 1.5,
    "timestamp": 1640995200.0,
    "metadata": {
        "source": "web",
        "session_id": "session_456"
    }
}
```

### Output Format
```json
{
    "window_start": 1640995200.0,
    "window_end": 1640995260.0,
    "event_type": "click",
    "count": 150,
    "sum_value": 225.0,
    "avg_value": 1.5,
    "unique_users": 75
}
```

## 🔍 Monitoring

### Health Check
```bash
curl http://localhost:8080/health
```

### Database Queries
```sql
-- View recent aggregations
SELECT * FROM aggregated_data 
ORDER BY created_at DESC 
LIMIT 10;

-- Event type summary
SELECT event_type, COUNT(*) as windows, AVG(avg_value) as overall_avg
FROM aggregated_data 
GROUP BY event_type;
```

### Redis Queue Monitoring
```bash
redis-cli LLEN pipeline_output
redis-cli LRANGE pipeline_output 0 -1
```

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, Any, AsyncGenerator, Optional, List
from queue import Queue
import threading
import sqlite3
import aiohttp
from aiohttp import web
import redis.asyncio as redis


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DataPoint:
    """Represents a single data point in the pipeline."""
    timestamp: float
    user_id: str
    event_type: str
    value: float
    metadata: Dict[str, Any]


@dataclass
class AggregatedData:
    """Represents aggregated data output."""
    window_start: float
    window_end: float
    event_type: str
    count: int
    sum_value: float
    avg_value: float
    unique_users: int


class DataTransformer:
    """Handles data transformation using generators for memory efficiency."""
    
    def __init__(self, window_size: int = 60):
        self.window_size = window_size  # seconds
        
    async def parse_json_stream(self, json_stream: AsyncGenerator[str, None]) -> AsyncGenerator[DataPoint, None]:
        """Parse JSON strings into DataPoint objects."""
        async for json_str in json_stream:
            try:
                data = json.loads(json_str)
                yield DataPoint(
                    timestamp=data.get('timestamp', time.time()),
                    user_id=data['user_id'],
                    event_type=data['event_type'],
                    value=float(data.get('value', 0)),
                    metadata=data.get('metadata', {})
                )
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"Error parsing JSON: {e}")
                continue
    
    async def windowed_aggregator(self, data_stream: AsyncGenerator[DataPoint, None]) -> AsyncGenerator[AggregatedData, None]:
        """Aggregate data in time windows using sliding window approach."""
        window_data = deque()
        current_window_start = None
        
        async for point in data_stream:
            current_time = point.timestamp
            
            # Initialize window if needed
            if current_window_start is None:
                current_window_start = current_time
            
            # Add point to current window
            window_data.append(point)
            
            # Remove old points outside the window
            window_end = current_window_start + self.window_size
            while window_data and window_data[0].timestamp < current_window_start:
                window_data.popleft()
            
            # Check if we should emit aggregated data
            if current_time >= window_end:
                if window_data:
                    # Group by event_type for aggregation
                    event_groups = {}
                    for point in window_data:
                        if point.event_type not in event_groups:
                            event_groups[point.event_type] = []
                        event_groups[point.event_type].append(point)
                    
                    # Emit aggregated data for each event type
                    for event_type, points in event_groups.items():
                        values = [p.value for p in points]
                        unique_users = len(set(p.user_id for p in points))
                        
                        yield AggregatedData(
                            window_start=current_window_start,
                            window_end=window_end,
                            event_type=event_type,
                            count=len(points),
                            sum_value=sum(values),
                            avg_value=sum(values) / len(values) if values else 0,
                            unique_users=unique_users
                        )
                
                # Move to next window
                current_window_start = window_end
                
                # Clear processed data
                window_data = deque([p for p in window_data if p.timestamp >= current_window_start])


class OutputSink(ABC):
    """Abstract base class for output sinks."""
    
    @abstractmethod
    async def write(self, data: AggregatedData) -> None:
        pass
    
    @abstractmethod
    async def close(self) -> None:
        pass


class DatabaseSink(OutputSink):
    """SQLite database sink for persistent storage."""
    
    def __init__(self, db_path: str = "pipeline_data.db"):
        self.db_path = db_path
        self.connection = None
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS aggregated_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start REAL,
                window_end REAL,
                event_type TEXT,
                count INTEGER,
                sum_value REAL,
                avg_value REAL,
                unique_users INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
    
    async def write(self, data: AggregatedData) -> None:
        """Write aggregated data to database."""
        def _write_sync():
            conn = sqlite3.connect(self.db_path)
            conn.execute('''
                INSERT INTO aggregated_data 
                (window_start, window_end, event_type, count, sum_value, avg_value, unique_users)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.window_start, data.window_end, data.event_type,
                data.count, data.sum_value, data.avg_value, data.unique_users
            ))
            conn.commit()
            conn.close()
        
        # Run database operation in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _write_sync)
    
    async def close(self) -> None:
        """Clean up database resources."""
        pass


class MessageQueueSink(OutputSink):
    """Redis message queue sink for real-time processing."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", queue_name: str = "pipeline_output"):
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.redis_client = None
    
    async def _get_redis_client(self):
        """Lazy initialization of Redis client."""
        if self.redis_client is None:
            self.redis_client = redis.from_url(self.redis_url)
        return self.redis_client
    
    async def write(self, data: AggregatedData) -> None:
        """Write aggregated data to message queue."""
        try:
            client = await self._get_redis_client()
            message = json.dumps(asdict(data))
            await client.lpush(self.queue_name, message)
        except Exception as e:
            logger.error(f"Error writing to message queue: {e}")
    
    async def close(self) -> None:
        """Clean up Redis connection."""
        if self.redis_client:
            await self.redis_client.close()


class DataPipeline:
    """Main data pipeline orchestrator."""
    
    def __init__(self, window_size: int = 60, max_queue_size: int = 1000):
        self.window_size = window_size
        self.max_queue_size = max_queue_size
        self.transformer = DataTransformer(window_size)
        self.input_queue = asyncio.Queue(maxsize=max_queue_size)
        self.output_sinks = []
        self.is_running = False
        self._processing_task = None
    
    def add_output_sink(self, sink: OutputSink):
        """Add an output sink to the pipeline."""
        self.output_sinks.append(sink)
    
    async def enqueue_data(self, json_data: str) -> None:
        """Add data to the processing queue."""
        try:
            await self.input_queue.put(json_data)
        except asyncio.QueueFull:
            logger.warning("Input queue full, dropping message")
    
    async def _json_stream_generator(self) -> AsyncGenerator[str, None]:
        """Generate JSON strings from the input queue."""
        while self.is_running:
            try:
                # Use timeout to allow periodic checks of is_running
                json_data = await asyncio.wait_for(
                    self.input_queue.get(), timeout=1.0
                )
                yield json_data
                self.input_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in JSON stream generator: {e}")
                break
    
    async def _process_data(self) -> None:
        """Main data processing loop."""
        json_stream = self._json_stream_generator()
        data_stream = self.transformer.parse_json_stream(json_stream)
        aggregated_stream = self.transformer.windowed_aggregator(data_stream)
        
        async for aggregated_data in aggregated_stream:
            # Write to all output sinks concurrently
            write_tasks = [
                sink.write(aggregated_data) for sink in self.output_sinks
            ]
            await asyncio.gather(*write_tasks, return_exceptions=True)
            
            logger.info(f"Processed aggregated data: {aggregated_data.event_type}, "
                       f"count: {aggregated_data.count}, avg: {aggregated_data.avg_value:.2f}")
    
    async def start(self) -> None:
        """Start the data pipeline."""
        self.is_running = True
        self._processing_task = asyncio.create_task(self._process_data())
        logger.info("Data pipeline started")
    
    async def stop(self) -> None:
        """Stop the data pipeline."""
        self.is_running = False
        
        if self._processing_task:
            await self._processing_task
        
        # Close all output sinks
        close_tasks = [sink.close() for sink in self.output_sinks]
        await asyncio.gather(*close_tasks, return_exceptions=True)
        
        logger.info("Data pipeline stopped")


class WebhookServer:
    """HTTP server to receive webhook data."""
    
    def __init__(self, pipeline: DataPipeline, host: str = "localhost", port: int = 8080):
        self.pipeline = pipeline
        self.host = host
        self.port = port
        self.app = web.Application()
        self.app.router.add_post('/webhook', self.handle_webhook)
        self.app.router.add_get('/health', self.health_check)
    
    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhook requests."""
        try:
            json_data = await request.text()
            await self.pipeline.enqueue_data(json_data)
            return web.Response(status=200, text="OK")
        except Exception as e:
            logger.error(f"Error handling webhook: {e}")
            return web.Response(status=500, text="Internal Server Error")
    
    async def health_check(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.Response(status=200, text="Healthy")
    
    async def start(self) -> None:
        """Start the webhook server."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info(f"Webhook server started on {self.host}:{self.port}")


# Example usage and main function
async def main():
    """Main function to demonstrate the pipeline."""
    
    # Create pipeline with 30-second windows
    pipeline = DataPipeline(window_size=30, max_queue_size=1000)
    
    # Add output sinks
    db_sink = DatabaseSink("pipeline_data.db")
    pipeline.add_output_sink(db_sink)
    
    # Try to add Redis sink if available
    try:
        mq_sink = MessageQueueSink("redis://localhost:6379")
        # Test Redis connection
        import redis.asyncio as redis
        test_redis = redis.from_url("redis://localhost:6379")
        await test_redis.ping()
        await test_redis.close()
        
        pipeline.add_output_sink(mq_sink)
        logger.info("Redis message queue sink added successfully")
    except Exception as e:
        logger.warning(f"Redis not available, running with database sink only: {e}")
    
    # Start pipeline
    await pipeline.start()
    
    # Start webhook server
    server = WebhookServer(pipeline, host="localhost", port=8080)
    await server.start()
    
    logger.info("Pipeline and server started. Send POST requests to http://localhost:8080/webhook")
    logger.info("Example payload: {\"user_id\": \"123\", \"event_type\": \"click\", \"value\": 1.5}")
    
    try:
        # Keep running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(main())


# Test data generator has been moved to test_data_generator.py
# Run: python test_data_generator.py
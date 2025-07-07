import logging
import time
from contextlib import contextmanager
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ResourceType(Enum):
    DATABASE = "database"
    API = "api"
    FILE = "file"

@dataclass
class ResourceMetrics:
    connect_time: float = 0.0
    operation_time: float = 0.0
    cleanup_time: float = 0.0
    success: bool = True
    error: Optional[str] = None

class ResourceManager:
    
    def __init__(self, name: str):
        self.name = name
        self.resources: Dict[str, Any] = {}
        self.metrics: Dict[str, ResourceMetrics] = {}
        self.start_time: Optional[float] = None
        self.nested_managers: list = []
    
    def __enter__(self):
        self.start_time = time.time()
        logger.info(f"🔄 Starting resource manager: {self.name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        total_time = time.time() - self.start_time if self.start_time else 0
        
        if exc_type:
            logger.error(f"❌ Exception in {self.name}: {exc_type.__name__}: {exc_val}")
            self._mark_all_failed(str(exc_val))
        
        self._cleanup_all_resources()
        
        self._log_final_metrics(total_time)
        
        return False
    
    def acquire_resource(self, resource_id: str, resource_type: ResourceType, config: Dict[str, Any] = None):
        start_time = time.time()
        metrics = ResourceMetrics()
        
        try:
            logger.info(f"🔗 Acquiring {resource_type.value} resource: {resource_id}")
            
            # Simulate resource acquisition based on type
            resource = self._create_resource(resource_type, config or {})
            
            metrics.connect_time = time.time() - start_time
            self.resources[resource_id] = resource
            self.metrics[resource_id] = metrics
            
            logger.info(f"✅ Successfully acquired {resource_id} in {metrics.connect_time:.3f}s")
            return resource
            
        except Exception as e:
            metrics.connect_time = time.time() - start_time
            metrics.success = False
            metrics.error = str(e)
            self.metrics[resource_id] = metrics
            
            logger.error(f"❌ Failed to acquire {resource_id}: {e}")
            raise
    
    def _create_resource(self, resource_type: ResourceType, config: Dict[str, Any]):
        # Simulate connection time
        time.sleep(0.1)
        
        if resource_type == ResourceType.DATABASE:
            return MockDatabase(config.get('host', 'localhost'), config.get('port', 5432))
        elif resource_type == ResourceType.API:
            return MockAPI(config.get('base_url', 'https://api.example.com'))
        elif resource_type == ResourceType.FILE:
            return MockFile(config.get('path', '/tmp/test.txt'))
        else:
            raise ValueError(f"Unknown resource type: {resource_type}")
    
    def _cleanup_all_resources(self):
        # Reverse cleanup to ensure dependencies are handled correctly
        for resource_id in reversed(list(self.resources.keys())):
            self._cleanup_resource(resource_id)
    
    def _cleanup_resource(self, resource_id: str):
        if resource_id not in self.resources:
            return
        
        start_time = time.time()
        try:
            logger.info(f"🧹 Cleaning up resource: {resource_id}")
            resource = self.resources[resource_id]
            
            # Call cleanup method if it exists
            if hasattr(resource, 'close'):
                resource.close()
            
            cleanup_time = time.time() - start_time
            self.metrics[resource_id].cleanup_time = cleanup_time
            
            logger.info(f"✅ Cleaned up {resource_id} in {cleanup_time:.3f}s")
            
        except Exception as e:
            cleanup_time = time.time() - start_time
            self.metrics[resource_id].cleanup_time = cleanup_time
            self.metrics[resource_id].error = str(e)
            logger.error(f"❌ Error cleaning up {resource_id}: {e}")
        
        finally:
            del self.resources[resource_id]
    
    def _mark_all_failed(self, error: str):
        for metrics in self.metrics.values():
            metrics.success = False
            if not metrics.error:
                metrics.error = error
    
    def _log_final_metrics(self, total_time: float):
        logger.info(f"📊 Final metrics for {self.name} (Total: {total_time:.3f}s)")
        
        for resource_id, metrics in self.metrics.items():
            status = "✅ SUCCESS" if metrics.success else "❌ FAILED"
            logger.info(f"  {resource_id}: {status}")
            logger.info(f"    Connect: {metrics.connect_time:.3f}s")
            logger.info(f"    Cleanup: {metrics.cleanup_time:.3f}s")
            if metrics.error:
                logger.info(f"    Error: {metrics.error}")
    
    # Support for nested context managers
    def nested_context(self, name: str):
        nested = ResourceManager(f"{self.name}.{name}")
        self.nested_managers.append(nested)
        return nested

# Mock resource classes for demonstration
class MockDatabase:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connected = True
    
    def query(self, sql: str):
        if not self.connected:
            raise RuntimeError("Database not connected")
        return f"Result for: {sql}"
    
    def close(self):
        self.connected = False

class MockAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session_active = True
    
    def get(self, endpoint: str):
        if not self.session_active:
            raise RuntimeError("API session not active")
        return {"data": f"Response from {endpoint}"}
    
    def close(self):
        self.session_active = False

class MockFile:
    def __init__(self, path: str):
        self.path = path
        self.handle = f"file_handle_{path}"
    
    def read(self):
        return f"Content from {self.path}"
    
    def close(self):
        self.handle = None

@contextmanager
def resource_manager(name: str):
    manager = ResourceManager(name)
    try:
        yield manager
    finally:
        pass  # Cleanup handled by ResourceManager.__exit__

# Example usage and demonstration
if __name__ == "__main__":
    # Example 1: Basic usage
    print("=== Example 1: Basic Resource Management ===")
    with ResourceManager("DataPipeline") as rm:
        # Acquire multiple resources
        db = rm.acquire_resource("primary_db", ResourceType.DATABASE, 
                                {"host": "prod-db", "port": 5432})
        api = rm.acquire_resource("weather_api", ResourceType.API, 
                                 {"base_url": "https://weather.api.com"})
        file_handle = rm.acquire_resource("config_file", ResourceType.FILE, 
                                        {"path": "/config/app.json"})
        
        # Use resources
        print(f"DB Query: {db.query('SELECT * FROM users')}")
        print(f"API Call: {api.get('/weather/current')}")
        print(f"File Read: {file_handle.read()}")
    
    print("\n=== Example 2: Exception Handling ===")
    try:
        with ResourceManager("ErrorDemo") as rm:
            db = rm.acquire_resource("test_db", ResourceType.DATABASE)
            # Simulate an error
            raise ValueError("Simulated processing error")
    except ValueError as e:
        print(f"Caught expected error: {e}")
    
    print("\n=== Example 3: Nested Context Managers ===")
    with ResourceManager("MainPipeline") as main_rm:
        main_db = main_rm.acquire_resource("main_db", ResourceType.DATABASE)
        
        # Nested context for a sub-operation
        with main_rm.nested_context("DataProcessing") as nested_rm:
            temp_file = nested_rm.acquire_resource("temp_file", ResourceType.FILE)
            processing_api = nested_rm.acquire_resource("processing_api", ResourceType.API)
            
            print("Performing nested operations...")
            print(f"Main DB: {main_db.query('SELECT count(*) FROM main_table')}")
            print(f"Temp File: {temp_file.read()}")
    
    print("\n=== Example 4: Function-based Context Manager ===")
    with resource_manager("SimpleDemo") as rm:
        api = rm.acquire_resource("simple_api", ResourceType.API)
        print(f"Simple API call: {api.get('/status')}")
    
    print("\n✅ All examples completed successfully!")
import multiprocessing as mp
import threading
import time
import queue
from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import uuid

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class Task:
    id: str
    func_name: str
    args: tuple
    priority: int = 1  # Higher number = higher priority
    dependencies: List[str] = None
    timeout: Optional[float] = None
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
    
    def __lt__(self, other):
        return self.priority > other.priority  # Higher priority first

# Global function registry
TASK_FUNCTIONS = {}

def register_task(name: str):
    """Decorator to register task functions"""
    def decorator(func):
        TASK_FUNCTIONS[name] = func
        return func
    return decorator

def worker_process(task_queue: mp.Queue, result_queue: mp.Queue, stop_event: mp.Event):
    """Worker process that executes tasks"""
    while not stop_event.is_set():
        try:
            # Get task with timeout
            task_data = task_queue.get(timeout=1)
            task = Task(**task_data)
            
            # Update status
            task.status = TaskStatus.RUNNING
            result_queue.put(task.__dict__)
            
            # Execute task
            try:
                func = TASK_FUNCTIONS[task.func_name]
                
                # Handle timeout
                if task.timeout:
                    import signal
                    def timeout_handler(signum, frame):
                        raise TimeoutError(f"Task timed out after {task.timeout}s")
                    
                    signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(int(task.timeout))
                    
                    try:
                        result = func(*task.args)
                        signal.alarm(0)  # Cancel alarm
                    except TimeoutError:
                        task.status = TaskStatus.FAILED
                        task.error = "Timeout"
                        result_queue.put(task.__dict__)
                        continue
                else:
                    result = func(*task.args)
                
                task.status = TaskStatus.COMPLETED
                task.result = result
                
            except Exception as e:
                task.status = TaskStatus.FAILED
                task.error = str(e)
            
            result_queue.put(task.__dict__)
            
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Worker error: {e}")

class DistributedTaskScheduler:
    def __init__(self, num_workers: int = 2):
        self.num_workers = num_workers
        self.tasks: Dict[str, Task] = {}
        self.dependencies: Dict[str, List[str]] = {}  # task_id -> [dependent_task_ids]
        
        # Multiprocessing components
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        self.stop_event = mp.Event()
        self.workers = []
        
        # Thread-safe priority queue
        self.priority_queue = queue.PriorityQueue()
        self.lock = threading.Lock()
        
        # Start workers and result processor
        self._start_workers()
        self._start_result_processor()
        
    def _start_workers(self):
        """Start worker processes"""
        for i in range(self.num_workers):
            worker = mp.Process(
                target=worker_process,
                args=(self.task_queue, self.result_queue, self.stop_event)
            )
            worker.start()
            self.workers.append(worker)
    
    def _start_result_processor(self):
        """Start background thread to process results"""
        def process_results():
            while not self.stop_event.is_set():
                try:
                    result_data = self.result_queue.get(timeout=1)
                    task = Task(**result_data)
                    
                    with self.lock:
                        self.tasks[task.id] = task
                        
                        # If task completed, check dependencies
                        if task.status == TaskStatus.COMPLETED:
                            self._check_dependencies(task.id)
                
                except queue.Empty:
                    continue
        
        thread = threading.Thread(target=process_results, daemon=True)
        thread.start()
    
    def _check_dependencies(self, completed_task_id: str):
        """Check if dependent tasks can now be executed"""
        if completed_task_id in self.dependencies:
            for dependent_id in self.dependencies[completed_task_id]:
                if self._can_execute(dependent_id):
                    task = self.tasks[dependent_id]
                    self.priority_queue.put(task)
    
    def _can_execute(self, task_id: str) -> bool:
        """Check if task dependencies are satisfied"""
        task = self.tasks[task_id]
        if task.status != TaskStatus.PENDING:
            return False
        
        for dep_id in task.dependencies:
            if dep_id not in self.tasks or self.tasks[dep_id].status != TaskStatus.COMPLETED:
                return False
        
        return True
    
    def _start_task_dispatcher(self):
        """Start background thread to dispatch tasks"""
        def dispatch_tasks():
            while not self.stop_event.is_set():
                try:
                    task = self.priority_queue.get(timeout=1)
                    
                    # Double-check if task is still valid
                    with self.lock:
                        if task.id in self.tasks and self.tasks[task.id].status == TaskStatus.PENDING:
                            self.task_queue.put(task.__dict__)
                
                except queue.Empty:
                    continue
        
        thread = threading.Thread(target=dispatch_tasks, daemon=True)
        thread.start()
    
    def submit_task(self, func_name: str, args: tuple = (), priority: int = 1, 
                   dependencies: List[str] = None, timeout: Optional[float] = None) -> str:
        """Submit a task for execution"""
        task_id = str(uuid.uuid4())
        
        if dependencies is None:
            dependencies = []
        
        task = Task(
            id=task_id,
            func_name=func_name,
            args=args,
            priority=priority,
            dependencies=dependencies,
            timeout=timeout
        )
        
        with self.lock:
            self.tasks[task_id] = task
            
            # Update dependency tracking
            for dep_id in dependencies:
                if dep_id not in self.dependencies:
                    self.dependencies[dep_id] = []
                self.dependencies[dep_id].append(task_id)
            
            # Add to queue if no dependencies or dependencies satisfied
            if self._can_execute(task_id):
                self.priority_queue.put(task)
        
        return task_id
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task"""
        with self.lock:
            if task_id in self.tasks and self.tasks[task_id].status == TaskStatus.PENDING:
                self.tasks[task_id].status = TaskStatus.CANCELLED
                return True
        return False
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get task status"""
        with self.lock:
            return self.tasks.get(task_id, Task("", "")).status
    
    def get_task_result(self, task_id: str) -> Any:
        """Get task result (blocks until completed)"""
        while True:
            with self.lock:
                if task_id in self.tasks:
                    task = self.tasks[task_id]
                    if task.status == TaskStatus.COMPLETED:
                        return task.result
                    elif task.status == TaskStatus.FAILED:
                        raise Exception(f"Task failed: {task.error}")
                    elif task.status == TaskStatus.CANCELLED:
                        raise Exception("Task was cancelled")
            time.sleep(0.1)
    
    def scale_workers(self, new_count: int):
        """Dynamically scale number of workers"""
        current_count = len(self.workers)
        
        if new_count > current_count:
            # Add workers
            for i in range(new_count - current_count):
                worker = mp.Process(
                    target=worker_process,
                    args=(self.task_queue, self.result_queue, self.stop_event)
                )
                worker.start()
                self.workers.append(worker)
        elif new_count < current_count:
            # Remove workers (terminate extra processes)
            for i in range(current_count - new_count):
                worker = self.workers.pop()
                worker.terminate()
                worker.join()
        
        self.num_workers = new_count
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status"""
        with self.lock:
            status_counts = {}
            for status in TaskStatus:
                status_counts[status.value] = sum(1 for t in self.tasks.values() 
                                                 if t.status == status)
            
            return {
                'num_workers': self.num_workers,
                'total_tasks': len(self.tasks),
                'task_status': status_counts,
                'queue_size': self.priority_queue.qsize()
            }
    
    def shutdown(self):
        """Gracefully shutdown the scheduler"""
        self.stop_event.set()
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5)
            if worker.is_alive():
                worker.terminate()
        
        print("Scheduler shutdown complete")

# Example task functions
@register_task("add")
def add_numbers(x, y):
    return x + y

@register_task("multiply")
def multiply_numbers(x, y):
    time.sleep(1)  # Simulate work
    return x * y

@register_task("fail")
def failing_task():
    raise ValueError("This task always fails")

@register_task("slow")
def slow_task(duration):
    time.sleep(duration)
    return f"Completed after {duration}s"

# Demo function
def demo():
    print("=== Distributed Task Scheduler Demo ===")
    
    # Create scheduler
    scheduler = DistributedTaskScheduler(num_workers=2)
    
    # Start task dispatcher
    scheduler._start_task_dispatcher()
    
    try:
        # Submit tasks with different priorities
        print("\n1. Submitting tasks with priorities...")
        
        task1 = scheduler.submit_task("add", (10, 20), priority=1)
        task2 = scheduler.submit_task("multiply", (5, 6), priority=3)  # Higher priority
        task3 = scheduler.submit_task("add", (1, 2), priority=2)
        
        # Wait and get results
        print(f"Task 1 result: {scheduler.get_task_result(task1)}")
        print(f"Task 2 result: {scheduler.get_task_result(task2)}")
        print(f"Task 3 result: {scheduler.get_task_result(task3)}")
        
        # Test dependencies
        print("\n2. Testing task dependencies...")
        
        task4 = scheduler.submit_task("add", (100, 200), priority=1)
        task5 = scheduler.submit_task("multiply", (2, 10), priority=1, 
                                    dependencies=[task4])  # Depends on task4
        
        print(f"Task 4 result: {scheduler.get_task_result(task4)}")
        print(f"Task 5 result: {scheduler.get_task_result(task5)}")
        
        # Test timeout
        print("\n3. Testing timeout...")
        task6 = scheduler.submit_task("slow", (3,), timeout=1.0)  # Will timeout
        
        try:
            result = scheduler.get_task_result(task6)
            print(f"Task 6 result: {result}")
        except Exception as e:
            print(f"Task 6 failed as expected: {e}")
        
        # Test cancellation
        print("\n4. Testing cancellation...")
        task7 = scheduler.submit_task("slow", (5,))
        time.sleep(0.1)  # Let it get queued
        
        if scheduler.cancel_task(task7):
            print("Task 7 cancelled successfully")
        else:
            print("Task 7 could not be cancelled")
        
        # Test scaling
        print("\n5. Testing worker scaling...")
        print(f"Current workers: {scheduler.num_workers}")
        scheduler.scale_workers(4)
        print(f"Scaled to: {scheduler.num_workers} workers")
        
        # Test failure handling
        print("\n6. Testing failure handling...")
        task8 = scheduler.submit_task("fail")
        
        try:
            result = scheduler.get_task_result(task8)
        except Exception as e:
            print(f"Task 8 failed as expected: {e}")
        
        # Show final status
        print("\n7. Final status:")
        status = scheduler.get_status()
        for key, value in status.items():
            print(f"  {key}: {value}")
    
    finally:
        scheduler.shutdown()

if __name__ == "__main__":
    demo()
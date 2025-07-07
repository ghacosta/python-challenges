from typing import Iterator, Iterable, Callable, Any, List
import itertools


class LazyCollection:    
    def __init__(self, source: Iterable[Any]):
        # Initialize with a source iterable
        self._source = source
        self._operations = []
    
    def __iter__(self) -> Iterator[Any]:
        # Start with the source iterator
        current_iter = iter(self._source)
        
        # Apply each operation in sequence
        for operation in self._operations:
            current_iter = operation(current_iter)
        
        return current_iter
    
    def _add_operation(self, operation: Callable[[Iterator], Iterator]) -> 'LazyCollection':
        new_collection = LazyCollection(self._source)
        new_collection._operations = self._operations + [operation]
        return new_collection
    
    # Core transformation methods
    def map(self, func: Callable[[Any], Any]) -> 'LazyCollection':
        return self._add_operation(lambda it: map(func, it))
    
    def filter(self, predicate: Callable[[Any], bool]) -> 'LazyCollection':
        return self._add_operation(lambda it: filter(predicate, it))
    
    def take(self, n: int) -> 'LazyCollection':
        return self._add_operation(lambda it: itertools.islice(it, n))
    
    # Chunking and Pagination
    def chunk(self, size: int) -> 'LazyCollection':
        def chunk_generator(it):
            iterator = iter(it)
            while True:
                chunk = list(itertools.islice(iterator, size))
                if not chunk:
                    break
                yield chunk
        return self._add_operation(chunk_generator)
    
    def paginate(self, page_size: int, page_number: int = 1) -> 'LazyCollection':
        if page_number < 1:
            raise ValueError("Page number must be >= 1")
        
        start_idx = (page_number - 1) * page_size
        return self._add_operation(
            lambda it: itertools.islice(it, start_idx, start_idx + page_size)
        )
    
    # Terminal operations
    def to_list(self) -> List[Any]:
        return list(self)
    
    def reduce(self, func: Callable[[Any, Any], Any], initial: Any = None) -> Any:
        from functools import reduce as functools_reduce
        if initial is None:
            return functools_reduce(func, self)
        return functools_reduce(func, self, initial)

def demonstrate_lazy_collection():
    print("=== Lazy Evaluation Demo ===")
    
    def expensive_transform(x):
        print(f"Processing {x}")
        return x * x
    
    # Create pipeline (no computation yet)
    lazy = (LazyCollection(range(1, 10))
            .filter(lambda x: x % 2 == 0) 
            .map(expensive_transform) # created to demonstrate lazy evaluation
            .take(3))                 
    
    print("Pipeline created - no computation yet!")
    print(f"Results: {lazy.to_list()}")        # Computation happens here
    
    print("\n=== Composability Demo ===")
    
    # Show method chaining
    result = (LazyCollection(range(1, 20))
              .filter(lambda x: x % 3 == 0)
              .map(lambda x: x * 2)            
              .take(4)                          
              .to_list())
    
    print(f"Chained operations: {result}")
    
    print("\n=== Memory Efficiency Demo ===")
    
    # Large dataset (1M) - only processes what's needed
    large_data = range(1, 1000000)  
    
    result = (LazyCollection(large_data)
              .filter(lambda x: x % 1000 == 0) 
              .map(lambda x: x // 1000)        
              .take(5)                          
              .to_list())
    
    print(f"From 1M items, got: {result}")
    
    print("\n=== Pagination Demo ===")
    
    # Simulate paginated results
    data = LazyCollection(range(1, 21))
    page1 = data.paginate(page_size=5, page_number=1).to_list()
    page2 = data.paginate(page_size=5, page_number=2).to_list()
    
    print(f"Page 1: {page1}")
    print(f"Page 2: {page2}")
    
    print("\n=== Chunking Demo ===")
    
    # Process data in chunks
    chunks = LazyCollection(range(1, 13)).chunk(4).to_list()
    print(f"Data in chunks of 4: {chunks}")
    
    print("\n=== Reduce Demo ===")
    
    # Demonstrate reduce operation
    sum_result = LazyCollection(range(1, 6)).reduce(lambda a, b: a + b)
    product_result = LazyCollection(range(1, 5)).reduce(lambda a, b: a * b)
    
    print(f"Sum of 1-5: {sum_result}")
    print(f"Product of 1-4: {product_result}")


if __name__ == "__main__":
    demonstrate_lazy_collection()
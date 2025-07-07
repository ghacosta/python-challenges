from typing import Dict, Type, Any
import inspect


class ContractViolationError(Exception):
    pass

# Global registry for auto-registered classes
CLASS_REGISTRY: Dict[str, Type] = {}


class Contract:
    def __init__(self, required_methods=None, required_attrs=None):
        self.required_methods = required_methods or []
        self.required_attrs = required_attrs or {}
    
    def validate(self, cls):
        # Check required methods
        for method_name in self.required_methods:
            if not hasattr(cls, method_name) or not callable(getattr(cls, method_name)):
                raise ContractViolationError(
                    f"Class {cls.__name__} missing required method '{method_name}'"
                )
        
        return True


class ContractMeta(type):
    def __new__(mcs, name, bases, dct, contract=None):
        cls = super().__new__(mcs, name, bases, dct)

        cls._contract = contract
        
        if contract:
            contract.validate(cls)
        
        # Inherit parent contracts
        for base in bases:
            if hasattr(base, '_contract') and base._contract and not contract:
                cls._contract = base._contract
                cls._contract.validate(cls)
        
        # Auto-register non-abstract classes
        if not name.startswith('_') and not inspect.isabstract(cls):
            CLASS_REGISTRY[name] = cls
            print(f"✓ Registered: {name}")
        
        return cls
    
    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        
        # Validate instance attributes if contract exists
        if hasattr(cls, '_contract') and cls._contract:
            for attr_name, attr_type in cls._contract.required_attrs.items():
                if not hasattr(instance, attr_name):
                    raise ContractViolationError(
                        f"Instance missing required attribute '{attr_name}'"
                    )
                
                attr_value = getattr(instance, attr_name)
                if not isinstance(attr_value, attr_type):
                    raise ContractViolationError(
                        f"Attribute '{attr_name}' must be {attr_type.__name__}, "
                        f"got {type(attr_value).__name__}"
                    )
        
        return instance


# Defining example contracts
PROCESSOR_CONTRACT = Contract(
    required_methods=['process'],
    required_attrs={'name': str, 'batch_size': int}
)

SERVICE_CONTRACT = Contract(
    required_methods=['start', 'stop'],
    required_attrs={'status': str}
)


# Example classes
class BaseProcessor(metaclass=ContractMeta, contract=PROCESSOR_CONTRACT):    
    def __init__(self, name: str, batch_size: int = 100):
        self.name = name
        self.batch_size = batch_size
    
    def process(self, data):
        return data


class TextProcessor(BaseProcessor):
    # Inherits contract from BaseProcessor
    
    def process(self, data):
        return [str(item).upper() for item in data]


class WebService(metaclass=ContractMeta, contract=SERVICE_CONTRACT):
    # Web service following the service contract
    
    def __init__(self, port: int):
        self.port = port
        self.status = 'stopped'
    
    def start(self):
        self.status = 'running'
        print(f"Service started on port {self.port}")
    
    def stop(self):
        self.status = 'stopped'
        print("Service stopped")


# Demo function
def demo():
    print("=== Meta-Programming Demo ===\n")
    
    print("1. Auto-registered classes:")
    for name in CLASS_REGISTRY:
        print(f"   - {name}")
    print()
    
    print("2. Creating valid instances:")
    processor = TextProcessor("MyProcessor", 50)
    service = WebService(8080)
    
    print(f"   ✓ {processor.__class__.__name__}: {processor.name}")
    print(f"   ✓ {service.__class__.__name__}: port {service.port}")
    print()
    
    print("3. Testing methods:")
    result = processor.process(["hello", "world"])
    print(f"   ✓ Processed: {result}")
    
    service.start()
    service.stop()
    print()
    
    print("4. Contract violation example:")
    try:
        class BadProcessor(metaclass=ContractMeta, contract=PROCESSOR_CONTRACT):
            def __init__(self):
                # using wrong types
                self.name = 123
                self.batch_size = "invalid"
        
        bad = BadProcessor()
    except ContractViolationError as e:
        print(f"   ✓ Caught: {e}")
    
    try:
        class IncompleteService(metaclass=ContractMeta, contract=SERVICE_CONTRACT):
            def __init__(self):
                self.status = 'stopped'
            # Missing start() and stop() methods!
    except ContractViolationError as e:
        print(f"   ✓ Caught: {e}")


if __name__ == "__main__":
    demo()
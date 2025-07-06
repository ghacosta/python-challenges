import asyncio
import random
import time
import json
import aiohttp
from typing import List, Dict, Any


class TestDataGenerator:
    """Generate test data for the data pipeline."""
    
    def __init__(self, webhook_url: str = "http://localhost:8080/webhook"):
        self.webhook_url = webhook_url
        self.event_types = ["click", "view", "purchase", "signup", "download", "share"]
        self.user_ids = [f"user_{i}" for i in range(100)]
        self.sources = ["web", "mobile", "api", "email", "social"]
    
    def generate_data_point(self, batch_id: int = 0) -> Dict[str, Any]:
        """Generate a single test data point."""
        return {
            "user_id": random.choice(self.user_ids),
            "event_type": random.choice(self.event_types),
            "value": round(random.uniform(0.1, 100.0), 2),
            "timestamp": time.time(),
            "metadata": {
                "source": random.choice(self.sources),
                "batch": batch_id,
                "session_id": f"session_{random.randint(1000, 9999)}",
                "user_agent": random.choice([
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    "Mozilla/5.0 (X11; Linux x86_64)"
                ])
            }
        }
    
    async def send_data_point(self, session: aiohttp.ClientSession, data: Dict[str, Any]) -> bool:
        """Send a single data point to the webhook."""
        try:
            async with session.post(
                self.webhook_url,
                json=data,
                timeout=aiohttp.ClientTimeout(total=2)
            ) as response:
                return response.status == 200
        except Exception as e:
            print(f"Error sending data point: {e}")
            return False
    
    async def generate_steady_stream(self, 
                                   duration_seconds: int = 300,
                                   rate_per_second: float = 10.0) -> None:
        """Generate a steady stream of test data."""
        print(f"Generating steady stream: {rate_per_second} messages/second for {duration_seconds} seconds")
        
        async with aiohttp.ClientSession() as session:
            end_time = time.time() + duration_seconds
            message_count = 0
            
            while time.time() < end_time:
                batch_start = time.time()
                
                # Send messages for this second
                for _ in range(int(rate_per_second)):
                    data = self.generate_data_point(message_count // 100)
                    success = await self.send_data_point(session, data)
                    
                    if success:
                        message_count += 1
                        if message_count % 100 == 0:
                            print(f"Sent {message_count} messages")
                
                # Wait for the remainder of the second
                elapsed = time.time() - batch_start
                sleep_time = max(0, 1.0 - elapsed)
                await asyncio.sleep(sleep_time)
        
        print(f"Finished sending {message_count} messages")
    
    async def generate_burst_traffic(self, 
                                   burst_size: int = 100,
                                   burst_interval: int = 30,
                                   num_bursts: int = 5) -> None:
        """Generate burst traffic to test pipeline resilience."""
        print(f"Generating burst traffic: {burst_size} messages every {burst_interval} seconds, {num_bursts} bursts")
        
        async with aiohttp.ClientSession() as session:
            for burst_num in range(num_bursts):
                print(f"Starting burst {burst_num + 1}/{num_bursts}")
                
                # Send burst of messages
                tasks = []
                for i in range(burst_size):
                    data = self.generate_data_point(burst_num)
                    task = self.send_data_point(session, data)
                    tasks.append(task)
                
                # Wait for all messages in burst to complete
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success_count = sum(1 for r in results if r is True)
                
                print(f"Burst {burst_num + 1} completed: {success_count}/{burst_size} messages sent successfully")
                
                # Wait before next burst
                if burst_num < num_bursts - 1:
                    await asyncio.sleep(burst_interval)
    
    async def generate_variable_rate_traffic(self, duration_seconds: int = 300) -> None:
        """Generate variable rate traffic to simulate real-world conditions."""
        print(f"Generating variable rate traffic for {duration_seconds} seconds")
        
        async with aiohttp.ClientSession() as session:
            end_time = time.time() + duration_seconds
            message_count = 0
            
            while time.time() < end_time:
                # Variable rate: sine wave between 1-50 messages per second
                elapsed = time.time() - (end_time - duration_seconds)
                rate = 25 + 24 * abs(asyncio.get_event_loop().time() % 60 - 30) / 30
                
                batch_start = time.time()
                
                # Send messages for this second
                for _ in range(int(rate)):
                    data = self.generate_data_point(message_count // 100)
                    success = await self.send_data_point(session, data)
                    
                    if success:
                        message_count += 1
                        if message_count % 100 == 0:
                            print(f"Sent {message_count} messages (current rate: {rate:.1f}/sec)")
                
                # Wait for the remainder of the second
                elapsed = time.time() - batch_start
                sleep_time = max(0, 1.0 - elapsed)
                await asyncio.sleep(sleep_time)
        
        print(f"Finished variable rate test: {message_count} messages sent")


async def main():
    """Main function to run test data generation."""
    generator = TestDataGenerator()
    
    print("Test Data Generator")
    print("==================")
    print("1. Steady stream (10 msg/sec for 5 minutes)")
    print("2. Burst traffic (100 msg bursts every 30 seconds)")
    print("3. Variable rate traffic (1-50 msg/sec for 5 minutes)")
    print("4. Single test message")
    print("5. Custom test")
    
    choice = input("\nSelect test type (1-5): ").strip()
    
    try:
        if choice == "1":
            await generator.generate_steady_stream(duration_seconds=300, rate_per_second=10)
        elif choice == "2":
            await generator.generate_burst_traffic(burst_size=100, burst_interval=30, num_bursts=5)
        elif choice == "3":
            await generator.generate_variable_rate_traffic(duration_seconds=300)
        elif choice == "4":
            async with aiohttp.ClientSession() as session:
                data = generator.generate_data_point()
                success = await generator.send_data_point(session, data)
                print(f"Test message sent: {success}")
                print(f"Data: {json.dumps(data, indent=2)}")
        elif choice == "5":
            duration = int(input("Duration (seconds): "))
            rate = float(input("Rate (messages/second): "))
            await generator.generate_steady_stream(duration_seconds=duration, rate_per_second=rate)
        else:
            print("Invalid choice")
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Error during test: {e}")


if __name__ == "__main__":
    asyncio.run(main())
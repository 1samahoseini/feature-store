"""
Batch Processing Example for BNPL Feature Store
Demonstrates efficient batch feature retrieval and processing
"""
import asyncio
import time
from typing import List, Dict, Any
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class BatchFeatureProcessor:
    """Efficient batch processing for feature store operations"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def process_batch_sequential(self, user_ids: List[str]) -> Dict[str, Any]:
        """Process batch sequentially (baseline approach)"""
        results = []
        start_time = time.time()
        
        for user_id in user_ids:
            try:
                response = self.session.get(f"{self.base_url}/api/v1/features/user/{user_id}")
                if response.status_code == 200:
                    results.append(response.json())
            except Exception as e:
                print(f"Error processing {user_id}: {e}")
        
        total_time = (time.time() - start_time) * 1000
        
        return {
            "method": "sequential",
            "total_users": len(user_ids),
            "successful_results": len(results),
            "total_time_ms": round(total_time, 2),
            "avg_time_per_user_ms": round(total_time / len(user_ids), 2),
            "results": results
        }
    
    def process_batch_concurrent(self, user_ids: List[str], max_workers: int = 10) -> Dict[str, Any]:
        """Process batch with concurrent requests (optimized approach)"""
        results = []
        start_time = time.time()
        
        def fetch_user_features(user_id: str):
            try:
                response = self.session.get(f"{self.base_url}/api/v1/features/user/{user_id}")
                if response.status_code == 200:
                    return response.json()
                return None
            except Exception as e:
                print(f"Error processing {user_id}: {e}")
                return None
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_user = {executor.submit(fetch_user_features, user_id): user_id 
                            for user_id in user_ids}
            
            for future in as_completed(future_to_user):
                result = future.result()
                if result:
                    results.append(result)
        
        total_time = (time.time() - start_time) * 1000
        
        return {
            "method": "concurrent",
            "max_workers": max_workers,
            "total_users": len(user_ids),
            "successful_results": len(results),
            "total_time_ms": round(total_time, 2),
            "avg_time_per_user_ms": round(total_time / len(user_ids), 2),
            "speedup_factor": "N/A",  # Will be calculated by comparison
            "results": results
        }
    
    def process_batch_api_endpoint(self, user_ids: List[str]) -> Dict[str, Any]:
        """Use dedicated batch API endpoint (most efficient)"""
        start_time = time.time()
        
        batch_request = {
            "requests": [
                {"user_id": user_id, "feature_types": ["user", "transaction", "risk"]}
                for user_id in user_ids
            ]
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/features/batch",
                json=batch_request,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                total_time = (time.time() - start_time) * 1000
                
                return {
                    "method": "batch_api",
                    "total_users": len(user_ids),
                    "successful_results": result.get("successful_requests", 0),
                    "total_time_ms": round(total_time, 2),
                    "avg_time_per_user_ms": round(total_time / len(user_ids), 2),
                    "server_processing_time_ms": result.get("total_response_time_ms", 0),
                    "results": result.get("responses", [])
                }
        except Exception as e:
            print(f"Batch API error: {e}")
        
        return {"method": "batch_api", "error": "Failed to process batch"}


async def async_batch_processing(user_ids: List[str], base_url: str = "http://localhost:8000"):
    """Async batch processing example"""
    import aiohttp
    
    async def fetch_user_features(session, user_id):
        try:
            async with session.get(f"{base_url}/api/v1/features/user/{user_id}") as response:
                if response.status == 200:
                    return await response.json()
                return None
        except Exception as e:
            print(f"Async error for {user_id}: {e}")
            return None
    
    start_time = time.time()
    results = []
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_user_features(session, user_id) for user_id in user_ids]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = [r for r in responses if r and not isinstance(r, Exception)]
    
    total_time = (time.time() - start_time) * 1000
    
    return {
        "method": "async",
        "total_users": len(user_ids),
        "successful_results": len(results),
        "total_time_ms": round(total_time, 2),
        "avg_time_per_user_ms": round(total_time / len(user_ids), 2),
        "results": results
    }


def performance_comparison():
    """Compare different batch processing approaches"""
    print("🏁 Batch Processing Performance Comparison")
    print("=" * 50)
    
    # Test with different batch sizes
    test_cases = [
        {"name": "Small Batch", "size": 10},
        {"name": "Medium Batch", "size": 50}, 
        {"name": "Large Batch", "size": 100}
    ]
    
    processor = BatchFeatureProcessor()
    
    for test_case in test_cases:
        print(f"\n📊 {test_case['name']} ({test_case['size']} users):")
        print("-" * 30)
        
        # Generate test user IDs
        user_ids = [f"user_{i+1:06d}" for i in range(test_case['size'])]
        
        # Sequential processing
        sequential_result = processor.process_batch_sequential(user_ids)
        print(f"Sequential:  {sequential_result['total_time_ms']:6.1f}ms total, {sequential_result['avg_time_per_user_ms']:5.1f}ms/user")
        
        # Concurrent processing
        concurrent_result = processor.process_batch_concurrent(user_ids, max_workers=10)
        speedup = sequential_result['total_time_ms'] / concurrent_result['total_time_ms']
        concurrent_result['speedup_factor'] = f"{speedup:.1f}x"
        print(f"Concurrent:  {concurrent_result['total_time_ms']:6.1f}ms total, {concurrent_result['avg_time_per_user_ms']:5.1f}ms/user ({concurrent_result['speedup_factor']} speedup)")
# Batch API (continued)
        batch_result = processor.process_batch_api_endpoint(user_ids)
        if 'error' not in batch_result:
            batch_speedup = sequential_result['total_time_ms'] / batch_result['total_time_ms']
            print(f"Batch API:   {batch_result['total_time_ms']:6.1f}ms total, {batch_result['avg_time_per_user_ms']:5.1f}ms/user ({batch_speedup:.1f}x speedup)")
        else:
            print(f"Batch API:   {batch_result.get('error', 'Unknown error')}")
        
        # Async processing
        async_result = asyncio.run(async_batch_processing(user_ids))
        async_speedup = sequential_result['total_time_ms'] / async_result['total_time_ms']
        print(f"Async:       {async_result['total_time_ms']:6.1f}ms total, {async_result['avg_time_per_user_ms']:5.1f}ms/user ({async_speedup:.1f}x speedup)")


def bnpl_use_case_example():
    """Real BNPL use case: Process checkout requests"""
    print("\n💳 BNPL Use Case: Checkout Processing")
    print("=" * 40)
    
    # Simulate checkout requests during peak hours
    checkout_requests = [
        {"user_id": "user_000001", "amount": 299.99, "merchant": "SHEIN"},
        {"user_id": "user_000002", "amount": 89.99, "merchant": "IKEA"}, 
        {"user_id": "user_000003", "amount": 459.99, "merchant": "Amazon"},
        {"user_id": "user_000004", "amount": 199.99, "merchant": "Noon"},
        {"user_id": "user_000005", "amount": 349.99, "merchant": "SHEIN"}
    ]
    
    processor = BatchFeatureProcessor()
    user_ids = [req["user_id"] for req in checkout_requests]
    
    print("Processing checkout decisions...")
    
    # Use batch API for efficiency
    start_time = time.time()
    batch_result = processor.process_batch_api_endpoint(user_ids)
    processing_time = (time.time() - start_time) * 1000
    
    if 'error' not in batch_result:
        print(f"✅ Processed {len(checkout_requests)} checkout requests in {processing_time:.1f}ms")
        print(f"📊 Average decision time: {processing_time/len(checkout_requests):.1f}ms per checkout")
        
        # Simulate credit decisions based on features
        for i, request in enumerate(checkout_requests):
            if i < len(batch_result.get('results', [])):
                user_features = batch_result['results'][i]
                risk_score = user_features.get('risk_features', {}).get('risk_score', 0.5)
                
                # Simple approval logic
                if risk_score < 0.3 and request['amount'] < 500:
                    decision = "APPROVED"
                    emoji = "✅"
                elif risk_score < 0.6 and request['amount'] < 300:
                    decision = "APPROVED" 
                    emoji = "✅"
                else:
                    decision = "DECLINED"
                    emoji = "❌"
                
                print(f"{emoji} {request['user_id']}: ${request['amount']} at {request['merchant']} - {decision}")
    else:
        print("❌ Failed to process checkout requests")


def data_pipeline_simulation():
    """Simulate daily batch processing pipeline"""
    print("\n🔄 Daily Batch Processing Pipeline")
    print("=" * 40)
    
    # Simulate processing different user segments
    segments = {
        "new_users": [f"new_user_{i:04d}" for i in range(1, 51)],        # 50 new users
        "active_users": [f"user_{i:06d}" for i in range(1, 201)],        # 200 active users  
        "high_value_users": [f"vip_user_{i:03d}" for i in range(1, 21)]  # 20 VIP users
    }
    
    processor = BatchFeatureProcessor()
    pipeline_results = {}
    
    total_start_time = time.time()
    
    for segment_name, user_ids in segments.items():
        print(f"\n📊 Processing {segment_name}: {len(user_ids)} users")
        
        # Use appropriate processing method based on segment
        if len(user_ids) > 100:
            result = processor.process_batch_concurrent(user_ids, max_workers=20)
        else:
            result = processor.process_batch_api_endpoint(user_ids)
        
        pipeline_results[segment_name] = result
        
        if 'error' not in result:
            print(f"   ✅ Completed in {result['total_time_ms']:.0f}ms")
            print(f"   📈 {result['successful_results']}/{result['total_users']} users processed")
        else:
            print(f"   ❌ Failed: {result.get('error', 'Unknown error')}")
    
    total_pipeline_time = (time.time() - total_start_time) * 1000
    
    # Pipeline summary
    print("\n📋 Pipeline Summary:")
    print(f"   🕒 Total pipeline time: {total_pipeline_time:.0f}ms")
    
    total_users = sum(len(users) for users in segments.values())
    successful_users = sum(
        result.get('successful_results', 0) 
        for result in pipeline_results.values() 
        if 'error' not in result
    )
    
    print(f"   👥 Total users processed: {successful_users}/{total_users}")
    print(f"   📊 Success rate: {(successful_users/total_users)*100:.1f}%")
    print(f"   ⚡ Average processing rate: {(successful_users/(total_pipeline_time/1000)):.0f} users/second")


def main():
    """Main batch processing demonstration"""
    print("📦 BNPL Feature Store - Batch Processing Examples")
    print("=" * 55)
    
    try:
        # Check if API is available
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code != 200:
            print("❌ Feature Store API not available. Start with: make up")
            return
    except requests.exceptions.RequestException:
        print("❌ Feature Store API not available. Start with: make up")
        return
    
    # Run examples
    performance_comparison()
    bnpl_use_case_example() 
    data_pipeline_simulation()
    
    print("\n🎯 Key Takeaways:")
    print("   • Batch API endpoint provides best performance for large batches")
    print("   • Concurrent processing offers good balance for medium batches")
    print("   • Async processing excels with high I/O concurrency")
    print("   • Choose method based on batch size and latency requirements")


if __name__ == "__main__":
    main()
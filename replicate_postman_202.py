#!/usr/bin/env python3
"""
Perfect Postman 202 Duplicate Replication
=========================================
This script exactly replicates the Postman collection behavior that other testers
are seeing, by following the exact pre-request script logic and timing.
"""

import requests
import json
import uuid
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

class PostmanEnvironment:
    """Simulate Postman environment exactly"""
    def __init__(self):
        self.variables = {}
        self.duplicated = False
        self.lock = threading.Lock()
    
    def get(self, key):
        return self.variables.get(key, None)
    
    def set(self, key, value):
        self.variables[key] = value
        if key == "duplicated":
            self.duplicated = value

class PostmanCollectionReplicator:
    """Exact replication of Postman collection behavior"""
    
    def __init__(self):
        self.pm_environment = PostmanEnvironment()
        # Initialize duplicated to false (Postman default)
        self.pm_environment.set("duplicated", False)
        
        self.url = "http://wallet-ss-sandbox.betprophet.io/api/v1/wallet/bets"
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Base payload template from collection
        self.payload_template = []
        for i in range(1, 201):  # 200 wagers like in collection
            self.payload_template.append({
                "userUuid": "14e1a397-2322-4a2f-a829-d138a8d9ee40",
                "transactionAmount": 1,
                "clientTransactionId": f"{{{{transaction_uuid{i}}}}}",
                "marketId": 1,
                "eventId": 2,
                "betId": f"{{{{bet_uuid{i}}}}}",
                "outcomeId": 1
            })
    
    def run_pre_request_script(self, request_name):
        """Execute the exact pre-request script logic from collection"""
        print(f"🔧 [{request_name}] Running pre-request script")
        print(f"   🔍 Current duplicated value: {self.pm_environment.get('duplicated')}")
        
        # Exact logic from collection:
        # if (pm.environment.get("duplicated") == false)
        if self.pm_environment.get("duplicated") == False:
            print(f"   📝 [{request_name}] duplicated == false: Generating NEW UUIDs")
            print(f"   📝 [{request_name}] console.log('reset uuid')")
            
            # Generate 200 UUIDs as per collection
            for i in range(1, 201):
                bet_id = str(uuid.uuid4())
                transaction_id = str(uuid.uuid4())
                
                self.pm_environment.set(f'bet_uuid{i}', bet_id)
                self.pm_environment.set(f'transaction_uuid{i}', transaction_id)
                
            # pm.environment.set("duplicated", true);
            self.pm_environment.set("duplicated", True)
            print(f"   ✅ [{request_name}] Set duplicated = True")
            
        else:
            print(f"   🔄 [{request_name}] duplicated == true: REUSING same UUIDs!")
            # pm.environment.set("duplicated", false);
            self.pm_environment.set("duplicated", False)
            print(f"   ✅ [{request_name}] Set duplicated = False")
    
    def substitute_variables(self, payload_template):
        """Replace {{variable}} placeholders with actual values"""
        payload_str = json.dumps(payload_template)
        
        # Replace all variables
        for var_name, var_value in self.pm_environment.variables.items():
            if var_name != "duplicated":
                payload_str = payload_str.replace(f'{{{{{var_name}}}}}', var_value)
        
        return json.loads(payload_str)
    
    def execute_request(self, request_name):
        """Execute a single request exactly like Postman"""
        
        # Run pre-request script
        self.run_pre_request_script(request_name)
        
        # Create payload with variable substitution
        payload = self.substitute_variables(self.payload_template)
        
        # Execute request
        start_time = time.time()
        try:
            print(f"🚀 [{request_name}] Executing POST request")
            print(f"   📊 Payload size: {len(payload)} wagers")
            
            response = requests.post(
                self.url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"   ✅ [{request_name}] HTTP {response.status_code} in {duration:.3f}s")
            
            # Analyze response
            try:
                response_data = response.json()
                
                # Print raw response for inspection
                print(f"   📄 [{request_name}] RAW RESPONSE:")
                print(f"      {json.dumps(response_data, indent=2)}")
                print(f"   📄 [{request_name}] END RAW RESPONSE\n")
                
                # Check for transaction statuses
                statuses = []
                duplicate_count = 0
                processing_count = 0
                completed_count = 0
                
                if 'data' in response_data and response_data['data']:
                    for transaction in response_data['data']:
                        if 'transactionStatus' in transaction:
                            status = transaction['transactionStatus']
                            statuses.append(status)
                            
                            if status == 202:
                                duplicate_count += 1
                                # Only show first few to avoid spam
                                if duplicate_count <= 3:
                                    print(f"      🎯 [{request_name}] DUPLICATE DETECTED! transactionStatus: 202")
                                    print(f"      📋 Message: {transaction.get('message', 'N/A')}")
                                    print(f"      🔑 ClientTransactionId: {transaction.get('clientTransactionId', 'N/A')}")
                            elif status == 102:
                                processing_count += 1
                            elif status == 200:
                                completed_count += 1
                
                # Summary instead of individual logs
                print(f"   📊 [{request_name}] SUMMARY:")
                print(f"      🎯 Duplicates (202): {duplicate_count}")
                print(f"      ⏳ Processing (102): {processing_count}")
                print(f"      ✅ Completed (200): {completed_count}")
                print(f"      📈 Total transactions: {len(statuses)}")
                print(f"      🔢 Unique statuses: {set(statuses)}")
                
            except Exception as e:
                print(f"   ⚠️ [{request_name}] Could not parse response: {e}")
                print(f"   📄 [{request_name}] RAW TEXT RESPONSE: {response.text[:500]}...")
            
            return {
                'request_name': request_name,
                'status_code': response.status_code,
                'duration': duration,
                'response_data': response_data if 'response_data' in locals() else None,
                'transaction_statuses': statuses if 'statuses' in locals() else [],
                'timestamp': start_time,
                'success': True
            }
            
        except Exception as e:
            end_time = time.time()
            print(f"   ❌ [{request_name}] Request failed: {str(e)}")
            
            return {
                'request_name': request_name,
                'status_code': None,
                'duration': end_time - start_time,
                'error': str(e),
                'timestamp': start_time,
                'success': False
            }
    
    def run_simultaneous_requests(self):
        """Run both requests simultaneously like clicking both in Postman"""
        print("="*80)
        print("🎯 PERFECT POSTMAN DUPLICATE REPLICATION")
        print("="*80)
        print("Executing exactly what other testers are doing in Postman")
        print("This should trigger the transactionStatus: 202 behavior")
        print(f"Timestamp: {datetime.now()}\n")
        
        # Reset to initial state
        self.pm_environment.set("duplicated", False)
        print("🔄 Reset environment: duplicated = False")
        
        print("\n⚡ Starting simultaneous execution (like clicking both requests in Postman)...")
        
        results = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both requests simultaneously
            future1 = executor.submit(self.execute_request, "Request-1")
            future2 = executor.submit(self.execute_request, "Request-2")
            
            # Wait for completion
            for future in as_completed([future1, future2]):
                result = future.result()
                results.append(result)
        
        return results
    
    def analyze_results(self, results):
        """Analyze results for the 202 behavior"""
        print("\n" + "="*80)
        print("📋 FINAL ANALYSIS")
        print("="*80)
        
        if not results:
            print("❌ No results to analyze")
            return
        
        # Sort by timestamp
        results.sort(key=lambda x: x['timestamp'])
        
        found_202 = False
        found_102 = False
        
        for i, result in enumerate(results):
            print(f"\n🔍 {result['request_name']} (executed {i+1}):")
            print(f"   HTTP Status Code: {result['status_code']}")
            print(f"   Duration: {result['duration']:.3f}s")
            
            if result['success'] and result['transaction_statuses']:
                unique_statuses = set(result['transaction_statuses'])
                print(f"   Transaction Statuses: {sorted(unique_statuses)}")
                
                if 202 in unique_statuses:
                    found_202 = True
                    print(f"   🎯 FOUND DUPLICATE! This request detected duplicates")
                
                if 102 in unique_statuses:
                    found_102 = True
                    print(f"   ⏳ Processing status found")
        
        # Summary
        print(f"\n🎯 DUPLICATE DETECTION RESULTS:")
        if found_202:
            print(f"✅ SUCCESS: Found transactionStatus: 202 (duplicate detected)")
            print(f"🎉 This matches what other testers are experiencing!")
        elif found_102:
            print(f"⏳ Found transactionStatus: 102 (processing)")
            print(f"💭 API is working, but duplicates might not have been properly created")
        else:
            print(f"❌ No duplicate status codes detected")
        
        # Timing analysis
        if len(results) == 2:
            time_diff = abs(results[1]['timestamp'] - results[0]['timestamp'])
            print(f"\n⏱️ Execution timing:")
            print(f"   Time difference: {time_diff:.3f}s")
            if time_diff < 0.1:
                print(f"   ✅ Truly concurrent execution")
            else:
                print(f"   ⚠️ Requests were not perfectly concurrent")

def main():
    """Main execution"""
    print("🚀 Starting Perfect Postman 202 Duplicate Replication")
    
    replicator = PostmanCollectionReplicator()
    results = replicator.run_simultaneous_requests()
    replicator.analyze_results(results)
    
    print(f"\nCompleted at: {datetime.now()}")

if __name__ == "__main__":
    main()
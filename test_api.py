#!/usr/bin/env python3
"""
Test script for the Job Queue API
Tests all endpoints and demonstrates the system functionality
"""

import requests
import json
import time

BASE_URL = 'http://localhost:5000'

def test_health_check():
    """Test health check endpoint"""
    print("🔍 Testing health check...")
    try:
        response = requests.get(f'{BASE_URL}/health')
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"   ❌ Health check failed: {e}")
        return False

def test_submit_jobs():
    """Test job submission with different priorities"""
    print("\n📝 Testing job submission...")
    
    # Test jobs with different priorities and types
    test_jobs = [
        {
            "job_type": "send_email",
            "priority": "high",
            "payload": {
                "to": "user1@example.com",
                "subject": "High Priority Email",
                "message": "This is a high priority email!"
            }
        },
        {
            "job_type": "send_email",
            "priority": "low",
            "payload": {
                "to": "user2@example.com",
                "subject": "Low Priority Email",
                "message": "This is a low priority email."
            }
        },
        {
            "job_type": "process_data",
            "priority": "high",
            "payload": {
                "data_type": "customer_analytics",
                "file_path": "/data/customers.csv"
            }
        },
        {
            "job_type": "generate_report",
            "priority": "low",
            "payload": {
                "report_type": "monthly_sales",
                "month": "December",
                "year": 2024
            }
        }
    ]
    
    job_ids = []
    
    for i, job_data in enumerate(test_jobs, 1):
        try:
            print(f"   📤 Submitting job {i}: {job_data['job_type']} ({job_data['priority']} priority)")
            response = requests.post(f'{BASE_URL}/submit-job', json=job_data)
            
            if response.status_code == 201:
                result = response.json()
                job_id = result['job_id']
                job_ids.append(job_id)
                print(f"      ✅ Job submitted successfully: {job_id}")
            else:
                print(f"      ❌ Failed to submit job: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"      ❌ Error submitting job: {e}")
    
    return job_ids

def test_job_status(job_ids):
    """Test job status endpoint"""
    print(f"\n🔍 Testing job status for {len(job_ids)} jobs...")
    
    for job_id in job_ids:
        try:
            response = requests.get(f'{BASE_URL}/jobs/status/{job_id}')
            
            if response.status_code == 200:
                job_info = response.json()
                print(f"   📋 Job {job_id[:8]}...")
                print(f"      Type: {job_info['job_type']}")
                print(f"      Priority: {job_info['priority']}")
                print(f"      Status: {job_info['status']}")
                print(f"      Retry Count: {job_info['retry_count']}")
            else:
                print(f"   ❌ Failed to get status for job {job_id}")
                
        except Exception as e:
            print(f"   ❌ Error getting job status: {e}")

def test_list_jobs():
    """Test list all jobs endpoint"""
    print("\n📋 Testing list all jobs...")
    
    try:
        response = requests.get(f'{BASE_URL}/jobs')
        
        if response.status_code == 200:
            result = response.json()
            jobs = result['jobs']
            total = result['total_jobs']
            
            print(f"   📊 Total jobs: {total}")
            
            status_counts = {}
            for job in jobs:
                status = job['status']
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print(f"   📈 Status breakdown: {status_counts}")
            
        else:
            print(f"   ❌ Failed to list jobs: {response.status_code}")
            
    except Exception as e:
        print(f"   ❌ Error listing jobs: {e}")

def test_queue_stats():
    """Test queue statistics endpoint"""
    print("\n📊 Testing queue statistics...")
    
    try:
        response = requests.get(f'{BASE_URL}/queue/stats')
        
        if response.status_code == 200:
            stats = response.json()
            print(f"   🔢 Queue size: {stats['queue_size']}")
            print(f"   📈 Status counts: {stats['status_counts']}")
            print(f"   📋 Total jobs: {stats['total_jobs']}")
        else:
            print(f"   ❌ Failed to get queue stats: {response.status_code}")
            
    except Exception as e:
        print(f"   ❌ Error getting queue stats: {e}")

def test_invalid_job():
    """Test submitting invalid job data"""
    print("\n🚫 Testing invalid job submission...")
    
    invalid_jobs = [
        # Missing required fields
        {"job_type": "send_email"},
        
        # Invalid priority
        {
            "job_type": "send_email",
            "priority": "medium",
            "payload": {"to": "test@example.com"}
        },
        
        # Invalid job type
        {
            "job_type": "invalid_type",
            "priority": "high",
            "payload": {"data": "test"}
        }
    ]
    
    for i, invalid_job in enumerate(invalid_jobs, 1):
        try:
            print(f"   🧪 Testing invalid job {i}...")
            response = requests.post(f'{BASE_URL}/submit-job', json=invalid_job)
            
            if response.status_code == 400:
                print(f"      ✅ Correctly rejected: {response.json()['error']}")
            else:
                print(f"      ❌ Unexpected response: {response.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error testing invalid job: {e}")

def monitor_job_processing(job_ids, duration=30):
    """Monitor job processing for a specified duration"""
    print(f"\n⏱️  Monitoring job processing for {duration} seconds...")
    print("   (Make sure the worker is running: python run_worker.py)")
    
    start_time = time.time()
    
    while time.time() - start_time < duration:
        try:
            response = requests.get(f'{BASE_URL}/queue/stats')
            if response.status_code == 200:
                stats = response.json()
                print(f"   📊 Queue: {stats['queue_size']} | Status: {stats['status_counts']}")
            
            time.sleep(5)  # Check every 5 seconds
            
        except Exception as e:
            print(f"   ❌ Error monitoring jobs: {e}")
            break

def main():
    """Main test function"""
    print("=" * 60)
    print("🧪 JOB QUEUE API TEST SUITE")
    print("=" * 60)
    print("📝 Make sure to:")
    print("   1. Start Redis server")
    print("   2. Start Flask API: python app.py")
    print("   3. Start Worker: python run_worker.py")
    print("=" * 60)
    
    # Test health check first
    if not test_health_check():
        print("❌ API is not running. Please start the Flask server first.")
        return
    
    # Submit test jobs
    job_ids = test_submit_jobs()
    
    if not job_ids:
        print("❌ No jobs were submitted successfully")
        return
    
    # Test other endpoints
    test_job_status(job_ids)
    test_list_jobs()
    test_queue_stats()
    test_invalid_job()
    
    # Monitor processing
    monitor_job_processing(job_ids, duration=20)
    
    # Final status check
    print("\n🏁 Final job status check:")
    test_job_status(job_ids)
    
    print("\n✅ Test suite completed!")
    print("💡 Check the worker console for job processing logs")

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Job Worker Startup Script
Runs the job worker to process queued jobs
"""

import sys
import os

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from worker import JobWorker

def main():
    """Main function to start the worker"""
    print("=" * 50)
    print("🏭 CUSTOM JOB QUEUE WORKER")
    print("=" * 50)
    
    try:
        worker = JobWorker()
        worker.start()
    except Exception as e:
        print(f"❌ Failed to start worker: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
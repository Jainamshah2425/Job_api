from flask import Flask, request, jsonify
import redis
import json
from models import Job
from database import Database
from config import Config

app = Flask(__name__)

# Initialize Redis connection
redis_client = redis.Redis(
    host=Config.REDIS_HOST,
    port=Config.REDIS_PORT,
    db=Config.REDIS_DB,
    decode_responses=True
)

# Initialize Database
db = Database()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        redis_client.ping()
        return jsonify({
            'status': 'healthy',
            'redis': 'connected',
            'database': 'connected'
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

@app.route('/submit-job', methods=['POST'])
def submit_job():
    """Submit a new job to the queue"""
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        required_fields = ['job_type', 'priority', 'payload']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing field: {field}'}), 400
        
        # Validate priority
        if data['priority'] not in ['high', 'low']:
            return jsonify({'error': 'Priority must be "high" or "low"'}), 400
        
        # Validate job_type
        valid_job_types = ['send_email', 'process_data', 'generate_report']
        if data['job_type'] not in valid_job_types:
            return jsonify({
                'error': f'Invalid job_type. Valid types: {valid_job_types}'
            }), 400
        
        # Create job
        job = Job(
            job_type=data['job_type'],
            priority=data['priority'],
            payload=data['payload']
        )
        
        # Save to database
        if not db.save_job(job):
            return jsonify({'error': 'Failed to save job to database'}), 500
        
        # Add to Redis queue with priority
        priority_score = Config.PRIORITY_SCORES[job.priority]
        redis_client.zadd(Config.JOB_QUEUE, {job.job_id: priority_score})
        
        print(f"✅ Job submitted: {job.job_id} ({job.priority} priority)")
        
        return jsonify({
            'message': 'Job submitted successfully',
            'job_id': job.job_id,
            'status': job.status,
            'priority': job.priority,
            'job_type': job.job_type
        }), 201
        
    except Exception as e:
        print(f"❌ Error submitting job: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/jobs/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get status of a specific job"""
    try:
        job = db.get_job(job_id)
        
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify({
            'job_id': job.job_id,
            'job_type': job.job_type,
            'priority': job.priority,
            'status': job.status,
            'retry_count': job.retry_count,
            'created_at': job.created_at,
            'picked_at': job.picked_at,
            'completed_at': job.completed_at,
            'error_message': job.error_message,
            'payload': job.payload
        }), 200
        
    except Exception as e:
        print(f"❌ Error getting job status: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/jobs', methods=['GET'])
def list_jobs():
    """List all jobs with their status"""
    try:
        jobs = db.get_all_jobs()
        
        jobs_data = []
        for job in jobs:
            jobs_data.append({
                'job_id': job.job_id,
                'job_type': job.job_type,
                'priority': job.priority,
                'status': job.status,
                'retry_count': job.retry_count,
                'created_at': job.created_at,
                'picked_at': job.picked_at,
                'completed_at': job.completed_at
            })
        
        return jsonify({
            'jobs': jobs_data,
            'total_jobs': len(jobs_data)
        }), 200
        
    except Exception as e:
        print(f"❌ Error listing jobs: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/queue/stats', methods=['GET'])
def queue_stats():
    """Get queue statistics"""
    try:
        queue_size = redis_client.zcard(Config.JOB_QUEUE)
        
        # Get jobs by status
        all_jobs = db.get_all_jobs()
        status_counts = {}
        for job in all_jobs:
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        return jsonify({
            'queue_size': queue_size,
            'status_counts': status_counts,
            'total_jobs': len(all_jobs)
        }), 200
        
    except Exception as e:
        print(f"❌ Error getting queue stats: {e}")
        return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("🚀 Starting Job Queue API Server...")
    print("📋 Available endpoints:")
    print("   POST /submit-job - Submit a new job")
    print("   GET  /jobs/status/<job_id> - Get job status")
    print("   GET  /jobs - List all jobs")
    print("   GET  /queue/stats - Queue statistics")
    print("   GET  /health - Health check")
    print("\n💡 Start the worker with: python run_worker.py")
    print("🌐 API running on: http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
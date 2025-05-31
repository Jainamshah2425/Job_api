import uuid
import json
from datetime import datetime
from typing import Dict, Any, Optional

class Job:
    def __init__(self, job_type: str, priority: str, payload: Dict[str, Any], 
                 job_id: Optional[str] = None):
        self.job_id = job_id or str(uuid.uuid4())
        self.job_type = job_type
        self.priority = priority
        self.payload = payload
        self.status = 'pending'
        self.retry_count = 0
        self.created_at = datetime.now().isoformat()
        self.picked_at = None
        self.completed_at = None
        self.error_message = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for storage"""
        return {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'priority': self.priority,
            'payload': self.payload,
            'status': self.status,
            'retry_count': self.retry_count,
            'created_at': self.created_at,
            'picked_at': self.picked_at,
            'completed_at': self.completed_at,
            'error_message': self.error_message
        }
    
    def to_json(self) -> str:
        """Convert job to JSON string"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        """Create job from dictionary"""
        job = cls(
            job_type=data['job_type'],
            priority=data['priority'],
            payload=data['payload'],
            job_id=data['job_id']
        )
        job.status = data['status']
        job.retry_count = data['retry_count']
        job.created_at = data['created_at']
        job.picked_at = data['picked_at']
        job.completed_at = data['completed_at']
        job.error_message = data['error_message']
        return job
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Job':
        """Create job from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def mark_picked(self):
        """Mark job as picked by worker"""
        self.status = 'processing'
        self.picked_at = datetime.now().isoformat()
    
    def mark_completed(self):
        """Mark job as completed"""
        self.status = 'completed'
        self.completed_at = datetime.now().isoformat()
    
    def mark_failed(self, error_message: str):
        """Mark job as failed"""
        self.status = 'failed'
        self.completed_at = datetime.now().isoformat()
        self.error_message = error_message
        self.retry_count += 1
    
    def should_retry(self, max_retries: int) -> bool:
        """Check if job should be retried"""
        return self.retry_count < max_retries
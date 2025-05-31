import sqlite3
import json
from typing import List, Optional
from models import Job
from config import Config

class Database:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DATABASE_PATH
        self.init_db()
    
    def init_db(self):
        """Initialize database with jobs table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    picked_at TEXT,
                    completed_at TEXT,
                    error_message TEXT
                )
            ''')
            conn.commit()
    
    def save_job(self, job: Job) -> bool:
        """Save job to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO jobs 
                    (job_id, job_type, priority, payload, status, retry_count, 
                     created_at, picked_at, completed_at, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    job.job_id, job.job_type, job.priority, 
                    json.dumps(job.payload), job.status, job.retry_count,
                    job.created_at, job.picked_at, job.completed_at, job.error_message
                ))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error saving job: {e}")
            return False
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    'SELECT * FROM jobs WHERE job_id = ?', (job_id,)
                )
                row = cursor.fetchone()
                
                if row:
                    return Job.from_dict({
                        'job_id': row['job_id'],
                        'job_type': row['job_type'],
                        'priority': row['priority'],
                        'payload': json.loads(row['payload']),
                        'status': row['status'],
                        'retry_count': row['retry_count'],
                        'created_at': row['created_at'],
                        'picked_at': row['picked_at'],
                        'completed_at': row['completed_at'],
                        'error_message': row['error_message']
                    })
        except Exception as e:
            print(f"Error getting job: {e}")
        
        return None
    
    def get_all_jobs(self) -> List[Job]:
        """Get all jobs"""
        jobs = []
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    'SELECT * FROM jobs ORDER BY created_at DESC'
                )
                
                for row in cursor.fetchall():
                    job = Job.from_dict({
                        'job_id': row['job_id'],
                        'job_type': row['job_type'],
                        'priority': row['priority'],
                        'payload': json.loads(row['payload']),
                        'status': row['status'],
                        'retry_count': row['retry_count'],
                        'created_at': row['created_at'],
                        'picked_at': row['picked_at'],
                        'completed_at': row['completed_at'],
                        'error_message': row['error_message']
                    })
                    jobs.append(job)
        except Exception as e:
            print(f"Error getting all jobs: {e}")
        
        return jobs
    
    def update_job_status(self, job: Job) -> bool:
        """Update job status in database"""
        return self.save_job(job)
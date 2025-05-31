import os

class Config:
    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # Database Configuration
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'jobs.db')
    
    # Job Configuration
    MAX_RETRIES = 3
    RETRY_DELAYS = [1, 4, 9]  # Exponential backoff: 1s, 4s, 9s
    
    # Queue Names
    JOB_QUEUE = 'job_queue'
    
    # Priority Scores (lower = higher priority)
    PRIORITY_SCORES = {
        'high': 1,
        'low': 2
    }
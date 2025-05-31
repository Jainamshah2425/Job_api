import redis
import time
import random
from models import Job
from database import Database
from config import Config

class JobWorker:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB,
            decode_responses=True
        )
        self.db = Database()
        self.running = False
    
    def start(self):
        """Start the worker to process jobs"""
        self.running = True
        print("🚀 Job Worker started! Waiting for jobs...")
        print("⏱️  Polling every 2 seconds...")
        print("🔄 Press Ctrl+C to stop\n")
        
        try:
            while self.running:
                job = self.get_next_job()
                if job:
                    self.process_job(job)
                else:
                    time.sleep(2)  # Wait 2 seconds before checking again
                    
        except KeyboardInterrupt:
            print("\n🛑 Worker stopped by user")
            self.running = False
    
    def get_next_job(self) -> Job:
        """Get the next highest priority job from queue"""
        try:
            # Get job with lowest score (highest priority) from sorted set
            result = self.redis_client.zpopmin(Config.JOB_QUEUE)
            
            if result:
                job_id, priority_score = result[0]
                job = self.db.get_job(job_id)
                
                if job and job.status == 'pending':
                    job.mark_picked()
                    self.db.update_job_status(job)
                    
                    print(f"📥 Picked job: {job.job_id} ({job.priority} priority)")
                    return job
                    
        except Exception as e:
            print(f"❌ Error getting next job: {e}")
        
        return None
    
    def process_job(self, job: Job):
        """Process a job with retry logic"""
        print(f"⚙️  Processing job {job.job_id} (attempt {job.retry_count + 1})")
        print(f"   Type: {job.job_type}")
        print(f"   Priority: {job.priority}")
        print(f"   Payload: {job.payload}")
        
        try:
            # Execute the job based on type
            success = self.execute_job(job)
            
            if success:
                job.mark_completed()
                self.db.update_job_status(job)
                print(f"✅ Job {job.job_id} completed successfully")
            else:
                self.handle_job_failure(job, "Job execution failed")
                
        except Exception as e:
            self.handle_job_failure(job, str(e))
    
    def execute_job(self, job: Job) -> bool:
        """Execute the actual job logic"""
        if job.job_type == 'send_email':
            return self.send_email(job.payload)
        elif job.job_type == 'process_data':
            return self.process_data(job.payload)
        elif job.job_type == 'generate_report':
            return self.generate_report(job.payload)
        else:
            print(f"❌ Unknown job type: {job.job_type}")
            return False
    
    def send_email(self, payload: dict) -> bool:
        """Mock email sending function"""
        try:
            print(f"   📧 Sending email to: {payload.get('to', 'unknown')}")
            print(f"   📧 Subject: {payload.get('subject', 'No Subject')}")
            print(f"   📧 Message: {payload.get('message', 'No Message')}")
            
            # Simulate email sending with some processing time
            time.sleep(random.uniform(1, 3))
            
            # Simulate occasional failures (20% chance)
            if random.random() < 0.2:
                raise Exception("SMTP server temporarily unavailable")
            
            print("   📬 Email sent successfully!")
            return True
            
        except Exception as e:
            print(f"   ❌ Email sending failed: {e}")
            return False
    
    def process_data(self, payload: dict) -> bool:
        """Mock data processing function"""
        try:
            print(f"   🔄 Processing data: {payload.get('data_type', 'unknown')}")
            
            # Simulate data processing
            time.sleep(random.uniform(2, 4))
            
            # Simulate occasional failures (15% chance)
            if random.random() < 0.15:
                raise Exception("Data processing error")
            
            print("   ✅ Data processed successfully!")
            return True
            
        except Exception as e:
            print(f"   ❌ Data processing failed: {e}")
            return False
    
    def generate_report(self, payload: dict) -> bool:
        """Mock report generation function"""
        try:
            report_type = payload.get('report_type', 'unknown')
            print(f"   📊 Generating {report_type} report...")
            
            # Simulate report generation
            time.sleep(random.uniform(3, 5))
            
            # Simulate occasional failures (10% chance)
            if random.random() < 0.1:
                raise Exception("Report generation timeout")
            
            print("   📈 Report generated successfully!")
            return True
            
        except Exception as e:
            print(f"   ❌ Report generation failed: {e}")
            return False
    
    def handle_job_failure(self, job: Job, error_message: str):
        """Handle job failure with retry logic"""
        job.mark_failed(error_message)
        print(f"❌ Job {job.job_id} failed: {error_message}")

        if not job.should_retry(Config.MAX_RETRIES):
            print(f"🚨 Sending alert: Job {job.job_id} permanently failed after {Config.MAX_RETRIES} attempts")
        # Optionally log to a file
            with open("failed_jobs.log", "a") as f:
                f.write(f"{job.job_id}: {error_message}\n")
        
        if job.should_retry(Config.MAX_RETRIES):
            retry_delay = Config.RETRY_DELAYS[job.retry_count - 1]
            print(f"🔄 Retrying job {job.job_id} in {retry_delay} seconds...")
            
            # Update job status
            job.status = 'pending'  # Reset to pending for retry
            self.db.update_job_status(job)
            
            # Wait for exponential backoff delay
            time.sleep(retry_delay)
            
            # Re-add to queue with same priority
            priority_score = Config.PRIORITY_SCORES[job.priority]
            self.redis_client.zadd(Config.JOB_QUEUE, {job.job_id: priority_score})
            
            print(f"📤 Job {job.job_id} re-queued for retry")
        else:
            print(f" Job {job.job_id} permanently failed after {Config.MAX_RETRIES} attempts")
            self.db.update_job_status(job)

if __name__ == '__main__':
    worker = JobWorker()
    worker.start()
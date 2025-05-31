![image](https://github.com/user-attachments/assets/defd7178-c1b1-4813-a131-f56856b543bf)


git clone https://github.com/Jainamshah2425/Job_api.git
cd job_queue
Create Virtual environment
python -m venv venv
source venv/bin/activate  
Install dependencies
pip install -r requirements.txt
Start redis server(using docker or locally)
# Terminal 1: Start API Server
python app.py
# Terminal 2: Start Worker
python run_worker.py
# Terminal 3: Run Tests
python test_api.py
Health
![image](https://github.com/user-attachments/assets/cb144858-3815-4b54-b572-02dcab003b4a)
Submit job
![image](https://github.com/user-attachments/assets/e81a5f35-6d9d-412c-9f72-6d1968afb48b)
Job Status
![image](https://github.com/user-attachments/assets/132538a8-9152-4b20-aa82-b2a4f637ff4b)
All Jobs
![image](https://github.com/user-attachments/assets/1fc28894-4a55-450e-8c91-34108b0847d2)
Status of all jobs
![image](https://github.com/user-attachments/assets/47ef779e-a172-4302-a71c-ac6d0ed0bc8d)
Job is failed after 3 try
![image](https://github.com/user-attachments/assets/36d9f461-dcd4-425a-ac7a-5f8adae742a9)





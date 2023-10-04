# Qlik cloud logs
This is a script which shows how you can pull Qlik audit/reload logs and share them with another platform. In this example the logs are being pushed to Dynatrace. The script is meant to be run on a schedule.

# Prerequisites to get this script running
1. A Qlik Cloud tenant + API key
2. A Dynatrace tenant + API key
3. A S3 bucket + access key + secret access key

# Running the script
1. Edit the env.example file and rename it to .env 
2. install the dependencies with `pip install -r requirements.txt`
3. run `python ./qlik_cloud_logs.py`

# Scheduling
Run this script periodically at a desired interval to fit your use case, e.g., using cron jobs or a task scheduler.
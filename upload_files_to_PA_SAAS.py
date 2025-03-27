"""
Planning Analytics SaaS File Upload Script

Description:
    This script automates the process of uploading files from a local directory to TM1 and create all target directories .
    This script connects to Planning Analytics as a Service Database using REST API with PA API Key. 
        https://www.ibm.com/support/pages/node/7137883
    The log file is saved in the script directory.

HISTORY:
    2025-02-16       akolchugin@cubeiwse.com    Create the script

PARAMETERS:
    <source_folder> - REQUIRED. Local directory with files to upload. All files will be uploaded
    [tm1_target_folder] - OPTIONAL. Target direcory in Planning Analytics. Folders'structure will be automaticaly created
                            By default: ".tmp//Uploads"

Usage:
    python upload_to_tm1.py <source_folder> [tm1_target_folder]

"""


import os
import sys
import time
from io import BytesIO
from datetime import datetime
from TM1py.Services import TM1Service 

# TM1 connection parameters
TM1PARAMS = {
    "base_url": "https://eu-central-1.planninganalytics.saas.ibm.com/api/<TenantId>/v0/tm1/<DatabaseName>/",
    "user": "apikey",
    "password": "<TheActualApiKey>",
    "async_requests_mode": True,
    "ssl": True,
    "verify": True
}

LOG_FILE_NAME = "tm1_upload_log"

def write_log(log_file, message):
    """Write a log message to the log file for tracking."""
    with open(log_file, "a") as log:
        log.write(message + "\n")
    print(message)  

def upload_files_to_tm1(source_folder, tm1_target_folder, tm1params):
    # Ensure source path exists
    if not os.path.exists(source_folder):
        print(f"Error: Source folder '{source_folder}' does not exist.")
        return
    
    # Execution start time
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Log file setup (saved in the script's directory)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(script_dir, f"{LOG_FILE_NAME}_{timestamp}.txt")

    # Initialize log file with execution parameters
    with open(log_file, "w") as log:
        log.write(f"Source Folder: {source_folder}\n")
        log.write(f"TM1 Target Folder: {tm1_target_folder}\n\n")

    total_files = 0
    uploaded_files = 0

    try:
        with TM1Service(**tm1params) as tm1:
            for file_name in os.listdir(source_folder):
                source_file = os.path.join(source_folder, file_name)

                # Skip directories
                if not os.path.isfile(source_file):
                    continue

                total_files += 1
                start_time = time.time()
                file_size_mb = f"{os.path.getsize(source_file) / (1024 * 1024):,.2f} MB"

                # Log start of upload
                log_start = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, STARTING UPLOAD: {file_name}, {file_size_mb}"
                write_log(log_file, log_start)

                try:
                    # Read the file content as bytes
                    with open(source_file, "rb") as f:
                        file_content = BytesIO(f.read())

                    # Construct TM1 target file path dynamically
                    tm1_file_path = f"{tm1_target_folder}/{file_name}"

                    # Upload file to TM1
                    tm1.files.update_or_create(
                        file_name=tm1_file_path,
                        file_content=file_content
                    )

                    end_time = time.time()
                    upload_time = round(end_time - start_time, 4)
                    status = f"Success in {upload_time} s"
                    uploaded_files += 1

                except Exception as e:
                    status = f"Failed ({e})"
                    write_log(log_file, status)

                # Log end of upload
                log_end = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, ENDING UPLOAD:, {tm1_file_path} , {status}"
                write_log(log_file, log_end)

    except Exception as main_error:
        print(f"Unexpected error occurred: {main_error}")
        write_log(log_file, f"Unexpected error occurred: {main_error}")

    # Print final summary and log it
    summary = (
        f"Total files found: {total_files}\n"
        f"Successfully uploaded: {uploaded_files} / {total_files}\n"
    )
    write_log(log_file, summary)
    print(f"Log saved to: {log_file}")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        upload_files_to_tm1(sys.argv[1], sys.argv[2], TM1PARAMS)
    elif len(sys.argv) == 2:
        upload_files_to_tm1(sys.argv[1], ".tmp//Uploads", TM1PARAMS)
    else:
        print("Usage: python upload_to_tm1.py <source_folder> [tm1_target_folder]")

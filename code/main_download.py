import pandas as pd
import sys
import os
import time
import numpy as np
import datetime

# Logging
from v_log import VLogger
import logging
import tqdm

#S3 interaction
from io import StringIO 
import boto3
import json  # to process the outputs of AWS CLI
import subprocess
import re

os.chdir("/home/ec2-user/audio/code")

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------

# FUNCTIONS

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------


####################################################################################
# Create hte command to execute
def comando_youtube(track_id, url, path_audio = 'data/'):
    """
    Once the audio file size has been checked, we will download the worst audio to mp3 format
    path_output = data/
    url = youtube url
    """
    comando1 = f'/root/.local/share/virtualenvs/audio-DWZ8joIe/bin/youtube-dl -ci -f "worstaudio" -x --audio-format mp3 '
    path_output = path_audio +  track_id + "_webm.mp3"
    comando2 = f" --output {path_output}"
    return comando1 + url + comando2
  
####################################################################################
# Functions for the upload

def file_to_S3(local_path, S3_path,  S3_BUCKET = 'tfmdavid'):
    """
    local_path = os.path.join("..","webscrapping","log","WebScrap.log")
    S3_path = nonmatch-query/log.txt
    """
    s3 = boto3.resource('s3')
    resp = s3.Object(S3_BUCKET, S3_path).put(Body=open(local_path, 'rb'))
    return resp

####################################################################################
# Functions for the completion of the upload

def get_now():
    now = datetime.datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    return dt_string

def get_current_instance_id():
    try:
        desc_inst = subprocess.check_output('ec2-metadata -i', shell = True).decode('utf-8')
        return desc_inst.strip().split(": ")[1]
    except:
        return False


# --------------------------------------------------------------------------
# --------------------------------------------------------------------------

# LOG

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
path_local_log_file = f"log/node.log"
log = VLogger(f'Node', uri_log = path_local_log_file, 
                        file_log_level = logging.INFO)

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------

# SQS functions

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
# Create SQS client
sqs = boto3.client('sqs')
URL_q_jobs = "https://sqs.eu-west-2.amazonaws.com/555381533193/jobs_download"
URL_q_status = "https://sqs.eu-west-2.amazonaws.com/555381533193/status"

####################################################################################
# Functions for the SQS interactions

def parse_job_message(response):
    """Parses the response as json to output the batch and iter numbers from message"""
    track_id, yt_url = response["Messages"][0]["Body"].split("::")
    return track_id, yt_url

def get_job():
    """
    Receive message from SQS queue: job_download.fifo
    """
    response = sqs.receive_message(
        QueueUrl=URL_q_jobs,
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=30, # TODO!!! 
        WaitTimeSeconds=0
    )
    return response

def get_id_message(response):
    """From the response of SQS, it parses the identifier (receiptHandler) of the message"""
    return response["Messages"][0]['ReceiptHandle']

def process_job(resp):
    """
    Gets the response from get_job
    """
    # If no message is present in the queue, finish the code
    if "Messages" not in resp:
        log.info("-------------------------------------------------------")
        log.info("QUEUE: no more messages to process")
        return False, False, False

    else:
        # Parse it to obtain batch and number of iteration
        # Add to the log file which batch num and num iter has received
        track_id, yt_url = parse_job_message(resp)
        log.info("-------------------------------------------------------")
        log.info(f"Received message. Track_id: {track_id}, URL: {yt_url}")

        #Get the identifier of that message to delete it
        id_message = get_id_message(resp)
        return track_id, yt_url, id_message
    
def send_status(status, track_id, yt_url):
    """Send message to SQS queue: status.fifo
    instance_name: tagged name of the instance 
    status: if the .mp3 has been uploaded correctly: 1
            if the .mp3 has not been uploaded correctly: 0
            if the .mp3 song download has failed: -1
    date_today: date and hour/minute/second precision to account for  the time of the status
    """
    instance_id = get_current_instance_id()
    date_today = get_now()
    message_body = f'{instance_id}::{status}::{track_id}::{yt_url}::{date_today}'
    response = sqs.send_message(
        QueueUrl=URL_q_status,
        DelaySeconds=0,
        MessageAttributes={},
        MessageBody=(message_body)
    )
    
def delete_message(id_message):
    """Deletes the message, it should be under the visibility timeout interval this command,
    otherwise the message will be re-send to the queue"""
    sqs.delete_message(
    QueueUrl=URL_q_jobs,
    ReceiptHandle=id_message
    )
    return


# --------------------------------------------------------------------------
# --------------------------------------------------------------------------

# Loop

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------

counter_failed = 0

while True:
    # #####################################
    # READ the queue
    #######################################
    response = get_job()
    
    # Parse the Message JSON response
    track_id, yt_url, id_message = process_job(response)
    
    # If no available messages exit the loop
    if track_id is False:
        break
        
    # #####################################
    # Download
    ####################################### 
    
    # ------------------- # 
    #Command to download
    # ------------------- # 
    comando_descargar_audio = comando_youtube(track_id, yt_url)
    
    # ------------------- # 
    # Execute download
    # ------------------- # 
    try:
        comando_output = subprocess.check_output(comando_descargar_audio, shell=True)
        log.info(f"Downloaded audio. Track_id: {track_id}, URL: {yt_url}")
    except Exception as e:
        
        # Update the counter of errors
        counter_failed += 1
        
        # ------------------- # 
        # Check Ban!
        # ------------------- #   
        if counter_failed > 2:

            # Send status of banned
            send_status(-1, track_id, yt_url)

            # Stop execution, since we are banned
            log.info(f"BANNED: We have reach 3 errros consecutively. Track_id: {track_id}, URL: {yt_url}, error: {e}")
            break

        # Log
        log.info(f"ERROR: Downloaded audio. Track_id: {track_id}, URL: {yt_url}, counter: {counter_failed},  error: {e}")
        continue 
        

    
    # ------------------- # 
    # Conversion to mp3
    # ------------------- # 
    try:
        comandoMP3 = f"""
        ffmpeg -i "data/{track_id}_webm.mp3" -vn -ab 64k -ar 44100 -y "data/{track_id}.mp3";
        """
        out = subprocess.check_output(comandoMP3, shell=True)
        log.info(f"Converted audio. Track_id: {track_id}, URL: {yt_url}")
    except:
        log.info(f"ERROR: Converted audio. Track_id: {track_id}, URL: {yt_url}")
        continue

    # ------------------------ # 
    # Remove the previous file
    # ------------------------ # 
    try:
        os.remove(f"data/{track_id}_webm.mp3")
        log.info(f"Removed previous audio. Track_id: {track_id}, URL: {yt_url}")
    except:
        log.info(f"ERROR: Removed previous audio. Track_id: {track_id}, URL: {yt_url}")
        
        
    # ----------------------------------------- # 
    # Upload to S3 and final reporting to status
    # ----------------------------------------- # 
    resp_S3 = file_to_S3(f"data/{track_id}.mp3", f"audio/{track_id}.mp3",  S3_BUCKET = 'tfmdavid')
    
    # Send to STATUS that this track_id has been downloaded if successful upload (200 HTTP code)
    if resp_S3['ResponseMetadata']['HTTPStatusCode'] == 200:

        # First delete the -mp3 in the data/ folder to avoid accumulating .mp3 in the EC2 instance
        os.remove(f"data/{track_id}.mp3")

        # Send a message to "status" queue
        send_status(1, track_id, yt_url)

        # Remove the message from the job_download queue
        try:
            delete_message(id_message)
        except:
            log.info(f"WARNING: id message {id_message} not found for deletion. Track_id: {track_id}, URL: {yt_url}")

        # Log
        log.info(f"COMPLETED. Track_id: {track_id}, URL: {yt_url}")
        
        # Set the counter of failed to 0
        counter_failed = 0
        
    else:
        # Send a message to "status" queue stating that an error uploading to S3 happened
        send_status(0, track_id, "")

        # Log
        log.info(f"ERROR: Uploading to S3 and deleting message. Track_id: {track_id}, URL: {yt_url}")
import boto3
import pandas as pd
import os
import sys
import subprocess
from scipy.io import wavfile
import numpy as np
from scipy.signal.windows import blackmanharris
import wave
import matplotlib.pyplot as plt
import librosa
import librosa.display
import subprocess
import datetime

# Logging
from v_log import VLogger
import logging


# --------------------------------------------------------------------------
# --------------------------------------------------------------------------

# SQS functions

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
# Create SQS client
sqs = boto3.client('sqs')
URL_q_jobs = "https://sqs.eu-west-2.amazonaws.com/555381533193/jobs_specto"
URL_q_status = "https://sqs.eu-west-2.amazonaws.com/555381533193/status_specto"

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------

# LOG

# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
path_local_log_file = f"log/audio.log"
log = VLogger(f'Node', uri_log = path_local_log_file, 
                        file_log_level = logging.INFO)

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
# Functions for audio processing
def get_mp3(track_id):
    """
    Download a track_id from the S3 bucket where we have all the audios
    """
    region_S3 = 'eu-west-2' #specify the region of the S3 bucket, especially if executing this code in a different region than Ireland
    s3 = boto3.client("s3",region_S3)
    
    # Folder
    path_song_S3 = "audio/"
    
    # Bucket name
    bucket_name = 'tfmdavid'
    
    # Save it locally in the tmp folder as song.mp3
    with open('song.mp3', 'wb') as f:
        s3.download_file(bucket_name, path_song_S3 + track_id + ".mp3", "song.mp3")
    return 

def get_size_check(track_id):
    """
    Checks if the original audio is more than 10MB of audio, if it is, returns a False
    """
    s3 = boto3.resource('s3')
    key_mp3 = "audio/" + track_id + ".mp3"
    s3object = s3.Object('tfmdavid',key_mp3)
    file_size = s3object.content_length #size in bytes
    size_megas = file_size / 1000000
    if size_megas > 10:
        return False
    else: 
        return True

def mp3_to_wav():
    """
    Convert mp3 to wav
    """
    comando_convertir_audio = f'ffmpeg -i song.mp3 song.wav -y'
    comando_output = subprocess.check_output(comando_convertir_audio, shell=True) 
    

def apply_median_filter(sample):
    """
    For the sample reduction of noise
    """
    median_filter_thresh = np.median(sample.ravel())
    min_sample = np.min(sample.ravel())
    sample[np.where(sample < median_filter_thresh)]= min_sample
    return sample

####################################################################################
# Functions for status report
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

    
def send_status(status, track_id, window, ini, fin, rows, cols):
    """Send message to SQS queue: status_specto
    instance_id: tagged name of the instance 
    status: if the .jpg have been created and uploaded correctly: 1
            if some error has avoided this upload to be processed correctly: 0
    track_id: which track_id has been analyzed
    window: which window number it is (from 0 to N for the same track_id)
    ini: starting seconds of that window
    fin: ending sconds of that window
    rows: size of the image, number of rows
    cols: size of the image, number of columns
    date_today: date and hour/minute/second precision to account for  the time of the status
    """
    instance_id = get_current_instance_id()
    date_today = get_now()
    message_body = f'{instance_id}::{status}::{track_id}::{window}::{ini}::{fin}::{rows}::{cols}::{date_today}'
    response = sqs.send_message(
        QueueUrl=URL_q_status,
        DelaySeconds=0,
        MessageAttributes={},
        MessageBody=(message_body)
    )

####################################################################################
# Functions for SQS
def parse_job_message(response):
    """Parses the response as json to output the track_id"""
    return response["Messages"][0]["Body"]

def get_id_message(response):
    """From the response of SQS, it parses the identifier (receiptHandler) of the message"""
    return response["Messages"][0]['ReceiptHandle']

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
        VisibilityTimeout=120, # if in 2 minutes it has not done everything until deleting it... resend it!!! 
        WaitTimeSeconds=0
    )
    return response

def process_job(resp):
    """
    Gets the response from get_job
    """
    # If no message is present in the queue, finish the code
    if "Messages" not in resp:
        log.info("-------------------------------------------------------")
        log.info("QUEUE: no more messages to process")
        return False, False

    else:
        # Parse it to obtain batch and number of iteration
        # Add to the log file which batch num and num iter has received
        track_id = parse_job_message(resp)
        log.info("-------------------------------------------------------")
        log.info(f"Received message. Track_id: {track_id}")

        #Get the identifier of that message to delete it
        id_message = get_id_message(resp)
        return track_id, id_message

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

while True:
    # #####################################
    # READ the queue
    #######################################
    response = get_job()
    
    # Parse the Message JSON response
    track_id, id_message = process_job(response)
    
    # If no available messages exit the loop
    if track_id is False:
        break

    # If the size of the audio is > 10MB, continue
    if get_size_check(track_id) is False:
        # Delete that message
        delete_message(id_message)
        continue
        
    # #####################################
    # Create .wav
    #######################################
    #Get mp3 from S3 as song.mp3
    get_mp3(track_id)

    # Convert to wav as song.wav
    mp3_to_wav()

    # Downsample the audio to 16k
    y, s = librosa.load("song.wav", sr = 16000)
    
    # #####################################
    # Set FFT properties 
    #######################################
    nfft = 1024 # in samples
    hop = int(nfft/2) # in samples
    duration = len(y) / s # in seconds
    number_mels = 256 # 256 values in the y-axis
    freq_max = 8000 # reaching a max freq of 8 kHz
    
    # #####################################
    # Mel Spectogram
    #######################################
    # Spectogram
    S = librosa.feature.melspectrogram(y=y, sr=s, n_mels=number_mels, fmax=freq_max, n_fft = nfft, hop_length = hop)
    
    # Power to db
    S_dB = librosa.power_to_db(S, ref = np.max)
    
    # Set the duration in seconds
    duration = np.floor(len(y) / s) # in seconds

    # Set the windows length of 30 seconds
    window_length = 30

    # Allow overlap of 10 seconds by moving the window 20 seconds
    move_window_each = 20 #seconds
    
    # #####################################
    # Create the windows
    #######################################
    # Windows specified in seconds
    w_list = []
    act_point = 0 # actual point of the start of the window

    while True:
        fin_act_point = act_point + window_length # suposed ending position of the actual window

        # if it has not surpassed yet the last position
        if fin_act_point <= duration: 
            w_act = (int(act_point), int(fin_act_point))
            w_list.append(w_act)

            # Update (moving window) the act_point for the next iter
            act_point += move_window_each

            # If the next starting position is superior than the duration, end here the loop
            if act_point >= duration:
                break

        else:
            # We have surpassed the end in the fin_act_point. So we make a final window
            # with the last position as the duration, and the inital as duration - window_length
            w_list.append((int(duration - window_length), int(duration)))
            break

    # #####################################
    # Save images
    #######################################
    
    # Save the shapes of each image
    shapes_imgs = dict() # each key is the window number (num_win)

    for num_win, win in enumerate(w_list):
        start_pos = win[0] # in seconds
        end_pos = win[1] # in seconds

        # Equivalent sample step in time (ms) for the STFT
        STFT_sample_step_ms = hop / s

        # Sample range (convert seconds range to sample range to slice the spectrogram matrix)
        ssii = int(start_pos / STFT_sample_step_ms)
        ssff = int(end_pos / STFT_sample_step_ms)

        # Window sample (of 30 seconds)
        sample = S_dB[:,ssii:ssff]

        # Apply median filter
        sample = apply_median_filter(sample)

        # Save the image locally
        plt.imsave(f'{track_id}__{num_win}__{start_pos}__{end_pos}.jpg', sample, cmap='gray_r', origin='lower')

        # Update the shape
        shapes_imgs[num_win] = sample.shape
        
        
    # #####################################
    # S3 upload each window image saved
    #######################################
    
    list_jpgs = os.listdir(".")
    list_jpgs = [jpgs for jpgs in list_jpgs if '.jpg' in jpgs]

    # Remove the .mp3 and .wav files
    os.remove("song.mp3")
    os.remove("song.wav")

    # Loop over the images saved
    for jpg in list_jpgs:

        # Parse the names
        jpg = jpg.split(".")[0] # name of the file without extension
        parse_jpg = jpg.split("__")
        j_track = parse_jpg[0] # track_id
        j_win = int(parse_jpg[1]) # identifier of the window
        j_ini = parse_jpg[2] #initiate window at second j_ini
        j_fin = parse_jpg[3] #end window at second j_fin


        # ----------------------------------------- # 
        # Upload to S3 and final reporting to status
        # ----------------------------------------- # 
        local_file = f"{jpg}.jpg"
        resp_S3 = file_to_S3(local_file, f"spec/{jpg}.jpg",  S3_BUCKET = 'tfmdavid')

        # ----------------------------------------- # 
        # Get the size of the uploaded image
        # ----------------------------------------- # 
        j_row = "0"
        j_col = "0"
        # Get the size of that window
        if j_win in shapes_imgs:
            sh = shapes_imgs[j_win]
            j_row = sh[0]
            j_col = sh[1]
            
        # ----------------------------------------- # 
        # Report to status_specto queue and remove jpg
        # ----------------------------------------- # 

        # Send to STATUS that this track_id has been downloaded if successful upload (200 HTTP code)
        if resp_S3['ResponseMetadata']['HTTPStatusCode'] == 200:

            # Remove this jpg
            os.remove(local_file)

            # Send a message to "status" queue       
            send_status(1, j_track, j_win, j_ini, j_fin, j_row, j_col)

            # Remove the message from the job_download queue
            try:
                delete_message(id_message)
            except:
                log.info(f"WARNING: id message {id_message} not found for deletion. Track_id: {j_track}")

            # Log
            log.info(f"COMPLETED. Track_id: {track_id}, Window: {j_win}")

        else:
            # Error in the UPLOAD
            # Send a message to "status" queue for that error      
            send_status(0, j_track, j_win, j_ini, j_fin, j_row, j_col)

            # Log
            log.info(f"ERROR: Uploading to S3 and deleting message. Track_id: {j_track}")

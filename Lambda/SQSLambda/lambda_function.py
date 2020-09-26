import json
import boto3
import psycopg2
import os

# SNS
def send_request(body, subject_send):
    # Create an SNS client
    sns = boto3.client('sns')
 
    # Publish a simple message to the specified SNS topic
    response = sns.publish(
        TopicArn=os.environ['email_topic'],    
        Subject=subject_send,
        Message=body
        )


def lambda_handler(event, context):
    
    # Client
    s3 = boto3.client("s3")
    data = event["Records"][0]["body"]
    
    # Prepare the query
    data = data.split("::")
    try:
        instance_id = data[0]
        stat = data[1]
        track_id = data[2]
        yt_url = data[3]
        date = data[4]
    except:
        subject_send = "AWSERROR: data splitting SQS message"
        text_email =  f"Error splitting data: {data}"
        send_request(text_email, subject_send)
        return {'statusCode': 200, 'body': json.dumps(f'Nothing uploaded'), 'see': data}
    
    
    query_insert = f"""
    INSERT INTO status (instance_id, track_id, yt_url, date, stat) VALUES ('{instance_id}','{track_id}','{yt_url}','{date}','{stat}')
    """.strip()

    ENDPOINT="tracksurl.czjs6btlvfgd.eu-west-2.rds.amazonaws.com"
    PORT="5432"
    USR="david"
    REGION="eu-west-2"
    DBNAME="postgres"
    PSSWD=["qrks","jfut","iv","uf","1"]
    
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=''.join(PSSWD))
    cur = conn.cursor()
    cur.execute(query_insert)
    conn.commit()
    conn.close()
    
    # If stat = -1 or 0 report it to SNS
    if stat == "0":
        subject_send = f"AWSERROR UPLOADING/DOWNLOADING TRACK"
        text_email =  f"AWSERROR: downloading/uploading track: {track_id} in instance: {instance_id}"
        send_request(text_email, subject_send)
        
    if stat == "-1":
        subject_send = f"AWSERROR BANNED"
        text_email =  f"AWSERROR: banned for track: {track_id} in instance: {instance_id}"
        send_request(text_email, subject_send)
        
    return {'statusCode': 200, 'body': json.dumps(f'Correctly uploaded {track_id} status')}
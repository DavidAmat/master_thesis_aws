import json
import boto3
import psycopg2
import os

def lambda_handler(event, context):
    
    # Client
    s3 = boto3.client("s3")
    data = event["Records"][0]["body"]
    
    # Prepare the query
    data = data.split("::")
    try:
        instance_id = data[0]
        stat = int(data[1])
        track_id = data[2]
        win = int(data[3])
        ini =int(data[4])
        fin = int(data[5])
        rows = int(data[6])
        cols = int(data[7])
        date = data[8]
    except:
        return {'statusCode': 200, 'body': json.dumps(f'Nothing uploaded'), 'see': data}
    
    
    query_insert = f"""
    INSERT INTO public.status_specto (instance_id, stat, track_id, win, ini, fin, rows, cols, date)
	VALUES ('{instance_id}', {stat},  '{track_id}', {win}, {ini}, {fin}, {rows}, {cols}, '{date}')
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

        
    return {'statusCode': 200, 'body': json.dumps(f'Correctly uploaded {track_id} status')}
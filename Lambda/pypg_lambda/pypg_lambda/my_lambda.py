import json
import boto3
import psycopg2


def lambda_handler(event, context):
    
    # Client
    s3 = boto3.client("s3")
    data = json.loads(event["Records"][0]["Body"])
    
    # Prepare the query
    instace_id = data[0]
    stat = int(data[1])
    track_id = data[2]
    yt_url = data[3]
    date = data[4]
    
    query_insert = f"""
    INSERT INTO results VALUES ('{instace_id}',{stat},'{track_id}','{yt_url}','{date}')
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

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


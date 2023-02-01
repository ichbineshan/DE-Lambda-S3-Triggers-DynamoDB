import boto3
import os

bucket_name = os.getenv('bucket_name')

s3_client = boto3.client("s3")
dynamodb= boto3.resource('dynamodb')
table= dynamodb.Table('employees')

def lambda_handler(event, context):
    # bucket_name = event ['Records'][0]['s3']['bucket']['name']
    s3_file_name = event ['Records'][0]['s3']['object']['key']
    
    if s3_file_name == 'employeedata.csv':
        resp= s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
        data = resp['Body'].read().decode("utf-8")
        employees = data.split("\n")
        for i in range(1,len(employees)):
            items=employees[i].split(",")
            ddmmyyyy=items[4].split("-")
            ddmmyyyy.reverse()              #converts to yyyymmdd
            # Add it to dynamodb
            table.put_item(
            Item = {
            "id" : int(items[0]),
            "dept" : items[1],
            "userName" : items[2]+" "+items[3],
            "DOB" : "-".join(ddmmyyyy)
            }
            )
    else :
        print("Check the file name!!")
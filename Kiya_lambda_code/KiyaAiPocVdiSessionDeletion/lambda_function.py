import json
import boto3
import os
import paramiko
import uuid
import time
Dynamo_InstanceTb="Kiya_Ai_Phase2_Instance_Details_Tb"
dynamo_client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')
def lambda_handler(event, context):
    print(event)

    SessionID=event["queryStringParameters"]["SessionID"]
    instance_ip=event["queryStringParameters"]["InstanceIp"]
    
    deleted_session = False
    
    deleted_user=True
    
    
    
    s3_client = boto3.client("s3")
    client_ec2 = boto3.client('ec2')
    response = client_ec2.describe_instances(
    Filters=[
        {
            'Name': 'ip-address',
            'Values': [instance_ip],
        },
    ]
    )
    
    reservations = response.get('Reservations', [])
    
    if reservations:
        # Assuming there's only one instance per IP, you can get the first instance
        instance_id1 = reservations[0]['Instances'][0]

    else:
        response = {"StatusCode":404,"Message":f"Details Not Found.Please verify once"}
        return response


    keypath="kiya-poc-08-2k23.pem"
    instance_id=instance_id1["InstanceId"]
    print(instance_id)
    dynamo_response = dynamo_client.query(
        TableName = Dynamo_InstanceTb,
        KeyConditionExpression='InstanceId =:ID',
        ExpressionAttributeValues={':ID':{'S':str(instance_id)}}
    )
    if dynamo_response['Items']:
        
        # downloading pem filr from S3
        s3_client.download_file("kiya-ai-poc", keypath, f"/tmp/{keypath}")
        # reading pem file and creating key object
        key = paramiko.RSAKey.from_private_key_file(f"/tmp/{keypath}")
        # an instance of the Paramiko.SSHClient
        ssh_client = paramiko.SSHClient()
        # setting policy to connect to unknown host
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
        print("Connecting to : " + instance_ip)
        # connecting to serverpassword=password
        ssh_client.connect(hostname=instance_ip, username="ubuntu", pkey=key)
        
        
        ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(f"sudo dcv  close-session {SessionID}-session ")
        
    
        error = ssh_stderr.read().decode()
        if error:
            print(f"Error while terminating the session: {error}")
        else:
            deleted_session = True
            print(f"Session {SessionID}-session terminated successfully.")
        # killall -u username
 
        
        ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(f"sudo pkill -9 -u  {SessionID}")
        error = ssh_stderr.read().decode()
        
        print(error)
    
        ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(f"sudo deluser --remove-home {SessionID}")
        
    
        error = ssh_stderr.read().decode()
        if error:
            print(f"Error while terminating the session: {error}")
        else:
            deleted_user = True
            print(f"User {SessionID} deleted successfully.")
            

        Sessions_Num = int(dynamo_response['Items'][0]['Sessions']['S'])
        
        if Sessions_Num==2:
            update_session_count=1
            response = dynamo_client.update_item(
            TableName=Dynamo_InstanceTb,
            Key={'InstanceId': {'S': instance_id}},
            UpdateExpression="SET Sessions = :SN",
            ExpressionAttributeValues={
                ":SN": {'S': str(update_session_count)}
            }
        )
            
        elif Sessions_Num==1:
            response = client_ec2.terminate_instances(InstanceIds=[instance_id])
            table = dynamodb.Table(Dynamo_InstanceTb)
            response = table.delete_item(
                Key={
                    'InstanceId': instance_id
                }
            )
        
            
         
        
        if deleted_session and deleted_user :
    
            
            response={
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps({
                    "Message ": f"User {SessionID} and Session {SessionID}-session terminated successfully."
                })
            }
            return response
                
        else:
            response={
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps({
                    "Message ": f"User {SessionID} and Session {SessionID}-session failed to terminate."
                })
            }
            
            return response
        
    else:
        response={
                "statusCode": 404,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps({
                    "Message ": f"Details Not Found.Please verify once"
                })
            }
        
        return response
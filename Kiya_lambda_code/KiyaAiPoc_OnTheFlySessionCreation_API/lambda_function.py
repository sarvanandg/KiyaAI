import json
import boto3
import paramiko
import json



keypath="kiya-poc-08-2k23.pem"


s3_client = boto3.client("s3")
def lambda_handler(event, context):

    print(event)
    
    try:
        SessionID=event["queryStringParameters"]["SessionID"]
    except:
        SessionID=event["SessionID"]
        
    
    try:
        instance_ip=event["queryStringParameters"]["InstanceIp"]
    except:
        instance_ip=event["InstanceIp"]
    
    
    
    # downloading pem filr from S3
    
    s3_client.download_file("kiya-ai-poc", keypath, f"/tmp/{keypath}")
    key = paramiko.RSAKey.from_private_key_file(f"/tmp/{keypath}")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=instance_ip, username="ubuntu", pkey=key)
    
    
    ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(f"sudo dcv create-session --owner ubuntu --user {SessionID} {SessionID}-session ")
    ssh_stdin.flush()
    
    

    # ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(f"sudo dcv set-display-layout --session {SessionID}-session 1920x1080+0+0 ")
    # ssh_stdin.flush()
    
    ssh_client.close()
    
    url=f"https://{instance_ip}:8443/#{SessionID}-session"
    
    return {'statusCode': 301,'headers': {'Location': url}}





# keypath="kiya-poc-08-2k23.pem"
# instance_id ="i-07a38df8f85556c0e"
# instance_ip="15.207.19.21"
# s3_client = boto3.client("s3")
# def lambda_handler(event, context):
#     SessionID=event["queryStringParameters"]["SessionID"]
    
#     # downloading pem filr from S3
#     s3_client.download_file("kiya-ai-poc", keypath, f"/tmp/{keypath}")
#     key = paramiko.RSAKey.from_private_key_file(f"/tmp/{keypath}")
#     ssh_client = paramiko.SSHClient()
#     ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     ssh_client.connect(hostname=instance_ip, username="ubuntu", pkey=key)
#     print("Connected to :" + instance_ip)
    
#     ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(f"sudo dcv create-session --type console --owner ubuntu --user {SessionID} {SessionID}-session ")
    
#     ssh_stdin.flush()
#     ssh_client.close()
    
#     url=f"https://{instance_ip}:8443/#{SessionID}-session"
    
#     return {'statusCode': 301,'headers': {'Location': url}}
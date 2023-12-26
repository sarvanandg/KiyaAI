import json
import boto3
import os
import paramiko
import uuid
import time
import Configurations as config_data

ec2_client = boto3.client("ec2")
ec2_resource = boto3.resource('ec2')
s3_client = boto3.client("s3")
user_detais_table = config_data.USER_DETAILS_TABLE_NAME
dynamo_resource = boto3.resource('dynamodb',region_name="ap-south-1").Table(user_detais_table)
dynamo_client = boto3.client('dynamodb',region_name="ap-south-1")



def db_update(request_id,created_user,session_url_list):
    if created_user:
        update_expression = "SET SessionUrls = :urls, DebuggingStatus=:ds, ApiStatus=:st"
        expression_attribute_values = {":urls": session_url_list,":ds": "Lambda 5 Successfully Executed",":st":"Sessions Generated Successfully"}
        
        response = dynamo_resource.update_item(
            Key={'RequestId': request_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )

def session_creation_in_ec2(instance_details):
    usernames_passwords = []
    session_url_list = []
    created_user = False
    keypath="kiya-poc-08-2k23.pem"
    
    # downloading pem filr from S3
    s3_client.download_file("kiya-ai-poc", keypath, f"/tmp/{keypath}")
    # reading pem file and creating key object
    key = paramiko.RSAKey.from_private_key_file(f"/tmp/{keypath}")
    # an instance of the Paramiko.SSHClient
    ssh_client = paramiko.SSHClient()
    # setting policy to connect to unknown host
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    for instance in instance_details:
        for inst in instance:
            instance_id = inst
            print("instance",instance)
            instance_response = ec2_resource.Instance(instance_id)
            instance_ip = instance_response.public_ip_address
            
            print("Connecting to : " + instance_ip)
            # connecting to serverpassword=password
            ssh_client.connect(hostname=instance_ip, username="ubuntu", pkey=key)
            # ssh_client.connect(hostname=instance_ip, username="ubuntu", password="Admin@1234")
            print("Connected to :" + instance_ip)
            
            Flag=True
            
            for _ in range(int(instance[instance_id])):
                random_uuid = uuid.uuid4()
                # Convert UUID to string and extract first 5 characters
                unique_identifier = str(random_uuid)[:5]
                # New user details
                new_username = "user" +unique_identifier  # Convert i to a string
                new_user_password = "Password" + unique_identifier  # Convert i to a string
            
                
                # Run user creation commands via SSH
                ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command(f"sudo adduser {new_username} ")
    
                ssh_stdin.write(f"{new_user_password}\n")
                ssh_stdin.flush()
    
                ssh_stdin.write(f"{new_user_password}\n")
                ssh_stdin.flush()
    
               # Wait for the prompt for Full Name and skip it
                ssh_stdout.channel.recv(1024)
                ssh_stdin.write(f"{new_username}\n") 
                
                # Wait for the prompt for Room Number and skip it
                ssh_stdout.channel.recv(1024)
                ssh_stdin.write("\n") 
                
                # Wait for the prompt for Work Phone and skip it
                ssh_stdout.channel.recv(1024)
                ssh_stdin.write("\n") 
                
                # Wait for the prompt for Home Phone and skip it
                ssh_stdout.channel.recv(1024)
                ssh_stdin.write("\n") 
                
                # Wait for the prompt for Other and skip it
                ssh_stdout.channel.recv(1024)
                ssh_stdin.write("\n") 
                
                # Confirm the information
                ssh_stdin.write("Y\n")
                ssh_stdin.flush()
    
                print("user created command successfully excuted")
        
                # if Flag:
                #     ssh_stdin, ssh_stdout, ssh_stderr = ssh_client.exec_command("sudo dcv close-session console")
                #     ssh_stdin.flush()
                #     time.sleep(0.2)
                #     Flag=False
                
                url="https://air12guyt0.execute-api.ap-south-1.amazonaws.com/dev"
                session_url = f"{url}/vdi?InstanceIp={instance_ip}&SessionID={new_username}"
                
                # usernames_passwords.append({"username": new_username, "password": new_user_password,"sessionId":f"https://{instance_ip}:8443/#{new_username}-session"})
                usernames_passwords.append({"username": new_username, "password": new_user_password,"sessionIdUrl":session_url})
                session_url_list.append(session_url)
                created_user = True
                
        
       
        # Close SSH connection outside of the loop
        ssh_client.close()
        
    return created_user,session_url_list,usernames_passwords


def get_details_from_db(request_id):
    instance_and_count_list = []

    try:
        dynamo_response = dynamo_client.query(
        TableName = config_data.USER_DETAILS_TABLE_NAME,
        KeyConditionExpression='RequestId =:RID',
        ExpressionAttributeValues={':RID':{'S':request_id}}
        )
        
        print("dynamo_response:-",dynamo_response)
        
        if "InstanceId" in dynamo_response['Items'][0]:
            InstanceId = dynamo_response['Items'][0]['InstanceId']['M']
        
        print("InstanceId",InstanceId)
            
        for instance in InstanceId:
            instance_and_count_list.append({instance:InstanceId[instance]['S']})
        
        return instance_and_count_list
    except Exception as e:
        print(f"Exception in getting_instance_list function :-- {e}")


def lambda_handler(event, context):
    print("Event-->",event)
    
    request_id = event["RequestId"]
    
    instance_details=get_details_from_db(request_id)
    print(instance_details)
    
    created_user,session_url_list,usernames_passwords=session_creation_in_ec2(instance_details)
    
    db_update(request_id,created_user,session_url_list)
        
    print("User Details :-->",usernames_passwords)

import json
import boto3
import uuid
import Configurations as config_data

user_detais_table = config_data.USER_DETAILS_TABLE_NAME

ec2 = boto3.client('ec2', region_name="ap-south-1")
dynamo_resource = boto3.resource('dynamodb',region_name="ap-south-1").Table(user_detais_table)
dynamo_client = boto3.client('dynamodb',region_name="ap-south-1")


def launch_ec2s(instance_count,request_id):
    unique_id=str(uuid.uuid4())
    # Launch EC2 instances
    ec2_response = ec2.run_instances(
        ImageId=config_data.AMI_ID,
        InstanceType=config_data.INSTANCE_TYPE,
        KeyName=config_data.KEY_PAIR_NAME,
        SecurityGroupIds=config_data.SECURITY_GROUP_IDS,
        MinCount=instance_count,
        MaxCount=instance_count,
        TagSpecifications=[
        {
        'ResourceType': 'instance',
        'Tags': [
            {'Key': 'Name', 'Value': f'Kiya_Ai_Testing-{request_id}'},
            {'Key': 'id', 'Value': unique_id}
        ]
        }])
    return ec2_response


def db_update(session_required,request_id,instances,UserUpdateBulkList):
    new_instance_ids={}
    
    for instance in instances:
        # instance_id_list.append(instance)
        
        if session_required !=0 :
            if int(session_required)%2==0:
                
                dynamo_client.put_item(TableName=config_data.INSTANCE_DETAILS_TABLE_NAME,Item = {
                    'InstanceId':{'S':instance},
                    'Sessions':{'S':"2"}
                })
                
                new_instance_ids.update({instance:  "2"})
                session_required=session_required-2
            else:

                dynamo_client.put_item(TableName=config_data.INSTANCE_DETAILS_TABLE_NAME,Item = {
                    'InstanceId':{'S':instance},
                    'Sessions':{'S':"1"}
                })
                
                new_instance_ids.update({instance:  "1"})
                session_required=session_required-1
            
    new_instance_ids.update(UserUpdateBulkList)    
    # Construct the update expression and attribute values
    update_expression = "SET InstanceId = :ids,DebuggingStatus = :ds"
    expression_attribute_values = {":ids": new_instance_ids,":ds": "Lambda 3 Successfully Executed"}
    
    dynamo_resource.update_item(
        Key={'RequestId': request_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
    )
    


def lambda_handler(event, context):
    print("Event-->",event)
    instance_list = []
    # if "instance_count" in event:
    instance_count = event["Ec2ToBeLaunchCount"]
    session_required = event["RequestedSessions"]
    # instance_count = 1
    # session_required = 5
    request_id = event["RequestId"]
    # request_id = "jfdslkjasdfildj"
    UserUpdateBulkList = event["UserUpdateBulkList"]
    
    ec2_response = launch_ec2s(instance_count,request_id)
    print("EC2 Response-->",ec2_response)
    
    instances = ec2_response["Instances"]
    
    for instance in instances:
        instance_id = instance["InstanceId"]
        instance_list.append(instance_id)
    
    # Print instance IDs
    print(f"Instance List :--{instance_list}")
    # instance_list=["i-ldsjsdlflsdfkk","i-lkjdsflsdfsldkjf","i-sdlsdfjdjoijoie"]
    # instance_list=["i-ldsksdlflsdfkk","i-lkjdsflsefsldkjf","i-sdlsdfjdjojjoie"]
    # instance_id_list=[]
    
    db_update(session_required,request_id,instance_list,UserUpdateBulkList)
    
    print(f"RequestId:- {request_id}")
    return {"RequestId":request_id}
    
            
    # print(f"Instance ids list--->{instance_id_list}")

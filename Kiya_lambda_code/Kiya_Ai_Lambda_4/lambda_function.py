import json
import boto3
import Configurations as config_data

dynamo_client = boto3.client('dynamodb',region_name="ap-south-1")
ec2_client = boto3.client('ec2', region_name="ap-south-1")
dynamo_resource = boto3.resource('dynamodb',region_name="ap-south-1").Table(config_data.USER_DETAILS_TABLE_NAME)



def status_update(request_id):
    update_expression = "SET DebuggingStatus = :ds"
    expression_attribute_values = {":ds": "Lambda 4-All Instances Running"}
    
    dynamo_resource.update_item(
        Key={'RequestId': request_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
    )
    

def checking_instance_status(instance_id_list):
    instance_status_list = []
    try:
        response = ec2_client.describe_instance_status(InstanceIds=instance_id_list,IncludeAllInstances=True)
        print(f"Instance status response :-- {response}")
        instances = response['InstanceStatuses']

        for instance in instances:
            instance_id = instance['InstanceId']
            instance_state = instance['InstanceState']["Name"]
            instance_status = instance["InstanceStatus"]["Status"]
            system_status = instance["SystemStatus"]["Status"]
            print(f"Instance {instance_id} is in state: {instance_state}")
            if instance_state == "running" and instance_status =="ok" and system_status=="ok":
                instance_status_list.append("True")
            else:
                instance_status_list.append("False")
                
        return instance_status_list

    except Exception as e:
        print(f"Exception is in checking_instance_status function : {e}")
    

def getting_instance_list(request_id):
    instance_id_list = []

    try:
        dynamo_response = dynamo_client.query(
        TableName = config_data.USER_DETAILS_TABLE_NAME,
        KeyConditionExpression='RequestId =:RID',
        ExpressionAttributeValues={':RID':{'S':request_id}}
        )
        
        print("dynamo_response:-",dynamo_response)
        
        if "InstanceId" in dynamo_response['Items'][0]:
            InstanceId = dynamo_response['Items'][0]['InstanceId']['M']
            
        for instanace in InstanceId:
            instance_id_list.append(instanace)
        
        return instance_id_list
    except Exception as e:
        print(f"Exception in getting_instance_list function :-- {e}")
    
    

def lambda_handler(event, context):
    print("Event----->",event)
    
    try:
        # request_id = "jfdslkjasdfildj"
        request_id = event["RequestId"]
        
        instance_id_list=getting_instance_list(request_id)
        
        print(f"Instance list:-->{instance_id_list}")
        
        
        instance_status=checking_instance_status(instance_id_list)
        
        print(f"Instances Status-->{instance_status}")
        
        if "False" in instance_status:
            status = False
            print("Some instances are still in pending state")
        else:
            print("All instance are running")
            status = True
            status_update(request_id)
            
        
        print(f"RequestId and Status:{request_id},{status}")
        return {"RequestId":request_id,"Status":status}
    except Exception as e:
        print(f"Exception in lambda_handler function :-- {e}")
    
    
    
    

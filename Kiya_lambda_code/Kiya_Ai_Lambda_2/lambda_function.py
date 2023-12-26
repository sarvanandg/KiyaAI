import json
import boto3
import Configurations as config_data
import math

dynamo_client = boto3.client('dynamodb')

dynamo_resource = boto3.resource('dynamodb',region_name="ap-south-1").Table(config_data.USER_DETAILS_TABLE_NAME)

    

def filter_out_instance(available_list,partial_list,available_session_count,required_session_count):
    final_instance_list=[]
    request_fulfill=False
    count=0
    
    if required_session_count%2==0:
        for item in  available_list:
            item.update({'db_sessions':'2'})
            final_instance_list.append(item)
            count+=2
            if count==required_session_count:
                request_fulfill=True
                break
            
        required_session_count=required_session_count-count
        
        count=0
        if not request_fulfill:
            for item in  partial_list:
                item.update({'db_sessions':'2'})
                final_instance_list.append(item)
                count+=1
                if count==required_session_count:
                    request_fulfill=True
                    break
        required_session_count-=count
    else:
        if required_session_count ==1:
            for item in  partial_list:
                item.update({'db_sessions':'2'})
                item.update({'Sessions':'1'})
                
                final_instance_list.append(item)
                request_fulfill=True
                required_session_count=0

                break
            if not request_fulfill:
                for item in  available_list:
                    
                    item.update({'db_sessions':'2'})
                    item.update({'Sessions':'1'})

                    final_instance_list.append(item)
                    request_fulfill=True
                    required_session_count=0
                    break
        else:
  

            for item in  available_list:
                
                if required_session_count==1:
                    if  partial_list:
                        break
                    else:
                        item.update({'db_sessions':'2'})
                        final_instance_list.append(item)
                        required_session_count-=1
                else:
                    item.update({'db_sessions':'2'})
                    final_instance_list.append(item)
                    
                    required_session_count-=2


                if required_session_count==0:
                    request_fulfill=True
                    break

            if not request_fulfill:
                for item in  partial_list:
                    item.update({'db_sessions':'2'})

                    final_instance_list.append(item)
            
                    required_session_count-=1
                
                    if required_session_count==0:
                        request_fulfill=True
                        break



    return final_instance_list,required_session_count,request_fulfill
            

    
def scan_dynamo_db_recursively(required_session_count):
    
    available_session_count=0
    
    available_list = []
    
    partial_list = []
    
    last_evaluated_key = None
    # try:
    while True:
        if last_evaluated_key:
            
            response = dynamo_client.scan(
                TableName=config_data.INSTANCE_DETAILS_TABLE_NAME,
                ExclusiveStartKey=last_evaluated_key,
                FilterExpression="Sessions = :value_0 OR Sessions = :value_1",
                ExpressionAttributeValues={
                    ":value_0": {"S": "0"},
                    ":value_1": {"S": "1"}
                    }
                )

        else:
            response = dynamo_client.scan(
            TableName=config_data.INSTANCE_DETAILS_TABLE_NAME,
            FilterExpression="Sessions = :value_0 OR Sessions = :value_1",
            ExpressionAttributeValues={
                ":value_0": {"S": "0"},
                ":value_1": {"S": "1"}
                }
            )
        print(response)
            

        for item in response['Items']:
            
            SessionCount=int(item['Sessions']['S'])

            
            
            if SessionCount==0:
                available_dict={'Sessions': SessionCount, 'InstanceId': item['InstanceId']['S']}
                available_list.append(available_dict)
            else:
                partial_dict={'Sessions': SessionCount, 'InstanceId': item['InstanceId']['S']}
                partial_list.append(partial_dict)
            
        
            if SessionCount==0:#handling 0 case 
                SessionCount=2
                
            available_session_count+=int(SessionCount)
            
            if available_session_count>= required_session_count:
                break

        last_evaluated_key=response.get('LastEvaluatedKey')

        if not last_evaluated_key or available_session_count>= required_session_count :
            print(f"LastEvaluatedKey:-{last_evaluated_key}")
            break
            
    
    
    # except Exception as e:
    #     print(f"Exception in lambda handler while loop:- {e} and LastEvaluatedKey:- {last_evaluated_key}")

    return available_list,partial_list,available_session_count
    
    
def get_details(RequestId):
    dynamo_response = dynamo_client.query(
        TableName = config_data.USER_DETAILS_TABLE_NAME,
        KeyConditionExpression='RequestId =:RID',
        ExpressionAttributeValues={':RID':{'S':RequestId}}
    )
    
    if "RequestedSessions" in dynamo_response['Items'][0]:
        required_session_count = int(dynamo_response['Items'][0]['RequestedSessions']['S'])
    return required_session_count
    
    
def update_instance_detail_in_user_db(RequestId,final_instance_list):
    
    #for instnace wala db
    bulk_update_user_tb={}
    
    for item in final_instance_list:
        session_count=item['db_sessions']
        
        user_session_count=item['Sessions']
        
        instance_id=item['InstanceId']
        
        response = dynamo_client.update_item(
            TableName=config_data.INSTANCE_DETAILS_TABLE_NAME,
            Key={'InstanceId': {'S': instance_id}},
            UpdateExpression="SET Sessions = :SN",
            ExpressionAttributeValues={
                ":SN": {'S': str(session_count)}
            }
        )
        if user_session_count=="0":

            bulk_update_user_tb.update({instance_id:  "2"})
        else:
            bulk_update_user_tb.update({instance_id:  str(user_session_count)})
        

    
    return bulk_update_user_tb
        



def session_calculator(available_session_count,required_session_count):
    required_session_count=int(required_session_count)-int(available_session_count)
    
    return required_session_count
    
def debugging_status_update(RequestId):
    response = dynamo_client.update_item(
            TableName=config_data.USER_DETAILS_TABLE_NAME,
            Key={'RequestId': {'S': RequestId}},
            UpdateExpression="SET DebuggingStatus = :DN",
            ExpressionAttributeValues={
                ":DN": {'S':"Lambda 2 Successfully Excuted"}
            }
        )
        
def request_fulfill_user_status_update(bulk_update_user_tb,request_id):
       
    # Construct the update expression and attribute values
    update_expression = "SET InstanceId = :ids"
    expression_attribute_values = {":ids": bulk_update_user_tb}
    
    dynamo_resource.update_item(
        Key={'RequestId': request_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
    )
def lambda_handler(event, context):
    print("Event",event)
    
    request_fulfill=False
    bulk_update_user_tb={}
    RequestId=event["RequestId"]

    required_session_count=get_details(RequestId)

    
    available_list,partial_list,available_session_count=scan_dynamo_db_recursively(required_session_count)
    
    if available_list or partial_list:
    
        final_instance_list,required_session_count,request_fulfill=filter_out_instance(available_list,partial_list,available_session_count,required_session_count)
        
        bulk_update_user_tb=update_instance_detail_in_user_db(RequestId,final_instance_list)
        
        
    ec2_count=required_session_count/2
    ec2_count=math.ceil(ec2_count)
    
    debugging_status_update(RequestId)
    
    if not request_fulfill:
        print("Required Session",required_session_count)
        
        return {"RequestedSessions":required_session_count,"UserUpdateBulkList":bulk_update_user_tb,"RequestId":RequestId,"Ec2ToBeLaunchCount":ec2_count}
    else:
        request_fulfill_user_status_update(bulk_update_user_tb,RequestId)
        return {"Ec2ToBeLaunchCount":ec2_count,"UserUpdateBulkList":bulk_update_user_tb,"Status":"Required request_fulfill Skip the ec2 spin up lambda go to status check","RequestId":RequestId}
        
    
  
        
        
        
        
        

    # algo
    # 1:dynamo db se get  session number
    # 2: scan intance loop db for session  availablbility
     
    # 3: whatever instance id available we will update the user detail db with key pair ex instance id : 2
    # 4: session available  - required session  to get exact number of  session required
    # 5: then divide the session value by 2 to get the exact number we can use round off
    # 6: then pass this request to next lambda  (spin up ) (event :- requestid, isntance to be launch , pending session number)
    
    
    
    
    
        
    
    
    
    
    

       
    # instance_count_calculator(instance_details)

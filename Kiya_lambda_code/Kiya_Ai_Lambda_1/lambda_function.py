import json
import boto3
import uuid
import Configurations as config_data

dynamo_client = boto3.client('dynamodb')
stf_client = boto3.client('stepfunctions')
sqs_client = boto3.client('sqs')

dynamo_resource = boto3.resource('dynamodb',region_name="ap-south-1").Table(config_data.USER_DETAILS_TABLE_NAME)


def lambda_handler(event, context):
    Event = event
    print("Event--->",Event)
    if "No_of_sessions" in Event:
        try:
            sqs_event = {}
            request_id = str(uuid.uuid4())
            MessageDeduplicationId = str(uuid.uuid4())
            MessageGroupId = str(uuid.uuid4())
            requested_sessions = event["No_of_sessions"]
            stf_response = stf_client.list_executions(
                stateMachineArn=config_data.STATE_MACHINE_ARN,
                statusFilter='RUNNING',
                maxResults=2
            )
            state_of_stf = stf_response["executions"]
            sqs_event.update({"No_of_sessions":requested_sessions,"RequestId":request_id})
            if len(state_of_stf) >=1:
                status="In Queue"
                sqs_response = sqs_client.send_message(
                    QueueUrl=config_data.QUEUE_URL,
                    MessageBody=json.dumps(sqs_event),
                    MessageDeduplicationId=MessageDeduplicationId,
                    MessageGroupId=MessageGroupId
                )
                
                dynamo_client.put_item(TableName=config_data.USER_DETAILS_TABLE_NAME,
                Item={
                    'RequestId':{'S':request_id},
                    'RequestedSessions':{'S':requested_sessions},
                    'ApiStatus':{'S':status},
                    'DebuggingStatus':{'S':"Lambda 1 Successfully Executed"}
                })
            else:
                status = "In Progress"
                input_for_stf = {"No_of_sessions":requested_sessions,"RequestId":request_id}
                start_stf = stf_client.start_execution(
                    stateMachineArn=config_data.STATE_MACHINE_ARN,
                    input=json.dumps(input_for_stf)
                )
                dynamo_client.put_item(TableName=config_data.USER_DETAILS_TABLE_NAME,
                Item={
                    'RequestId':{'S':request_id},
                    'RequestedSessions':{'S':requested_sessions},
                    'ApiStatus':{'S':status},
                    'DebuggingStatus':{'S':"Lambda 1 Successfully Executed"}
                })
                
            api_response = {"RequestId":request_id,"Status":status}
            return api_response
            
        except Exception as e:
            print(f"Exception in No_of_sessions request --> {e}")
            api_response = {"StatusCode":404,"Status":str(e)}
            return api_response
            
        
        
    elif "RequestId" in Event:
        session_links =""
        session_url = []
        try:
            request_id = Event["RequestId"]
            dynamo_response = dynamo_client.query(
                TableName = config_data.USER_DETAILS_TABLE_NAME,
                KeyConditionExpression='RequestId =:RID',
                ExpressionAttributeValues={':RID':{'S':request_id}}
            )
            print(f"Get DynamoDb Details: {dynamo_response}")
            
            if not dynamo_response['Items']:
                api_response = {"StatusCode":404,"Status":"RequestID Not Found"}
                return api_response
                
            if "ApiStatus" in dynamo_response['Items'][0]:
                status = dynamo_response['Items'][0]['ApiStatus']['S']
                
            if "SessionUrls" in dynamo_response['Items'][0]:
                session_links = dynamo_response['Items'][0]['SessionUrls']['L']
                
                for session_link in session_links:
                    print(session_link)
                    session_url.append(session_link['S'])
                    
            
            if session_url:
                api_response = {"RequestId":request_id,"Status":status,"SessionUrls":session_url}
                return api_response
            else:
                api_response = {"RequestId":request_id,"Status":status}
                return api_response
        except Exception as e:
            print(f"Exception in get RequestId details :--> {e}")
            api_response = {"StatusCode":404,"Status":e}
            return api_response
        
    else:
        
        sqs = boto3.resource('sqs')
        messages_to_delete=[]
        
        queue = sqs.get_queue_by_name(QueueName="Kiya_Ai_SQS.fifo")
        
        for message in queue.receive_messages(MaxNumberOfMessages=1):
            # process message body
            print(message.body)
            

            body = json.dumps(message.body)
          
            
            event =json.loads(body)

            my_dict = json.loads(event)


            
            stf_response = stf_client.list_executions(
                stateMachineArn=config_data.STATE_MACHINE_ARN,
                statusFilter='RUNNING',
                maxResults=2
            )
            state_of_stf = stf_response["executions"]
            
            if len(state_of_stf) >=1:
                pass
            else:
            
                status = "In Progress"
                
                requested_sessions=my_dict["No_of_sessions"]
                
                request_id = my_dict["RequestId"]
                input_for_stf = {"No_of_sessions":requested_sessions,"RequestId":request_id}
                start_stf = stf_client.start_execution(
                    stateMachineArn=config_data.STATE_MACHINE_ARN,
                    input=json.dumps(input_for_stf)
                )
                
                update_expression = "SET ApiStatus = :as"
                expression_attribute_values = {":as": status}
                
                dynamo_resource.update_item(
                    Key={'RequestId': request_id},
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_attribute_values,
                )
                
 
                
            
            
            # add message to delete
            messages_to_delete.append({'Id': message.message_id,
            'ReceiptHandle': message.receipt_handle})
            
            delete_response = queue.delete_messages(Entries=messages_to_delete)

    
    
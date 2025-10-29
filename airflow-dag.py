import boto3
import requests
import time
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task


logger = logging.getLogger(__name__)

STUDENT_ID = "xhh6fb"
API_ENDPOINT = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{STUDENT_ID}"
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
EXPECTED_MESSAGES = 21

# default arguments for the DAG
default_args = {
    'owner': 'xhh6fb',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


@dag(
    dag_id='sqs_fragment_assembler',
    default_args=default_args,
    description='Airflow pipeline to collect and assemble SQS message fragments',
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ds3022', 'sqs', 'data-pipeline']
)
def message_assembly_dag():
    """
    Main DAG with 4 tasks:
    1. Hit API to scatter messages
    2. Collect all fragments from queue
    3. Sort and assemble final phrase
    4. Submit result
    """

    @task
    def scatter_messages_to_queue():
        """
        task 1 (call the scatter API endpoint):
        returns the SQS queue URL where the messages were sent
        """
        logger.info(f"Calling scatter API for student: {STUDENT_ID}")
        
        try:
            # sending POST request to trigger message scatter
            api_response = requests.post(API_ENDPOINT, timeout=15)
            api_response.raise_for_status()
            
            # extracting queue URL from the response
            response_data = api_response.json()
            queue_location = response_data.get('sqs_url')
            
            logger.info(f"Messages successfully scattered to: {queue_location}")
            return queue_location
            
        except requests.exceptions.RequestException as error:
            logger.error(f"API request failed: {error}")
            raise
    
    
    @task
    def collect_queue_fragments(queue_url: str):
        """
        task 2 (polling SQS queue and collecting all message fragments):
        waits for delayed messages and processes them as they arrive and also
        returns list of (order_number, word) tuples
        """
        sqs_client = boto3.client('sqs', region_name='us-east-1')
        fragment_collection = []
        
        logger.info(f"Starting fragment collection from queue: {queue_url}")
        
        # continue polling until we have all expected messages
        while len(fragment_collection) < EXPECTED_MESSAGES:
            try:
                # requesting up to 10 messages with long polling
                poll_response = sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,  # SQS maximum
                    MessageAttributeNames=['All'],  # getting all attributes
                    WaitTimeSeconds=20  # long polling to be more efficient
                )
                
                # processing received messages
                if 'Messages' in poll_response:
                    for individual_msg in poll_response['Messages']:
                        try:
                            # extracting the custom attributes
                            attributes = individual_msg.get('MessageAttributes', {})
                            
                            # pulling out the order number and word
                            position = attributes.get('order_no', {}).get('StringValue')
                            text = attributes.get('word', {}).get('StringValue')
                            
                            if position and text:
                                # storing as tuple with integer position
                                fragment_collection.append((int(position), text))
                                logger.info(f"Fragment collected: position {position} = '{text}'")
                                
                                # removing message from queue after it's processed
                                sqs_client.delete_message(
                                    QueueUrl=queue_url,
                                    ReceiptHandle=individual_msg['ReceiptHandle']
                                )
                        
                        except Exception as parse_error:
                            logger.warning(f"Skipping message due to error: {parse_error}")
                            continue
                
                else:
                    # no messages available yet and wait before retrying
                    logger.info("Queue empty on this poll, waiting for delayed messages...")
                    time.sleep(15)
                    
            except Exception as receive_error:
                logger.error(f"Error during message retrieval: {receive_error}")
                time.sleep(10)  # waiting before retrying
        
        logger.info(f"Collection complete: {len(fragment_collection)} fragments gathered")
        return fragment_collection
    
    
    @task
    def construct_final_phrase(fragments: list):
        """
        task 3 (sorting fragments by position and join into complete phrase)
        """
        logger.info("Constructing phrase from fragments")
        
        # sorting by position number (first element of tuple)
        ordered_fragments = sorted(fragments, key=lambda item: item[0])
        
        # logging the ordering to verify
        logger.info("Fragment order:")
        for position, text in ordered_fragments:
            logger.info(f"  {position}: {text}")
        
        # combining all words with spaces
        complete_phrase = ' '.join([text for position, text in ordered_fragments])
        
        logger.info(f"Final phrase: {complete_phrase}")
        return complete_phrase
    
    
    @task
    def submit_final_answer(phrase: str):
        """
        task 4 (sending assembled phrase to submission queue):
        platform is set to 'airflow' for this DAG
        """
        sqs_client = boto3.client('sqs', region_name='us-east-1')
        
        logger.info("Submitting final answer to grading queue")
        
        try:
            # sending message with required attributes
            submission_response = sqs_client.send_message(
                QueueUrl=SUBMISSION_URL,
                MessageBody=f"Airflow submission from {STUDENT_ID}",
                MessageAttributes={
                    'uvaid': {
                        'DataType': 'String',
                        'StringValue': STUDENT_ID
                    },
                    'phrase': {
                        'DataType': 'String',
                        'StringValue': phrase
                    },
                    'platform': {
                        'DataType': 'String',
                        'StringValue': 'airflow'  # different from prefect version
                    }
                }
            )
            
            # checking the response
            response_code = submission_response['ResponseMetadata']['HTTPStatusCode']
            message_identifier = submission_response.get('MessageId', 'Unknown')
            
            logger.info(f"Submission HTTP code: {response_code}")
            logger.info(f"Message ID: {message_identifier}")
            
            if response_code == 200:
                logger.info("✓ Submission successful!")
            else:
                logger.warning(f"Unexpected response code: {response_code}")
            
            return True
            
        except Exception as submit_error:
            logger.error(f"Submission failed: {submit_error}")
            raise
    
    
    # defining the task execution order
    queue_url = scatter_messages_to_queue()
    collected_fragments = collect_queue_fragments(queue_url)
    assembled_phrase = construct_final_phrase(collected_fragments)
    submit_final_answer(assembled_phrase)


dag_instance = message_assembly_dag()

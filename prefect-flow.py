import boto3
import requests
import time
from prefect import task, flow
from prefect.logging import get_run_logger
from botocore.exceptions import ClientError


COMPUTING_ID = "xhh6fb"
API_ENDPOINT = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{COMPUTING_ID}"
SUBMISSION_QUEUE = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"


@task(retries=2)
def trigger_message_scatter():
    """
    Step 1: Hit the API to scatter 21 messages into my SQS queue
    The API returns my personal queue URL
    """
    logger = get_run_logger()
    logger.info(f"Triggering message scatter for {COMPUTING_ID}")
    
    try:
        # POST request to scatter endpoint
        resp = requests.post(API_ENDPOINT, timeout=10)
        resp.raise_for_status()
        
        data = resp.json()
        my_queue = data.get('sqs_url')
        
        logger.info(f"Messages scattered successfully to: {my_queue}")
        return my_queue
        
    except requests.RequestException as err:
        logger.error(f"Failed to call scatter API: {err}")
        raise


@task
def check_queue_metrics(queue_url):
    """
    Helper to check how many messages are in various states
    Helps us decide when to keep polling vs when we're done
    """
    logger = get_run_logger()
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    
    try:
        attrs_response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible', 
                'ApproximateNumberOfMessagesDelayed'
            ]
        )
        
        attrs = attrs_response['Attributes']
        
        # parsing the three key metrics
        ready = int(attrs.get('ApproximateNumberOfMessages', 0))
        processing = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        pending = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
        
        logger.info(f"Queue state -> Ready: {ready}, Processing: {processing}, Pending: {pending}")
        
        return {
            'ready': ready,
            'processing': processing,
            'pending': pending,
            'combined_total': ready + processing + pending
        }
    
    except ClientError as err:
        logger.warning(f"Could not fetch queue metrics: {err}")
        return {'ready': 0, 'processing': 0, 'pending': 0, 'combined_total': 0}


@task
def retrieve_all_fragments(queue_url):
    """
    Step 2: Continuously poll the queue until we have all 21 message fragments
    Each message has order_no and word attributes we need to extract
    """
    logger = get_run_logger()
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    
    fragments = []  # will store tuples of (order_number, word)
    expected_count = 21
    timeout_seconds = 1200  # i'm giving it up to 20 min for all delays
    start_time = time.time()
    
    logger.info("Beginning message collection...")
    
    # keep looping until i have all messages or timeout
    while len(fragments) < expected_count:
        # check if i've been waiting too long
        if time.time() - start_time > timeout_seconds:
            logger.warning(f"Timeout reached. Only collected {len(fragments)}/{expected_count}")
            break
        
        # seeing what the queue status is
        metrics = check_queue_metrics.fn(queue_url)
        
        # if nothing is ready but stuff is still delayed then programmed to wait a bit
        if metrics['ready'] == 0 and metrics['combined_total'] > len(fragments):
            logger.info(f"No messages ready yet. {metrics['pending']} still delayed. Waiting...")
            time.sleep(25)
            continue
        
        # trying to grab up to 10 messages
        try:
            recv_response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=10,  # SQS max per request
                WaitTimeSeconds=20  # long polling
            )
            
            # if we get messages back then process them
            if 'Messages' in recv_response:
                for msg in recv_response['Messages']:
                    # Pull out the custom attributes
                    msg_attributes = msg.get('MessageAttributes', {})
                    
                    order_val = msg_attributes.get('order_no', {}).get('StringValue')
                    word_val = msg_attributes.get('word', {}).get('StringValue')
                    
                    if order_val and word_val:
                        # converting order to int and saving the pair
                        fragments.append((int(order_val), word_val))
                        logger.info(f"Captured fragment #{order_val}: '{word_val}'")
                        
                        # cleaning up and deleting from queue so we don't reprocess
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=msg['ReceiptHandle']
                        )
            else:
                # no messages in this poll so i programmed the code to wait a sec before trying again
                logger.info("Poll returned empty. Retrying shortly...")
                time.sleep(10)
                
        except ClientError as err:
            logger.error(f"Error during message receive: {err}")
            time.sleep(5)  # pause before retrying
    
    logger.info(f"Collection finished. Retrieved {len(fragments)} fragments total")
    return fragments


@task
def build_complete_message(fragments):
    """
    Step 3: Sort the fragments by order number and join into final phrase
    """
    logger = get_run_logger()
    
    if not fragments:
        logger.error("No fragments to assemble!")
        raise ValueError("Fragment list is empty")
    
    # sorting by the order number (first element of each tuple)
    ordered = sorted(fragments, key=lambda item: item[0])
    
    # logging what we're assembling
    logger.info("Assembling in order:")
    for num, text in ordered:
        logger.info(f"  Position {num}: {text}")
    
    # joining all the words together
    complete_phrase = ' '.join([text for num, text in ordered])
    
    logger.info(f"Assembled result: {complete_phrase}")
    return complete_phrase


@task(retries=2)
def deliver_solution(phrase):
    """
    Step 4: Send the final assembled phrase to the submission queue
    Must include my computing ID and specify platform as 'prefect'
    """
    logger = get_run_logger()
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    
    logger.info("Submitting solution...")
    
    try:
        submit_response = sqs_client.send_message(
            QueueUrl=SUBMISSION_QUEUE,
            MessageBody=f"Project submission from {COMPUTING_ID}",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': COMPUTING_ID
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': 'prefect'
                }
            }
        )
        
        # Check the response code
        status_code = submit_response['ResponseMetadata']['HTTPStatusCode']
        msg_id = submit_response.get('MessageId', 'N/A')
        
        logger.info(f"Submission response code: {status_code}")
        logger.info(f"Message ID: {msg_id}")
        
        if status_code == 200:
            logger.info("SUCCESS - Solution delivered!")
            return True
        else:
            logger.warning(f"Unexpected response code: {status_code}")
            return False
            
    except ClientError as err:
        logger.error(f"Submission failed: {err}")
        raise


@flow(name="message-reassembly-pipeline")
def main_pipeline():
    """
    Main orchestration flow
    Coordinates all the tasks in sequence
    """
    logger = get_run_logger()
    logger.info("="*60)
    logger.info(f"Starting message assembly pipeline for {COMPUTING_ID}")
    logger.info("="*60)
    
    # task 1 (trigger the scatter)
    queue_url = trigger_message_scatter()
    
    # task 2 (collect the fragments)
    message_fragments = retrieve_all_fragments(queue_url)
    
    # task 3 (reassemble them in order)
    final_phrase = build_complete_message(message_fragments)
    
    # task 4 (submit the answer)
    success = deliver_solution(final_phrase)

  
    logger.info("="*60)
    if success:
        logger.info("Pipeline completed successfully!")
        logger.info(f"Final phrase: {final_phrase}")
    else:
        logger.warning("Pipeline finished but submission may have issues")
    logger.info("="*60)
    
    return final_phrase


# execute this when run directly
if __name__ == "__main__":
    result = main_pipeline()

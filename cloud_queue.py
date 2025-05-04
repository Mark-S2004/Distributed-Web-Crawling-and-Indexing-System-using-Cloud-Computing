import boto3
import json
import logging
from botocore.exceptions import ClientError

class CloudQueue:
    """
    CloudQueue class for managing URL queues using AWS SQS.
    Provides methods to enqueue and dequeue URLs for crawler nodes.
    Features:
    - Fully cloud-based operation using AWS SQS
    - No local fallback (AWS-only operation)
    - Consistent message format
    """

    def __init__(self, queue_name: str, status_queue_name: str):
        self.queue_name = queue_name
        self.status_queue_name = status_queue_name
        self.shutdown = False

        # Initialize SQS queues
        self._initialize_sqs_queues()

    def _initialize_sqs_queues(self):
        """Initialize SQS queues and create them if they don't exist."""
        # Initialize SQS client
        self.sqs = boto3.client('sqs')

        # Try to get the main queue URL
        try:
            queues = self.sqs.list_queues(QueueNamePrefix=self.queue_name)
            if 'QueueUrls' in queues and queues['QueueUrls']:
                self.queue_url = queues['QueueUrls'][0]
                logging.info(f"Found existing queue: {self.queue_url}")
            else:
                # Create the queue if it doesn't exist
                logging.info(f"Creating new queue: {self.queue_name}")
                response = self.sqs.create_queue(QueueName=self.queue_name)
                self.queue_url = response['QueueUrl']
        except Exception as e:
            logging.error(f"Error accessing main queue: {e}")
            raise

        # Try to get the status queue URL
        try:
            status_queues = self.sqs.list_queues(QueueNamePrefix=self.status_queue_name)
            if 'QueueUrls' in status_queues and status_queues['QueueUrls']:
                self.status_queue_url = status_queues['QueueUrls'][0]
                logging.info(f"Found existing status queue: {self.status_queue_url}")
            else:
                # Create the status queue if it doesn't exist
                logging.info(f"Creating new status queue: {self.status_queue_name}")
                response = self.sqs.create_queue(QueueName=self.status_queue_name)
                self.status_queue_url = response['QueueUrl']
        except Exception as e:
            logging.error(f"Error accessing status queue: {e}")
            raise

        logging.info("Successfully initialized SQS queues")

    def is_cloud_mode(self) -> bool:
        """Return whether the queue is operating in cloud mode."""
        return True

    def get_queue_size(self) -> int:
        """
        Return the approximate number of messages in the crawl queue.
        Uses SQS ApproximateNumberOfMessages.
        """
        try:
            attrs = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            return int(attrs['Attributes'].get('ApproximateNumberOfMessages', 0))
        except ClientError as e:
            logging.error(f"Error fetching queue size from SQS: {e}")
            return 0

    def send_message(self, url: str) -> bool:
        """Enqueue a URL for crawling."""
        try:
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=url)
            return True
        except Exception as e:
            logging.error(f"Error sending message to SQS queue: {e}")
            return False

    def receive_messages(self, WaitTimeSeconds=0, MaxNumber=1):
        """
        Dequeue messages for crawlers.
        Returns a list of message objects.
        """
        try:
            resp = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=MaxNumber,
                WaitTimeSeconds=WaitTimeSeconds
            )
            return resp.get('Messages', [])
        except Exception as e:
            logging.error(f"Error receiving messages from SQS: {e}")
            return []

    def delete_message(self, message) -> None:
        """Delete a message once processed."""
        try:
            # Check if message has ReceiptHandle
            if isinstance(message, dict) and 'ReceiptHandle' in message:
                self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message['ReceiptHandle'])
            else:
                logging.warning(f"Cannot delete message, invalid format: {message}")
        except Exception as e:
            logging.error(f"Error deleting message from SQS: {e}")

    def send_status_message(self, body: dict) -> bool:
        """Send a status/heartbeat/discovered URLs message to master."""
        payload = json.dumps(body)
        try:
            self.sqs.send_message(QueueUrl=self.status_queue_url, MessageBody=payload)
            return True
        except Exception as e:
            logging.error(f"Error sending status message to SQS: {e}")
            return False

    def receive_status_messages(self, WaitTimeSeconds=0, MaxNumber=10):
        """
        Receive status messages (heartbeat, errors, discoveries).
        Returns a list of message objects.
        """
        try:
            resp = self.sqs.receive_message(
                QueueUrl=self.status_queue_url,
                MaxNumberOfMessages=MaxNumber,
                WaitTimeSeconds=WaitTimeSeconds
            )
            return resp.get('Messages', [])
        except Exception as e:
            logging.error(f"Error receiving status messages from SQS: {e}")
            return []

    def delete_status_message(self, message) -> None:
        """Delete a status message once processed."""
        try:
            # Check if message has ReceiptHandle
            if isinstance(message, dict) and 'ReceiptHandle' in message:
                self.sqs.delete_message(QueueUrl=self.status_queue_url, ReceiptHandle=message['ReceiptHandle'])
            else:
                logging.warning(f"Cannot delete status message, invalid format: {message}")
        except Exception as e:
            logging.error(f"Error deleting status message from SQS: {e}")

    def purge_queue(self) -> bool:
        """Purge all messages from the crawl queue."""
        try:
            self.sqs.purge_queue(QueueUrl=self.queue_url)
            return True
        except Exception as e:
            logging.error(f"Error purging SQS queue: {e}")
            return False

    def shutdown_queue(self):
        """Shutdown the queue."""
        logging.info("Shutting down CloudQueue")
        self.shutdown = True

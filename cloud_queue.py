import boto3
import json
import logging
import os
import time
import uuid
from botocore.exceptions import ClientError
from datetime import datetime
from collections import deque

class CloudQueue:
    """
    CloudQueue class for managing URL queues using AWS SQS.
    Provides methods to enqueue and dequeue URLs for crawler nodes.
    Falls back to local in-memory queue if SQS is not available.
    """
    
    def __init__(self, queue_name='crawler-url-queue', status_queue_name='crawler-status-queue'):
        """Initialize the CloudQueue with SQS queues"""
        self.use_cloud = True
        self.local_queue = deque()  # Fallback local queue
        self.local_status_queue = deque()  # Fallback local status queue
        
        try:
            self.sqs = boto3.client('sqs')
            # Set up the URL queue
            self.queue_url = self._get_queue_url(queue_name)
            # Set up the status queue
            self.status_queue_url = self._get_queue_url(status_queue_name)
            logging.info(f"CloudQueue initialized with SQS queues: {queue_name}, {status_queue_name}")
        except Exception as e:
            logging.warning(f"Failed to initialize SQS, falling back to local queue: {e}")
            self.use_cloud = False
            # Create data directory for local queue persistence
            os.makedirs('data/queue', exist_ok=True)
    
    def _get_queue_url(self, queue_name):
        """Get the URL for an existing queue or create a new one"""
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except ClientError as e:
            if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                # Queue doesn't exist, create it
                response = self.sqs.create_queue(QueueName=queue_name)
                return response['QueueUrl']
            else:
                # Some other error
                raise
    
    def send_message(self, message_body, message_attributes=None):
        """Send a message to the queue"""
        if isinstance(message_body, dict) or isinstance(message_body, list):
            message_body = json.dumps(message_body)
        
        if self.use_cloud:
            try:
                # Check if message is too large for SQS (262144 bytes)
                encoded_message = message_body.encode('utf-8')
                message_size = len(encoded_message)
                
                if message_size > 250000:  # Use a slightly lower limit than 262144 for safety
                    logging.warning(f"Message size ({message_size} bytes) exceeds SQS limit, truncating content")
                    # Truncate message body if too large
                    if isinstance(message_body, str):
                        try:
                            # If JSON, extract metadata and truncate content
                            message_dict = json.loads(message_body)
                            if 'content' in message_dict:
                                # Truncate content field to fit within limits
                                message_dict['content'] = message_dict['content'][:100000]  # Truncate to 100K
                                message_dict['truncated'] = True
                                message_body = json.dumps(message_dict)
                            else:
                                # Simple truncation
                                message_body = message_body[:250000]
                        except json.JSONDecodeError:
                            # Not valid JSON, just truncate
                            message_body = message_body[:250000]
                
                if message_attributes:
                    response = self.sqs.send_message(
                        QueueUrl=self.queue_url,
                        MessageBody=message_body,
                        MessageAttributes=message_attributes
                    )
                else:
                    response = self.sqs.send_message(
                        QueueUrl=self.queue_url,
                        MessageBody=message_body
                    )
                return {"status": "success", "message_id": response.get('MessageId')}
            except Exception as e:
                logging.error(f"Error sending message to SQS: {e}")
                # Fall back to local queue
                self.local_queue.append(message_body)
                self._persist_local_queue()  # Persist changes to disk
                return {"status": "success_local", "message_id": str(uuid.uuid4())}
        else:
            # Use local queue
            self.local_queue.append(message_body)
            self._persist_local_queue()  # Persist changes to disk
            return {"status": "success_local", "message_id": str(uuid.uuid4())}
    
    def receive_messages(self, max_messages=10, wait_time=5):
        """Receive messages from the queue"""
        if self.use_cloud:
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=max_messages,
                    WaitTimeSeconds=wait_time,
                    VisibilityTimeout=30  # 30 seconds to process the message
                )
                
                messages = response.get('Messages', [])
                return messages
            except Exception as e:
                logging.error(f"Error receiving messages from SQS: {e}")
                # Fall back to local queue
                return self._receive_local_messages(max_messages)
        else:
            # Use local queue
            return self._receive_local_messages(max_messages)
    
    def _receive_local_messages(self, max_messages):
        """Receive messages from the local queue"""
        messages = []
        # Load the latest state from disk
        self._load_local_queue()
        
        # Get up to max_messages from the local queue
        for _ in range(min(max_messages, len(self.local_queue))):
            if self.local_queue:
                message_body = self.local_queue.popleft()
                message_id = str(uuid.uuid4())
                messages.append({
                    'MessageId': message_id,
                    'ReceiptHandle': message_id,  # Use the same ID for receipt handle
                    'Body': message_body,
                    'Attributes': {
                        'SentTimestamp': str(int(time.time() * 1000))
                    }
                })
        
        # Persist changes to disk
        self._persist_local_queue()
        return messages
    
    def delete_message(self, message):
        """Delete a message from the queue after processing"""
        if self.use_cloud:
            try:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                return True
            except Exception as e:
                logging.error(f"Error deleting message from SQS: {e}")
                return False
        else:
            # For local queue, messages are already removed during receive
            return True
    
    def send_status_update(self, node_id, status_data):
        """Send a status update to the status queue"""
        message_body = {
            'node_id': node_id,
            'timestamp': datetime.now().isoformat(),
            'status': status_data
        }
        
        if self.use_cloud:
            try:
                response = self.sqs.send_message(
                    QueueUrl=self.status_queue_url,
                    MessageBody=json.dumps(message_body)
                )
                return {"status": "success", "message_id": response.get('MessageId')}
            except Exception as e:
                logging.error(f"Error sending status update to SQS: {e}")
                # Fall back to local status queue
                self.local_status_queue.append(message_body)
                return {"status": "success_local", "message_id": str(uuid.uuid4())}
        else:
            # Use local status queue
            self.local_status_queue.append(message_body)
            return {"status": "success_local", "message_id": str(uuid.uuid4())}
    
    def get_queue_size(self):
        """Get the approximate number of messages in the queue"""
        if self.use_cloud:
            try:
                response = self.sqs.get_queue_attributes(
                    QueueUrl=self.queue_url,
                    AttributeNames=['ApproximateNumberOfMessages']
                )
                return int(response['Attributes']['ApproximateNumberOfMessages'])
            except Exception as e:
                logging.error(f"Error getting queue size from SQS: {e}")
                # Fall back to local queue size
                return len(self.local_queue)
        else:
            # Use local queue size
            return len(self.local_queue)
    
    def enqueue_url(self, url, metadata=None):
        """Enqueue a URL with optional metadata"""
        message = {
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        return self.send_message(json.dumps(message))
    
    def _persist_local_queue(self):
        """Persist the local queue to disk"""
        try:
            with open('data/queue/local_queue.json', 'w') as f:
                json.dump(list(self.local_queue), f)
        except Exception as e:
            logging.error(f"Error persisting local queue: {e}")
    
    def _load_local_queue(self):
        """Load the local queue from disk"""
        try:
            if os.path.exists('data/queue/local_queue.json'):
                with open('data/queue/local_queue.json', 'r') as f:
                    queue_data = json.load(f)
                    self.local_queue = deque(queue_data)
        except Exception as e:
            logging.error(f"Error loading local queue: {e}")
    
    def is_cloud_mode(self):
        """Check if the queue is operating in cloud mode"""
        return self.use_cloud 
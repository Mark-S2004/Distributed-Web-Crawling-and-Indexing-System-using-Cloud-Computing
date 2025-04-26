import boto3
import json
import logging
import os
import time
import uuid
from botocore.exceptions import ClientError

class CloudQueue:
    """
    Cloud-based task queue using AWS SQS (Simple Queue Service).
    Provides methods to create, send messages to, and receive messages from SQS queues.
    Includes fallback to local queue if SQS is unavailable.
    """
    
    def __init__(self, queue_name="web-crawler-task-queue", region_name=None):
        """
        Initialize the CloudQueue class.
        
        Args:
            queue_name (str): The SQS queue name to use.
            region_name (str): The AWS region to use.
        """
        self.logger = logging.getLogger('CloudQueue')
        self.logger.setLevel(logging.INFO)
        
        # Make sure the logger has a handler
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - CloudQueue - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            # Add file handler
            log_dir = "logs"
            os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.FileHandler(os.path.join(log_dir, 'queue.log'))
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # Try to load AWS configuration from file
        try:
            if os.path.exists('aws_config.json'):
                with open('aws_config.json', 'r') as f:
                    aws_config = json.load(f)
                    self.logger.info("Loaded AWS configuration from aws_config.json")
                    config_region = aws_config.get('aws', {}).get('region')
                    
                    # Use provided value or config value
                    region_name = region_name or config_region
        except Exception as e:
            self.logger.warning(f"Failed to load AWS configuration file: {e}")
        
        # Get region from environment variable if not provided
        self.region_name = region_name or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        self.queue_name = queue_name
        
        # Initialize local queue as fallback
        self.local_queue = []
        
        # Try to connect to AWS SQS
        try:
            self.sqs = boto3.resource('sqs', region_name=self.region_name)
            self.queue = self._ensure_queue_exists()
            self.cloud_mode = True
            self.logger.info(f"Successfully connected to AWS SQS in region {self.region_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize SQS client: {e}")
            # Initialize to None to allow graceful fallback to local queue
            self.sqs = None
            self.queue = None
            self.cloud_mode = False
            self.logger.warning("Falling back to local queue mode")
    
    def _ensure_queue_exists(self):
        """Ensure the SQS queue exists, creating it if necessary."""
        try:
            # Try to get the queue first
            for queue in self.sqs.queues.filter(QueueNamePrefix=self.queue_name):
                if queue.url.endswith(self.queue_name):
                    self.logger.info(f"Queue {self.queue_name} already exists")
                    return queue
            
            # Queue doesn't exist, create it
            self.logger.info(f"Queue {self.queue_name} does not exist. Creating...")
            queue = self.sqs.create_queue(
                QueueName=self.queue_name,
                Attributes={
                    'DelaySeconds': '0',
                    'MessageRetentionPeriod': '86400',  # 24 hours
                    'VisibilityTimeout': '60'  # 60 seconds
                }
            )
            self.logger.info(f"Successfully created queue {self.queue_name}")
            return queue
        except ClientError as e:
            self.logger.error(f"Error with SQS queue: {e}")
            raise
    
    def send_message(self, message, attributes=None):
        """
        Send a message to the queue.
        
        Args:
            message (dict or str): The message to send to the queue
            attributes (dict): Optional message attributes
            
        Returns:
            dict: Information about the sent message including MessageId
        """
        if not isinstance(message, str):
            message = json.dumps(message)
        
        # If SQS is available, use it
        if self.cloud_mode and self.queue:
            try:
                msg_attributes = {}
                if attributes:
                    for key, value in attributes.items():
                        msg_attributes[key] = {
                            'DataType': 'String',
                            'StringValue': str(value)
                        }
                
                response = self.queue.send_message(
                    MessageBody=message,
                    MessageAttributes=msg_attributes if msg_attributes else {}
                )
                self.logger.info(f"Sent message to SQS: {response.get('MessageId')}, content: {message[:50]}{'...' if len(message) > 50 else ''}")
                return {
                    'success': True,
                    'message_id': response.get('MessageId'),
                    'queue_type': 'sqs'
                }
            except Exception as e:
                self.logger.error(f"Failed to send message to SQS: {e}")
                self.cloud_mode = False
                self.logger.warning("Falling back to local queue mode")
        
        # Fall back to local queue
        message_id = str(uuid.uuid4())
        self.local_queue.append({
            'Id': message_id,
            'MessageBody': message,
            'Attributes': attributes or {}
        })
        self.logger.info(f"Added message to local queue: {message_id}, content: {message[:50]}{'...' if len(message) > 50 else ''}")
        return {
            'success': True,
            'message_id': message_id,
            'queue_type': 'local'
        }
    
    def receive_messages(self, max_messages=1, wait_time=0):
        """
        Receive messages from the queue.
        
        Args:
            max_messages (int): Maximum number of messages to receive (1-10)
            wait_time (int): Time in seconds to wait for messages (0-20)
            
        Returns:
            list: List of received message objects
        """
        # If SQS is available, use it
        if self.cloud_mode and self.queue:
            try:
                messages = self.queue.receive_messages(
                    MaxNumberOfMessages=min(max_messages, 10),
                    WaitTimeSeconds=min(wait_time, 20),
                    AttributeNames=['All'],
                    MessageAttributeNames=['All']
                )
                
                if messages:
                    message_bodies = [m.body[:30] + ('...' if len(m.body) > 30 else '') for m in messages]
                    self.logger.info(f"Received {len(messages)} messages from SQS: {message_bodies}")
                    return messages
                return []
            except Exception as e:
                self.logger.error(f"Failed to receive messages from SQS: {e}")
                self.cloud_mode = False
                self.logger.warning("Falling back to local queue mode")
        
        # Fall back to local queue
        if not self.local_queue:
            if wait_time > 0:
                time.sleep(wait_time)  # Simulate wait time
            return []
        
        # Return up to max_messages from the local queue
        num_msgs = min(max_messages, len(self.local_queue))
        messages = self.local_queue[:num_msgs]
        
        if messages:
            message_bodies = [m.get('MessageBody', '')[:30] + ('...' if len(m.get('MessageBody', '')) > 30 else '') for m in messages]
            self.logger.info(f"Received {len(messages)} messages from local queue: {message_bodies}")
        
        return messages
    
    def delete_message(self, message):
        """
        Delete a message from the queue.
        
        Args:
            message: The message object to delete
        
        Returns:
            bool: True if successful, False otherwise
        """
        # If SQS is available, use it
        if self.cloud_mode and self.queue and hasattr(message, 'delete'):
            try:
                message_body = message.body[:30] + ('...' if len(message.body) > 30 else '')
                message.delete()
                self.logger.info(f"Deleted message from SQS: {message_body}")
                return True
            except Exception as e:
                self.logger.error(f"Failed to delete message from SQS: {e}")
                self.cloud_mode = False
                self.logger.warning("Falling back to local queue mode")
        
        # Fall back to local queue
        try:
            message_id = message.get('Id') if isinstance(message, dict) else None
            message_body = message.get('MessageBody', '')[:30] + ('...' if len(message.get('MessageBody', '')) > 30 else '') if isinstance(message, dict) else str(message)[:30]
            
            if message_id:
                self.local_queue = [m for m in self.local_queue if m.get('Id') != message_id]
            else:
                if message in self.local_queue:
                    self.local_queue.remove(message)
            self.logger.info(f"Removed message from local queue: {message_body}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete message from local queue: {e}")
            return False
    
    def get_queue_size(self):
        """
        Get the approximate number of messages in the queue.
        
        Returns:
            int: Approximate number of messages
        """
        # If SQS is available, use it
        if self.cloud_mode and self.queue:
            try:
                attributes = self.queue.attributes
                queue_size = int(attributes.get('ApproximateNumberOfMessages', 0))
                self.logger.info(f"SQS queue size: {queue_size}")
                return queue_size
            except Exception as e:
                self.logger.error(f"Failed to get queue size from SQS: {e}")
                self.cloud_mode = False
                self.logger.warning("Falling back to local queue mode")
        
        # Fall back to local queue
        queue_size = len(self.local_queue)
        self.logger.info(f"Local queue size: {queue_size}")
        return queue_size
    
    def purge_queue(self):
        """
        Purge the queue of all messages.
        
        Returns:
            bool: True if successful, False otherwise
        """
        # If SQS is available, use it
        if self.cloud_mode and self.queue:
            try:
                self.queue.purge()
                self.logger.info(f"Purged SQS queue {self.queue_name}")
                return True
            except Exception as e:
                self.logger.error(f"Failed to purge SQS queue: {e}")
                self.cloud_mode = False
                self.logger.warning("Falling back to local queue mode")
        
        # Fall back to local queue
        self.local_queue = []
        self.logger.info("Cleared local queue")
        return True
    
    def is_cloud_mode(self):
        """
        Check if the queue is operating in cloud mode.
        
        Returns:
            bool: True if using SQS, False if using local queue
        """
        return self.cloud_mode 
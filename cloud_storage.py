import boto3
import os
import logging
import json
import hashlib
import time
from botocore.exceptions import ClientError
from datetime import datetime

class CloudStorage:
    """
    Cloud Storage class for storing crawled data in AWS S3.
    Provides methods to store both raw HTML and processed text from crawled web pages.
    """
    
    def __init__(self, bucket_name=None, region_name=None):
        """
        Initialize the CloudStorage class.
        
        Args:
            bucket_name (str): The S3 bucket name to use. If None, will use environment variable or default.
            region_name (str): The AWS region to use.
        """
        self.logger = logging.getLogger('CloudStorage')
        self.logger.setLevel(logging.INFO)
        
        # Make sure the logger has a handler
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - CloudStorage - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            # Add file handler
            file_handler = logging.FileHandler('storage.log')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # Try to load AWS configuration from file
        try:
            if os.path.exists('aws_config.json'):
                with open('aws_config.json', 'r') as f:
                    aws_config = json.load(f)
                    self.logger.info("Loaded AWS configuration from aws_config.json")
                    config_bucket = aws_config.get('aws', {}).get('bucket_name')
                    config_region = aws_config.get('aws', {}).get('region')
                    
                    # Use provided values or config values
                    bucket_name = bucket_name or config_bucket
                    region_name = region_name or config_region
        except Exception as e:
            self.logger.warning(f"Failed to load AWS configuration file: {e}")
        
        # Get bucket name from environment variable if not provided
        self.bucket_name = bucket_name or os.environ.get('AWS_S3_BUCKET', 'web-crawler-data-storage')
        self.region_name = region_name or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        
        # AWS credentials can be configured in several ways:
        # 1. Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        # 2. Shared credential file (~/.aws/credentials)
        # 3. IAM role for EC2 instance
        try:
            self.s3_client = boto3.client('s3', region_name=self.region_name)
            self.s3_resource = boto3.resource('s3', region_name=self.region_name)
            self._ensure_bucket_exists()
            self.logger.info(f"Successfully connected to AWS S3 in region {self.region_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {e}")
            # Initialize to None to allow graceful fallback to local storage
            self.s3_client = None
            self.s3_resource = None
    
    def _ensure_bucket_exists(self):
        """Ensure the S3 bucket exists, creating it if necessary."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"Bucket {self.bucket_name} already exists")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            
            if error_code == '404':
                self.logger.info(f"Bucket {self.bucket_name} does not exist. Creating...")
                try:
                    if self.region_name == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region_name}
                        )
                    self.logger.info(f"Successfully created bucket {self.bucket_name}")
                except ClientError as create_error:
                    self.logger.error(f"Failed to create bucket: {create_error}")
                    raise
            else:
                self.logger.error(f"Error checking bucket: {e}")
                raise
    
    def _generate_key(self, url, content_type):
        """
        Generate a unique S3 key for a URL.
        
        Args:
            url (str): The URL of the crawled page
            content_type (str): Type of content (raw_html, processed_text, metadata)
            
        Returns:
            str: The S3 key
        """
        # Create a hash of the URL to use in the key
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        # Use the current date as part of the key for organization
        date_str = datetime.now().strftime('%Y/%m/%d')
        
        # Return a key in the format: content_type/YYYY/MM/DD/url_hash.extension
        extension = 'html' if content_type == 'raw_html' else 'json' if content_type == 'metadata' else 'txt'
        return f"{content_type}/{date_str}/{url_hash}.{extension}"
    
    def store_raw_html(self, url, html_content):
        """
        Store raw HTML content in S3.
        
        Args:
            url (str): The URL of the crawled page
            html_content (str): The raw HTML content
            
        Returns:
            dict: Storage info including success status and storage location
        """
        if not self.s3_client:
            self.logger.warning("S3 client not initialized. Falling back to local storage.")
            return self._local_store(url, html_content, 'raw_html')
        
        key = self._generate_key(url, 'raw_html')
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=html_content,
                ContentType='text/html',
                Metadata={
                    'url': url,
                    'timestamp': str(int(time.time())),
                    'storage_type': 'raw_html'
                }
            )
            self.logger.info(f"Successfully stored raw HTML for {url} at s3://{self.bucket_name}/{key}")
            return {
                'success': True,
                'storage_type': 's3',
                'bucket': self.bucket_name,
                'key': key,
                'url': url,
                'content_type': 'raw_html',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to store raw HTML for {url}: {e}")
            # Fall back to local storage
            return self._local_store(url, html_content, 'raw_html')
    
    def store_processed_text(self, url, processed_text, metadata=None):
        """
        Store processed text content in S3.
        
        Args:
            url (str): The URL of the crawled page
            processed_text (str): The processed text content
            metadata (dict): Optional metadata about the processed content
            
        Returns:
            dict: Storage info including success status and storage location
        """
        if not self.s3_client:
            self.logger.warning("S3 client not initialized. Falling back to local storage.")
            return self._local_store(url, processed_text, 'processed_text', metadata)
        
        text_key = self._generate_key(url, 'processed_text')
        try:
            # Store the processed text
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=text_key,
                Body=processed_text,
                ContentType='text/plain',
                Metadata={
                    'url': url,
                    'timestamp': str(int(time.time())),
                    'storage_type': 'processed_text'
                }
            )
            self.logger.info(f"Successfully stored processed text for {url} at s3://{self.bucket_name}/{text_key}")
            
            # Store metadata if provided
            if metadata:
                meta_key = self._generate_key(url, 'metadata')
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=meta_key,
                    Body=json.dumps(metadata),
                    ContentType='application/json',
                    Metadata={
                        'url': url,
                        'timestamp': str(int(time.time())),
                        'storage_type': 'metadata'
                    }
                )
                self.logger.info(f"Successfully stored metadata for {url} at s3://{self.bucket_name}/{meta_key}")
            
            return {
                'success': True,
                'storage_type': 's3',
                'bucket': self.bucket_name,
                'text_key': text_key,
                'meta_key': meta_key if metadata else None,
                'url': url,
                'content_type': 'processed_text',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to store processed text for {url}: {e}")
            # Fall back to local storage
            return self._local_store(url, processed_text, 'processed_text', metadata)
    
    def _local_store(self, url, content, content_type, metadata=None):
        """
        Fall back to local storage when S3 storage fails.
        
        Args:
            url (str): The URL of the crawled page
            content (str): The content to store
            content_type (str): Type of content (raw_html, processed_text)
            metadata (dict): Optional metadata about the content
            
        Returns:
            dict: Storage info including success status and storage location
        """
        try:
            # Create directories if they don't exist
            local_storage_dir = os.path.join('crawled_data', content_type)
            os.makedirs(local_storage_dir, exist_ok=True)
            
            # Create a filename based on URL hash
            url_hash = hashlib.md5(url.encode()).hexdigest()
            extension = 'html' if content_type == 'raw_html' else 'txt'
            filename = f"{url_hash}.{extension}"
            filepath = os.path.join(local_storage_dir, filename)
            
            # Write content to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Store metadata if provided
            if metadata:
                meta_filename = f"{url_hash}.json"
                meta_filepath = os.path.join('crawled_data', 'metadata', meta_filename)
                os.makedirs(os.path.join('crawled_data', 'metadata'), exist_ok=True)
                
                with open(meta_filepath, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2)
            
            self.logger.info(f"Successfully stored {content_type} for {url} locally at {filepath}")
            return {
                'success': True,
                'storage_type': 'local',
                'filepath': filepath,
                'meta_filepath': meta_filepath if metadata else None,
                'url': url,
                'content_type': content_type,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to store {content_type} locally for {url}: {e}")
            return {
                'success': False,
                'storage_type': 'none',
                'error': str(e),
                'url': url,
                'content_type': content_type,
                'timestamp': datetime.now().isoformat()
            }
    
    def retrieve_raw_html(self, url):
        """
        Retrieve raw HTML content from S3.
        
        Args:
            url (str): The URL of the crawled page
            
        Returns:
            str: The raw HTML content or None if not found/error
        """
        if not self.s3_client:
            return self._local_retrieve(url, 'raw_html')
        
        key = self._generate_key(url, 'raw_html')
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            html_content = response['Body'].read().decode('utf-8')
            self.logger.info(f"Successfully retrieved raw HTML for {url}")
            return html_content
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'NoSuchKey':
                self.logger.warning(f"No raw HTML found for {url} in S3")
                # Try local storage as fallback
                return self._local_retrieve(url, 'raw_html')
            else:
                self.logger.error(f"Error retrieving raw HTML for {url}: {e}")
                return None
        except Exception as e:
            self.logger.error(f"Error retrieving raw HTML for {url}: {e}")
            return None
    
    def retrieve_processed_text(self, url):
        """
        Retrieve processed text content from S3.
        
        Args:
            url (str): The URL of the crawled page
            
        Returns:
            tuple: (text, metadata) where text is the processed text and metadata is a dict or None
        """
        if not self.s3_client:
            return self._local_retrieve(url, 'processed_text', with_metadata=True)
        
        text_key = self._generate_key(url, 'processed_text')
        meta_key = self._generate_key(url, 'metadata')
        
        try:
            # Get processed text
            text_response = self.s3_client.get_object(Bucket=self.bucket_name, Key=text_key)
            processed_text = text_response['Body'].read().decode('utf-8')
            
            # Try to get metadata
            metadata = None
            try:
                meta_response = self.s3_client.get_object(Bucket=self.bucket_name, Key=meta_key)
                metadata = json.loads(meta_response['Body'].read().decode('utf-8'))
            except ClientError:
                self.logger.warning(f"No metadata found for {url} in S3")
            
            self.logger.info(f"Successfully retrieved processed text for {url}")
            return processed_text, metadata
        
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'NoSuchKey':
                self.logger.warning(f"No processed text found for {url} in S3")
                # Try local storage as fallback
                return self._local_retrieve(url, 'processed_text', with_metadata=True)
            else:
                self.logger.error(f"Error retrieving processed text for {url}: {e}")
                return None, None
        except Exception as e:
            self.logger.error(f"Error retrieving processed text for {url}: {e}")
            return None, None
    
    def _local_retrieve(self, url, content_type, with_metadata=False):
        """
        Retrieve content from local storage.
        
        Args:
            url (str): The URL of the crawled page
            content_type (str): Type of content (raw_html, processed_text)
            with_metadata (bool): Whether to retrieve metadata as well
            
        Returns:
            Union[str, tuple]: Content string or (content, metadata) tuple if with_metadata
        """
        try:
            # Create filepath
            url_hash = hashlib.md5(url.encode()).hexdigest()
            extension = 'html' if content_type == 'raw_html' else 'txt'
            filename = f"{url_hash}.{extension}"
            filepath = os.path.join('crawled_data', content_type, filename)
            
            # Check if file exists
            if not os.path.exists(filepath):
                self.logger.warning(f"No {content_type} found for {url} locally")
                return None if not with_metadata else (None, None)
            
            # Read content
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not with_metadata:
                return content
            
            # Try to read metadata if requested
            metadata = None
            if with_metadata:
                meta_filepath = os.path.join('crawled_data', 'metadata', f"{url_hash}.json")
                if os.path.exists(meta_filepath):
                    with open(meta_filepath, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
            
            return content, metadata
            
        except Exception as e:
            self.logger.error(f"Error retrieving {content_type} locally for {url}: {e}")
            return None if not with_metadata else (None, None)
    
    def list_stored_urls(self, content_type='raw_html', limit=100):
        """
        List URLs that have been stored in S3.
        
        Args:
            content_type (str): Type of content (raw_html, processed_text)
            limit (int): Maximum number of URLs to return
            
        Returns:
            list: List of URLs stored in S3
        """
        if not self.s3_client:
            return self._local_list_urls(content_type, limit)
        
        try:
            # List objects in the bucket with the specified prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=f"{content_type}/",
                MaxKeys=limit
            )
            
            # Extract URLs from metadata
            urls = []
            for obj in response.get('Contents', []):
                try:
                    # Get object metadata
                    head = self.s3_client.head_object(Bucket=self.bucket_name, Key=obj['Key'])
                    url = head.get('Metadata', {}).get('url')
                    if url:
                        urls.append(url)
                except Exception as e:
                    self.logger.error(f"Error retrieving metadata for {obj['Key']}: {e}")
            
            return urls
        except Exception as e:
            self.logger.error(f"Error listing URLs in S3: {e}")
            return self._local_list_urls(content_type, limit)
    
    def _local_list_urls(self, content_type, limit):
        """
        List URLs that have been stored locally.
        
        Args:
            content_type (str): Type of content (raw_html, processed_text)
            limit (int): Maximum number of URLs to return
            
        Returns:
            list: List of URLs stored locally
        """
        try:
            directory = os.path.join('crawled_data', content_type)
            if not os.path.exists(directory):
                return []
            
            # List files in the directory
            files = os.listdir(directory)[:limit]
            
            # TODO: In a real implementation, we would store URL information with each file
            # For now, we just return the filenames
            return [f.split('.')[0] for f in files]
        except Exception as e:
            self.logger.error(f"Error listing URLs locally: {e}")
            return []


# For testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create storage instance
    storage = CloudStorage(bucket_name='test-crawler-bucket')
    
    # Test storing raw HTML
    test_url = "https://example.com/test"
    test_html = "<html><body><h1>Test Page</h1><p>This is a test.</p></body></html>"
    result = storage.store_raw_html(test_url, test_html)
    print(f"Storage result: {result}")
    
    # Test storing processed text with metadata
    test_text = "Test Page\n\nThis is a test."
    test_metadata = {
        "title": "Test Page",
        "keywords": ["test", "example"],
        "extracted_links": ["https://example.com/link1", "https://example.com/link2"]
    }
    result = storage.store_processed_text(test_url, test_text, test_metadata)
    print(f"Processed text storage result: {result}")
    
    # Test retrieval
    html = storage.retrieve_raw_html(test_url)
    print(f"Retrieved HTML: {html[:50]}...")
    
    text, metadata = storage.retrieve_processed_text(test_url)
    print(f"Retrieved text: {text}")
    print(f"Retrieved metadata: {metadata}") 
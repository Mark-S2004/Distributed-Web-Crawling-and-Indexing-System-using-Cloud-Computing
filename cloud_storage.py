import boto3
import logging
from botocore.exceptions import ClientError

class CloudStorage:
    """
    CloudStorage class for managing content upload/download using AWS S3.
    Features:
    - Bucket existence check and creation if needed
    - AWS S3 only operation (no local fallback)
    """

    def __init__(self, bucket_name: str, region: str = None):
        self.bucket = bucket_name
        self.region = region

        # Initialize S3 client and check bucket
        self._initialize_s3()

    def _initialize_s3(self):
        """Initialize S3 client and check/create bucket."""
        # Create a session that respects the passed-in region
        session = boto3.session.Session(region_name=self.region)
        self.s3 = session.client('s3')

        # Check if bucket exists
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            logging.info(f"Using existing S3 bucket: {self.bucket}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                # Bucket doesn't exist, create it
                self._create_bucket()
            else:
                # Other error (permissions, etc.)
                logging.error(f"Error accessing S3 bucket: {e}")
                raise

    def _create_bucket(self):
        """Create S3 bucket if it doesn't exist."""
        if self.region and self.region != 'us-east-1':
            # For regions other than us-east-1, we need to specify LocationConstraint
            self.s3.create_bucket(
                Bucket=self.bucket,
                CreateBucketConfiguration={'LocationConstraint': self.region}
            )
        else:
            # For us-east-1, we don't specify LocationConstraint
            self.s3.create_bucket(Bucket=self.bucket)

        logging.info(f"Created new S3 bucket: {self.bucket}")
        return True

    def is_cloud_mode(self) -> bool:
        """Return whether storage is operating in cloud mode."""
        return True

    def upload_content(self, key: str, content: bytes) -> bool:
        """
        Upload bytes content under the given key to S3.
        """
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=content)
            logging.info(f"Uploaded to S3: {key}")
            return True
        except Exception as e:
            logging.error(f"S3 upload error for {key}: {e}")
            return False

    def get_content(self, key: str) -> bytes:
        """
        Download the content for the given key from S3.
        """
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            content = resp['Body'].read()
            logging.info(f"Downloaded from S3: {key}")
            return content
        except Exception as e:
            logging.error(f"S3 download error for {key}: {e}")
            return b""

# This method is already defined above

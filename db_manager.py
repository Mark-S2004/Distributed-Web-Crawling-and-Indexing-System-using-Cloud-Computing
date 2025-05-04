# db_manager.py

import boto3
import json
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from datetime import datetime
from typing import List

class DBManager:
    """
    Database Manager for storing crawl metadata using AWS DynamoDB.
    Provides methods to track crawled URLs, node status, system metrics,
    and URL-tracking for the indexer (url_tracking table).
    """

    def __init__(self):
        """Initialize DynamoDB resources and ensure all tables exist."""
        self.dynamodb = boto3.resource('dynamodb')
        # DynamoDB tables
        self.url_tracking_table  = self.dynamodb.Table('url_tracking')
        self.crawled_urls_table  = self.dynamodb.Table('crawled-urls')
        self.node_status_table   = self.dynamodb.Table('node-status')
        self.metrics_table       = self.dynamodb.Table('crawler-metrics')

        # Core tables
        self.crawled_urls_table = self._ensure_table_exists('crawled-urls', 'url')
        self.node_status_table  = self._ensure_table_exists('node-status', 'node_id')
        self.metrics_table      = self._ensure_table_exists('crawler-metrics', 'metric_id')
        # URL-tracking table (url: PK, indexed: 'yes'|'no', last_updated)
        self.url_tracking_table = self._ensure_table_exists('url_tracking', 'url')

        logging.info("DBManager initialized with DynamoDB tables")

    def _ensure_table_exists(self, table_name: str, primary_key: str):
        """
        Ensure a DynamoDB table with given name and primary key exists.
        If not, create it with modest provisioned throughput.
        """
        try:
            table = self.dynamodb.Table(table_name)
            _ = table.table_status  # will throw if missing
            return table

        except ClientError as e:
            code = e.response['Error']['Code']
            if code == 'ResourceNotFoundException':
                table = self.dynamodb.create_table(
                    TableName=table_name,
                    KeySchema=[{'AttributeName': primary_key, 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': primary_key, 'AttributeType': 'S'}],
                    ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                )
                table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
                logging.info(f"Created DynamoDB table: {table_name}")
                return table
            else:
                logging.error(f"Error accessing DynamoDB table {table_name}: {e}")
                raise

    # ──────────────────────────────────────────────────────────────────── #
    #             Crawled URLs, Node Status & Metrics Methods            #
    # ──────────────────────────────────────────────────────────────────── #
# Node status update method

    def add_crawled_url(self, url: str, metadata: dict = None) -> bool:
        """Record that `url` has been crawled, with optional metadata."""
        item = {
            'url': url,
            'crawl_time': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        try:
            self.crawled_urls_table.put_item(Item=item)
            return True
        except ClientError as e:
            logging.error(f"Error adding crawled URL to DynamoDB: {e}")
            return False

    def url_exists(self, url: str) -> bool:
        """Return True if `url` already exists in crawled-urls table."""
        try:
            resp = self.crawled_urls_table.get_item(Key={'url': url})
            return 'Item' in resp
        except ClientError as e:
            logging.error(f"Error checking URL in DynamoDB: {e}")
            return False

    def update_node_status(self, node_id: str, status: dict) -> bool:
        """Upsert heartbeat/status for a crawler or indexer node."""
        item = {
            'node_id': node_id,
            'status': status,
            'last_updated': datetime.now().isoformat()
        }
        try:
            self.node_status_table.put_item(Item=item)
            return True
        except ClientError as e:
            logging.error(f"Error updating node status in DynamoDB: {e}")
            return False

    def add_metric(self, metric_id: str, metric_name: str, value: float, metadata: dict = None) -> bool:
        """Record a monitoring metric."""
        item = {
            'metric_id': metric_id,
            'metric_name': metric_name,
            'value': value,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        try:
            self.metrics_table.put_item(Item=item)
            return True
        except ClientError as e:
            logging.error(f"Error adding metric to DynamoDB: {e}")
            return False

    # ──────────────────────────────────────────────────────────────────── #
    #                 URL-TRACKING FOR THE INDEXER                       #
    # ──────────────────────────────────────────────────────────────────── #

    def add_url_to_tracking(self, url: str) -> None:
        """
        Add a discovered URL to the tracking table with indexed='no'.
        If already present, ignore silently.
        """
        item = {
            'url': url,
            'indexed': 'no',
            'last_updated': datetime.now().isoformat()
        }
        try:
            self.url_tracking_table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(#u)',
                ExpressionAttributeNames={'#u': 'url'}
            )
        except ClientError as e:
            code = e.response['Error']['Code']
            if code != 'ConditionalCheckFailedException':
                logging.error(f"DynamoDB error inserting URL into tracking: {e}")


    # URL fetching and indexing methods

    def mark_url_as_fetched(self, url: str) -> None:
        """
        Called by the crawler after it uploads a page to S3.
        Marks fetched = 'yes' in the url_tracking table.
        """
        try:
            self.url_tracking_table.update_item(
                Key={'url': url},
                UpdateExpression='SET fetched = :yes, last_updated = :ts',
                ExpressionAttributeValues={
                    ':yes': 'yes',
                    ':ts' : datetime.now().isoformat()
                }
            )
        except ClientError as e:
            logging.error(f"DynamoDB error marking URL fetched: {e}")


    def mark_url_as_indexed(self, url: str) -> None:
        """
        After the indexer processes a URL, mark it indexed='yes'
        so it never gets picked up again.
        """
        try:
            self.url_tracking_table.update_item(
                Key={'url': url},
                UpdateExpression='SET #ix = :yes, last_updated = :ts',
                ExpressionAttributeNames={'#ix': 'indexed'},
                ExpressionAttributeValues={
                    ':yes': 'yes',
                    ':ts' : datetime.now().isoformat()
                }
            )
        except ClientError as e:
            logging.error(f"DynamoDB error marking URL indexed: {e}")

    def get_unindexed_urls(self, limit: int = 50) -> List[str]:
        """
        Scan the tracking table for up to `limit` URLs where indexed='no' AND fetched='yes'.
        This ensures we only try to index content that has been successfully crawled.
        """
        try:
            # Use a more specific filter expression to get only fetched but unindexed URLs
            from boto3.dynamodb.conditions import Attr

            resp = self.url_tracking_table.scan(
                FilterExpression=Key('indexed').eq('no') & Attr('fetched').eq('yes')
            )

            urls = [item['url'] for item in resp.get('Items', [])]
            if urls:
                logging.info(f"Found {len(urls)} unindexed URLs that have been fetched")
            return urls[:limit]
        except ClientError as e:
            logging.error(f"DynamoDB error scanning unindexed URLs: {e}")
            return []

    def get_url_info(self, url: str) -> dict:
        """
        Get all information about a URL from the tracking table.
        Returns None if the URL is not found.
        """
        try:
            resp = self.url_tracking_table.get_item(Key={'url': url})
            return resp.get('Item')
        except ClientError as e:
            logging.error(f"DynamoDB error getting URL info: {e}")
            return None

    def increment_index_failure_count(self, url: str) -> int:
        """
        Increment the failure count for a URL that couldn't be indexed.
        Returns the new failure count.
        """
        try:
            resp = self.url_tracking_table.update_item(
                Key={'url': url},
                UpdateExpression='ADD index_failures :inc SET last_updated = :ts',
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':ts': datetime.now().isoformat()
                },
                ReturnValues='UPDATED_NEW'
            )

            # Get the new failure count
            new_attrs = resp.get('Attributes', {})
            failure_count = new_attrs.get('index_failures', 1)

            logging.info(f"Incremented failure count for {url} to {failure_count}")
            return failure_count
        except ClientError as e:
            logging.error(f"DynamoDB error incrementing failure count: {e}")
            return 1  # Default to 1 on error

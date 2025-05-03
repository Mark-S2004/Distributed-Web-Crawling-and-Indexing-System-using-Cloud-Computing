import boto3
import json
import logging
from botocore.exceptions import ClientError
from datetime import datetime
import os
import sqlite3

class DBManager:
    """
    Database Manager for storing crawl metadata using AWS DynamoDB.
    Provides methods to track crawled URLs, node status, and system metrics.
    Falls back to SQLite if DynamoDB is not available.
    """
    
    def __init__(self):
        """Initialize database connections for both DynamoDB and local SQLite"""
        self.use_cloud = True
        
        try:
            # Initialize DynamoDB resources
            self.dynamodb = boto3.resource('dynamodb')
            
            # Set up tables
            self.crawled_urls_table = self._ensure_table_exists('crawled-urls', 'url')
            self.node_status_table = self._ensure_table_exists('node-status', 'node_id')
            self.metrics_table = self._ensure_table_exists('crawler-metrics', 'metric_id')
            
            logging.info("DBManager initialized with DynamoDB tables")
        except Exception as e:
            logging.warning(f"Failed to initialize DynamoDB, falling back to SQLite: {e}")
            self.use_cloud = False
            
            # Set up local SQLite database
            os.makedirs('data/db', exist_ok=True)
            self.db_path = 'data/db/crawler.db'
            self._setup_sqlite()
    
    def _ensure_table_exists(self, table_name, primary_key):
        """Ensure DynamoDB table exists, creating it if needed"""
        try:
            # Check if table exists
            table = self.dynamodb.Table(table_name)
            table.table_status  # This will throw an exception if table doesn't exist
            return table
        except ClientError as e:
            # Table doesn't exist, create it
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                table = self.dynamodb.create_table(
                    TableName=table_name,
                    KeySchema=[
                        {
                            'AttributeName': primary_key,
                            'KeyType': 'HASH'  # Partition key
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': primary_key,
                            'AttributeType': 'S'
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
                # Wait for table creation
                table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
                logging.info(f"Created DynamoDB table: {table_name}")
                return table
            else:
                # Some other error
                raise
    
    def _setup_sqlite(self):
        """Set up SQLite database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawled_urls (
                url TEXT PRIMARY KEY,
                crawl_time TEXT,
                metadata TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS node_status (
                node_id TEXT PRIMARY KEY,
                status TEXT,
                last_updated TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                metric_id TEXT PRIMARY KEY,
                timestamp TEXT,
                metric_name TEXT,
                value REAL,
                metadata TEXT
            )
        ''')
        
        # Add new URL tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS url_tracking (
                url TEXT PRIMARY KEY,
                indexed TEXT DEFAULT 'no',
                last_updated TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("SQLite database initialized")
    
    def add_crawled_url(self, url, metadata=None):
        """Add a URL to the crawled URLs database"""
        timestamp = datetime.now().isoformat()
        metadata = metadata or {}
        
        if self.use_cloud:
            try:
                self.crawled_urls_table.put_item(
                    Item={
                        'url': url,
                        'crawl_time': timestamp,
                        'metadata': metadata
                    }
                )
                return True
            except Exception as e:
                logging.error(f"Error adding URL to DynamoDB: {e}")
                # Fall back to SQLite
                return self._add_url_sqlite(url, timestamp, metadata)
        else:
            # Use SQLite
            return self._add_url_sqlite(url, timestamp, metadata)
    
    def _add_url_sqlite(self, url, timestamp, metadata):
        """Add a URL to the SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT OR REPLACE INTO crawled_urls (url, crawl_time, metadata) VALUES (?, ?, ?)",
                (url, timestamp, json.dumps(metadata))
            )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Error adding URL to SQLite: {e}")
            return False
    
    def url_exists(self, url):
        """Check if a URL has already been crawled"""
        if self.use_cloud:
            try:
                response = self.crawled_urls_table.get_item(
                    Key={'url': url}
                )
                return 'Item' in response
            except Exception as e:
                logging.error(f"Error checking URL in DynamoDB: {e}")
                # Fall back to SQLite
                return self._url_exists_sqlite(url)
        else:
            # Use SQLite
            return self._url_exists_sqlite(url)
    
    def _url_exists_sqlite(self, url):
        """Check if a URL exists in the SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT 1 FROM crawled_urls WHERE url = ?", (url,))
            result = cursor.fetchone() is not None
            
            conn.close()
            return result
        except Exception as e:
            logging.error(f"Error checking URL in SQLite: {e}")
            return False
    
    def update_node_status(self, node_id, status):
        """Update the status of a node"""
        timestamp = datetime.now().isoformat()
        
        if self.use_cloud:
            try:
                self.node_status_table.put_item(
                    Item={
                        'node_id': node_id,
                        'status': status,
                        'last_updated': timestamp
                    }
                )
                return True
            except Exception as e:
                logging.error(f"Error updating node status in DynamoDB: {e}")
                # Fall back to SQLite
                return self._update_node_status_sqlite(node_id, status, timestamp)
        else:
            # Use SQLite
            return self._update_node_status_sqlite(node_id, status, timestamp)
    
    def _update_node_status_sqlite(self, node_id, status, timestamp):
        """Update node status in SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT OR REPLACE INTO node_status (node_id, status, last_updated) VALUES (?, ?, ?)",
                (node_id, json.dumps(status) if isinstance(status, dict) else status, timestamp)
            )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Error updating node status in SQLite: {e}")
            return False
    
    def get_node_status(self, node_id):
        """Get the status of a node"""
        if self.use_cloud:
            try:
                response = self.node_status_table.get_item(
                    Key={'node_id': node_id}
                )
                return response.get('Item')
            except Exception as e:
                logging.error(f"Error getting node status from DynamoDB: {e}")
                # Fall back to SQLite
                return self._get_node_status_sqlite(node_id)
        else:
            # Use SQLite
            return self._get_node_status_sqlite(node_id)
    
    def _get_node_status_sqlite(self, node_id):
        """Get node status from SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT node_id, status, last_updated FROM node_status WHERE node_id = ?", (node_id,))
            row = cursor.fetchone()
            
            conn.close()
            
            if row:
                status = row[1]
                try:
                    # Try to parse as JSON if it's a JSON string
                    status = json.loads(status)
                except:
                    pass
                
                return {
                    'node_id': row[0],
                    'status': status,
                    'last_updated': row[2]
                }
            return None
        except Exception as e:
            logging.error(f"Error getting node status from SQLite: {e}")
            return None
    
    def record_metric(self, metric_name, value, metadata=None):
        """Record a metric data point"""
        timestamp = datetime.now().isoformat()
        metric_id = f"{metric_name}_{timestamp}"
        metadata = metadata or {}
        
        if self.use_cloud:
            try:
                self.metrics_table.put_item(
                    Item={
                        'metric_id': metric_id,
                        'timestamp': timestamp,
                        'metric_name': metric_name,
                        'value': value,
                        'metadata': metadata
                    }
                )
                return True
            except Exception as e:
                logging.error(f"Error recording metric in DynamoDB: {e}")
                # Fall back to SQLite
                return self._record_metric_sqlite(metric_id, metric_name, value, timestamp, metadata)
        else:
            # Use SQLite
            return self._record_metric_sqlite(metric_id, metric_name, value, timestamp, metadata)
    
    def _record_metric_sqlite(self, metric_id, metric_name, value, timestamp, metadata):
        """Record a metric in SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO metrics (metric_id, timestamp, metric_name, value, metadata) VALUES (?, ?, ?, ?, ?)",
                (metric_id, timestamp, metric_name, value, json.dumps(metadata))
            )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Error recording metric in SQLite: {e}")
            return False
    
    def get_metrics(self, metric_name, start_time=None, end_time=None, limit=100):
        """Retrieve metrics for analysis"""
        if self.use_cloud:
            try:
                # We need to use a secondary index or scan filtering for DynamoDB
                # This is a simplified implementation
                response = self.metrics_table.scan(
                    FilterExpression='metric_name = :metric_name',
                    ExpressionAttributeValues={
                        ':metric_name': metric_name
                    },
                    Limit=limit
                )
                return response.get('Items', [])
            except Exception as e:
                logging.error(f"Error getting metrics from DynamoDB: {e}")
                # Fall back to SQLite
                return self._get_metrics_sqlite(metric_name, start_time, end_time, limit)
        else:
            # Use SQLite
            return self._get_metrics_sqlite(metric_name, start_time, end_time, limit)
    
    def _get_metrics_sqlite(self, metric_name, start_time=None, end_time=None, limit=100):
        """Get metrics from SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            query = "SELECT metric_id, timestamp, metric_name, value, metadata FROM metrics WHERE metric_name = ?"
            params = [metric_name]
            
            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time)
            
            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            conn.close()
            
            results = []
            for row in rows:
                metadata = row[4]
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
                
                results.append({
                    'metric_id': row[0],
                    'timestamp': row[1],
                    'metric_name': row[2],
                    'value': row[3],
                    'metadata': metadata
                })
            
            return results
        except Exception as e:
            logging.error(f"Error getting metrics from SQLite: {e}")
            return []
    
    def is_cloud_mode(self):
        """Check if the database is operating in cloud mode"""
        return self.use_cloud
    
    def add_url_to_tracking(self, url):
        """Add a URL to the tracking table if it doesn't exist"""
        timestamp = datetime.now().isoformat()
        
        if self.use_cloud:
            try:
                self.url_tracking_table.put_item(
                    Item={
                        'url': url,
                        'indexed': 'no',
                        'last_updated': timestamp
                    },
                    ConditionExpression='attribute_not_exists(url)'
                )
                return True
            except Exception as e:
                if 'ConditionalCheckFailedException' not in str(e):
                    logging.error(f"Error adding URL to tracking table: {e}")
                return False
        else:
            return self._add_url_tracking_sqlite(url, timestamp)
    
    def _add_url_tracking_sqlite(self, url, timestamp):
        """Add a URL to the SQLite tracking table if it doesn't exist"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT OR IGNORE INTO url_tracking (url, indexed, last_updated) VALUES (?, 'no', ?)",
                (url, timestamp)
            )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Error adding URL to SQLite tracking table: {e}")
            return False
    
    def mark_url_as_indexed(self, url):
        """Mark a URL as indexed in the tracking table"""
        timestamp = datetime.now().isoformat()
        
        if self.use_cloud:
            try:
                self.url_tracking_table.update_item(
                    Key={'url': url},
                    UpdateExpression='SET indexed = :indexed, last_updated = :timestamp',
                    ExpressionAttributeValues={
                        ':indexed': 'yes',
                        ':timestamp': timestamp
                    }
                )
                return True
            except Exception as e:
                logging.error(f"Error marking URL as indexed: {e}")
                return False
        else:
            return self._mark_url_indexed_sqlite(url, timestamp)
    
    def _mark_url_indexed_sqlite(self, url, timestamp):
        """Mark a URL as indexed in SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE url_tracking SET indexed = 'yes', last_updated = ? WHERE url = ?",
                (timestamp, url)
            )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Error marking URL as indexed in SQLite: {e}")
            return False
    
    def get_unindexed_urls(self, limit=100):
        """Get a list of unindexed URLs"""
        if self.use_cloud:
            try:
                response = self.url_tracking_table.scan(
                    FilterExpression='indexed = :indexed',
                    ExpressionAttributeValues={
                        ':indexed': 'no'
                    },
                    Limit=limit
                )
                return [item['url'] for item in response.get('Items', [])]
            except Exception as e:
                logging.error(f"Error getting unindexed URLs: {e}")
                return []
        else:
            return self._get_unindexed_urls_sqlite(limit)
    
    def _get_unindexed_urls_sqlite(self, limit):
        """Get unindexed URLs from SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT url FROM url_tracking WHERE indexed = 'no' LIMIT ?",
                (limit,)
            )
            
            urls = [row[0] for row in cursor.fetchall()]
            conn.close()
            return urls
        except Exception as e:
            logging.error(f"Error getting unindexed URLs from SQLite: {e}")
            return [] 
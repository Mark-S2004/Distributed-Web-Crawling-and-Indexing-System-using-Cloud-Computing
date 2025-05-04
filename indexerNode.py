#!/usr/bin/env python3
import os
import time
import logging
import urllib.parse
import boto3
import signal
import threading
import json
import socket
import shutil
import sys
from datetime import datetime
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# Increase the recursion limit to handle deeper recursion
sys.setrecursionlimit(20000)  # Double the recursion limit

# Set a maximum content size to avoid memory issues
MAX_CONTENT_SIZE = 50000  # 50KB max content size

# Import Whoosh libraries
from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, TEXT, ID, DATETIME, KEYWORD, STORED
from whoosh.analysis import StemmingAnalyzer, StandardAnalyzer
from whoosh.qparser import QueryParser, MultifieldParser, OrGroup
from whoosh.query import Term, And, Or, Not

from cloud_storage import CloudStorage
from cloud_queue import CloudQueue
from db_manager import DBManager

# Message types
MSG_HEARTBEAT = 'heartbeat'
MSG_ERROR = 'error'
MSG_TERMINATE = 'terminate'

# Global variables for graceful shutdown
shutdown_event = threading.Event()

def signal_handler(sig, frame):
    """Handle termination signals gracefully."""
    logging.info("Received termination signal. Shutting down indexer gracefully...")
    shutdown_event.set()

# ─── Logging setup ─────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", "indexer.log")

# Clear any existing handlers to avoid duplicate logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Indexer - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

class EnhancedTextExtractor:
    """Enhanced text extraction with NLP capabilities."""

    def __init__(self):
        """Initialize the text extractor with NLP tools."""
        try:
            # Download NLTK resources if not already downloaded
            import nltk
            try:
                nltk.data.find('tokenizers/punkt')
            except LookupError:
                nltk.download('punkt')

            try:
                nltk.data.find('corpora/stopwords')
            except LookupError:
                nltk.download('stopwords')

            self.stemmer = PorterStemmer()
            self.stop_words = set(stopwords.words('english'))
            logging.info("NLP tools initialized successfully")
        except Exception as e:
            logging.error(f"Error initializing NLP tools: {e}")
            # Fallback to simpler processing
            self.stemmer = None
            self.stop_words = set()

    def extract_text(self, html_content):
        """Extract meaningful text from HTML content with enhanced processing."""
        try:
            # Limit HTML content size before parsing to avoid memory issues
            if len(html_content) > 500000:  # 500KB
                html_content = html_content[:500000]
                logging.warning("HTML content truncated before parsing due to excessive size")

            soup = BeautifulSoup(html_content, 'html.parser', parse_only=None)

            # Remove script, style, and other non-content elements
            for element in soup(['script', 'style', 'header', 'footer', 'nav', 'iframe', 'noscript']):
                element.extract()

            # Get text
            text = soup.get_text()

            # Break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())

            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

            # Remove blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)

            # Limit text length to avoid recursion issues
            if len(text) > MAX_CONTENT_SIZE:
                text = text[:MAX_CONTENT_SIZE] + "... (content truncated)"
                logging.warning("Content truncated due to excessive length")

            return text
        except Exception as e:
            logging.error(f"Error extracting text from HTML: {e}")
            # Return a very short error message to avoid any issues
            return "Error extracting text"

    def extract_keywords(self, text, max_keywords=20):
        """Extract important keywords from text using NLP techniques."""
        if not text:
            return []

        try:
            # Tokenize text
            tokens = word_tokenize(text.lower())

            # Remove stopwords and short words
            filtered_tokens = [w for w in tokens if w not in self.stop_words and len(w) > 2]

            # Apply stemming
            if self.stemmer:
                stemmed_tokens = [self.stemmer.stem(w) for w in filtered_tokens]
            else:
                stemmed_tokens = filtered_tokens

            # Count frequency
            from collections import Counter
            word_freq = Counter(stemmed_tokens)

            # Get most common words
            keywords = [word for word, freq in word_freq.most_common(max_keywords)]

            return keywords
        except Exception as e:
            logging.error(f"Error extracting keywords: {e}")
            return []

    def extract_summary(self, text, max_length=200):
        """Extract a summary of the text."""
        if not text:
            return ""

        try:
            # Simple summary: first few sentences
            sentences = text.split('.')
            summary = '.'.join(sentences[:3]) + '.'

            # Truncate if too long
            if len(summary) > max_length:
                summary = summary[:max_length] + '...'

            return summary
        except Exception as e:
            logging.error(f"Error extracting summary: {e}")
            return text[:max_length] + '...' if text else ""

class WhooshIndexer:
    """Enhanced indexer using Whoosh with proper error handling."""

    def __init__(self, index_dir="search_index"):
        """Initialize the Whoosh indexer."""
        self.index_dir = index_dir
        os.makedirs(index_dir, exist_ok=True)

        # Define schema with additional fields
        self.schema = Schema(
            url=ID(stored=True, unique=True),
            title=TEXT(stored=True),
            content=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            summary=STORED,
            keywords=KEYWORD(stored=True, commas=True),
            last_updated=DATETIME(stored=True),
            content_type=STORED
        )

        # Initialize or open the index
        self._initialize_index()

        logging.info(f"Whoosh indexer initialized with directory: {index_dir}")

    def _initialize_index(self):
        """Initialize or open the index with proper error handling."""
        try:
            # Check if index exists
            if not exists_in(self.index_dir):
                self.ix = create_in(self.index_dir, self.schema)
                logging.info("Created new Whoosh index")
            else:
                try:
                    # Try to open existing index
                    self.ix = open_dir(self.index_dir)
                    logging.info("Opened existing Whoosh index")
                except Exception as e:
                    logging.error(f"Error opening existing index: {e}")
                    # Backup and recreate
                    self._backup_and_recreate()
        except Exception as e:
            logging.error(f"Error initializing index: {e}")
            # Try to recreate
            self._backup_and_recreate()

    def _backup_and_recreate(self):
        """Backup existing index and create a new one."""
        try:
            # Create backup directory
            backup_dir = f"{self.index_dir}_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            if os.path.exists(self.index_dir):
                shutil.copytree(self.index_dir, backup_dir)
                logging.info(f"Backed up index to {backup_dir}")

                # Remove existing index directory completely
                shutil.rmtree(self.index_dir)
                os.makedirs(self.index_dir)

            # Create new index
            self.ix = create_in(self.index_dir, self.schema)
            logging.info("Recreated Whoosh index after backup")
        except Exception as e:
            logging.error(f"Error backing up and recreating index: {e}")
            # Last resort - delete and create new
            try:
                shutil.rmtree(self.index_dir, ignore_errors=True)
                os.makedirs(self.index_dir, exist_ok=True)
                self.ix = create_in(self.index_dir, self.schema)
                logging.info("Created new Whoosh index after removing old one")
            except Exception as e2:
                logging.error(f"Critical error recreating index: {e2}")
                raise

    def add_document(self, url, title, content, keywords=None, summary=None, content_type="text/html"):
        """Add a document to the index with enhanced metadata."""
        # Further limit content size for indexing
        if content and len(content) > MAX_CONTENT_SIZE:
            content = content[:MAX_CONTENT_SIZE]
            logging.warning(f"Content for {url} truncated to {MAX_CONTENT_SIZE} characters for indexing")

        # Try with AsyncWriter first
        try:
            # Use AsyncWriter to avoid recursion issues
            from whoosh.writing import AsyncWriter
            writer = AsyncWriter(self.ix)

            # Prepare document with size limits
            doc = {
                "url": url,
                "title": title or url,
                "content": content if content else "",
                "last_updated": datetime.now(),
                "content_type": content_type
            }

            # Add optional fields if provided
            if keywords:
                doc["keywords"] = ", ".join(keywords[:30])  # Limit keywords even more

            if summary:
                doc["summary"] = summary[:500]  # Limit summary size even more

            # Add to index
            writer.add_document(**doc)
            writer.commit()
            return True
        except Exception as e:
            logging.error(f"Error adding document to index with AsyncWriter: {e}")
            try:
                writer.cancel()
            except:
                pass

            # Try with standard writer as fallback
            try:
                logging.info(f"Trying fallback indexing method for {url}")

                # Create a new writer directly
                writer = self.ix.writer()

                # Create a minimal document with just essential fields
                minimal_doc = {
                    "url": url,
                    "title": title or url,
                    "content": content[:5000] if content else "",  # Very limited content
                    "last_updated": datetime.now()
                }

                # Add to index
                writer.add_document(**minimal_doc)
                writer.commit()
                logging.info(f"Successfully indexed {url} using fallback method")
                return True
            except Exception as e2:
                logging.error(f"Fallback indexing also failed for {url}: {e2}")
                try:
                    writer.cancel()
                except:
                    pass

                # Last resort - try to recreate the index if it seems corrupted
                if "index is not readable" in str(e2) or "corrupt" in str(e2):
                    try:
                        logging.warning("Index may be corrupted, attempting to recreate")
                        self._backup_and_recreate()
                    except:
                        pass

                # Mark as indexed anyway to avoid retrying problematic URLs
                return False

    def search(self, query_string, fields=None, limit=10):
        """
        Search the index with enhanced capabilities.

        Supports:
        - Multi-field search
        - Boolean operators (AND, OR, NOT)
        - Phrase search with quotes
        """
        try:
            # Default to searching title and content
            if not fields:
                fields = ["title", "content"]

            # Create parser
            parser = MultifieldParser(fields, schema=self.ix.schema, group=OrGroup)

            # Parse query
            query = parser.parse(query_string)

            # Execute search
            with self.ix.searcher() as searcher:
                results = searcher.search(query, limit=limit)

                # Format results
                formatted_results = []
                for hit in results:
                    result = {
                        "url": hit["url"],
                        "title": hit["title"],
                        "score": hit.score
                    }

                    # Add summary if available
                    if "summary" in hit:
                        result["summary"] = hit["summary"]

                    # Add keywords if available
                    if "keywords" in hit:
                        result["keywords"] = hit["keywords"]

                    formatted_results.append(result)

                return formatted_results
        except Exception as e:
            logging.error(f"Error searching index: {e}")
            return []

    def close(self):
        """Close the index."""
        # Whoosh doesn't require explicit closing
        pass

def indexer_process(bucket: str = 'web-crawler-data-storage', region: str = None,
                   status_queue: str = 'crawler-status-queue'):
    """
    Enhanced Indexer:
      - Sets default AWS region for boto3
      - Polls DynamoDB for URLs with indexed='no' AND fetched='yes'
      - Downloads page HTML from S3 under URL-derived key
      - Processes HTML with enhanced text extraction and NLP
      - Adds document to Whoosh index with robust error handling
      - Marks URLs as indexed in DynamoDB
      - Handles graceful shutdown
    """
    # ─── Signal handlers for graceful shutdown ─────────────────────────
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ─── AWS region setup ─────────────────────────────────────────────
    if region:
        boto3.setup_default_session(region_name=region)

    storage = CloudStorage(bucket_name=bucket, region=region)
    db = DBManager()
    task_queue = CloudQueue(queue_name='crawler-url-queue', status_queue_name=status_queue)

    node_id = socket.gethostname()

    # ─── Indexer stats ───────────────────────────────────────────────
    stats = {
        'urls_indexed': 0,
        'index_errors': 0,
        'start_time': datetime.now().isoformat()
    }

    # ─── Initialize enhanced components ────────────────────────────────
    text_extractor = EnhancedTextExtractor()
    indexer = WhooshIndexer()
    logging.info("Enhanced indexer components initialized")

    # ─── Heartbeat thread ───────────────────────────────────────────
    def heartbeat():
        while not shutdown_event.is_set():
            try:
                # Send heartbeat with stats
                task_queue.send_status_message({
                    'type': MSG_HEARTBEAT,
                    'node_id': f"indexer-{node_id}",
                    'timestamp': time.time(),
                    'stats': stats
                })
                # Update node status in DynamoDB
                db.update_node_status(f"indexer-{node_id}", {
                    'last_beat': datetime.now().isoformat(),
                    'stats': stats
                })
            except Exception as e:
                logging.error(f"Error in heartbeat thread: {e}")

            # Sleep for 10 seconds or until shutdown
            shutdown_event.wait(10)

    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()

    # ─── Main indexing loop ─────────────────────────────────────────────
    try:
        while not shutdown_event.is_set():
            try:
                # Check for termination messages first
                status_messages = task_queue.receive_status_messages(WaitTimeSeconds=1, MaxNumber=5)
                for status_msg in status_messages:
                    try:
                        body = json.loads(status_msg.get('Body', '{}'))
                        if body.get('type') == MSG_TERMINATE:
                            logging.info("Received terminate message from master")
                            shutdown_event.set()
                            break
                    except Exception:
                        pass  # Ignore malformed messages
                    finally:
                        task_queue.delete_status_message(status_msg)

                # Exit the loop if shutdown is requested
                if shutdown_event.is_set():
                    break

                # Get unindexed URLs from DynamoDB
                urls = db.get_unindexed_urls(limit=5)  # Reduced batch size
                if not urls:
                    # No URLs to index, wait a bit and check again
                    shutdown_event.wait(5)
                    continue

                # Process each URL
                for url in urls:
                    # Skip non-absolute URLs
                    if not url.lower().startswith(('http://', 'https://')):
                        logging.info(f"Skipping non-absolute URL: {url}")
                        db.mark_url_as_indexed(url)  # Mark as indexed to avoid retrying
                        continue

                    try:
                        # Get content from storage
                        key = urllib.parse.quote_plus(url)
                        data = storage.get_content(key)
                        if not data:
                            logging.warning(f"No content for {url}")

                            # Check if the URL has been marked as fetched
                            url_info = db.get_url_info(url)
                            if url_info and url_info.get('fetched') == 'yes':
                                # If it's marked as fetched but we can't find content,
                                # increment a failure counter
                                failure_count = db.increment_index_failure_count(url)

                                # If we've tried too many times, mark as indexed to avoid retrying forever
                                if failure_count >= 2:  # Reduced from 3 to 2
                                    logging.warning(f"Giving up on {url} after {failure_count} attempts")
                                    db.mark_url_as_indexed(url)
                            else:
                                # If it's not marked as fetched, it might be a race condition
                                # Don't mark as indexed yet, let it be retried later
                                logging.info(f"URL {url} not marked as fetched yet, will retry later")

                            continue

                        # Check if content is too large before decoding
                        if len(data) > 1000000:  # 1MB
                            logging.warning(f"Content for {url} is too large ({len(data)} bytes), truncating")
                            data = data[:1000000]

                        # Decode content
                        try:
                            html_content = data.decode('utf-8')
                        except UnicodeDecodeError:
                            try:
                                # Try with different encoding
                                html_content = data.decode('latin-1')
                            except Exception:
                                logging.warning(f"Could not decode content for {url}")
                                db.mark_url_as_indexed(url)
                                continue

                        # Extract title from HTML - with error handling
                        title = ""
                        try:
                            # Use a simple regex to extract title instead of full parsing
                            import re
                            title_match = re.search('<title>(.*?)</title>', html_content, re.IGNORECASE | re.DOTALL)
                            if title_match:
                                title = title_match.group(1).strip()
                        except Exception as e:
                            logging.warning(f"Error extracting title for {url}: {e}")

                        # Enhanced text extraction
                        text_content = text_extractor.extract_text(html_content)

                        # Extract keywords and summary - with error handling
                        try:
                            keywords = text_extractor.extract_keywords(text_content)
                            summary = text_extractor.extract_summary(text_content)
                        except Exception as e:
                            logging.warning(f"Error extracting keywords/summary for {url}: {e}")
                            keywords = []
                            summary = text_content[:200] + "..." if len(text_content) > 200 else text_content

                        # Add document to index
                        success = indexer.add_document(
                            url=url,
                            title=title or url,
                            content=text_content,
                            keywords=keywords,
                            summary=summary,
                            content_type="text/html"
                        )

                        if success:
                            # Mark URL as indexed in DynamoDB
                            db.mark_url_as_indexed(url)

                            # Update stats
                            stats['urls_indexed'] += 1

                            logging.info(f"Indexed & marked: {url}")
                        else:
                            logging.error(f"Failed to index {url}")
                            stats['index_errors'] += 1

                            # Mark as indexed anyway to avoid retrying problematic URLs
                            db.mark_url_as_indexed(url)

                    except Exception as e:
                        logging.error(f"Indexing error for {url}: {e}")
                        stats['index_errors'] += 1
                        # Try to mark as indexed to avoid retrying problematic URLs
                        try:
                            db.mark_url_as_indexed(url)
                        except Exception:
                            pass

                    # Small delay between processing URLs to avoid memory buildup
                    time.sleep(0.1)

                # Small delay between batches
                shutdown_event.wait(1)

            except Exception as e:
                logging.error(f"Unexpected error in indexer main loop: {e}")
                shutdown_event.wait(5)  # Wait before retrying

    finally:
        # Perform cleanup
        logging.info("Indexer shutting down...")

        # Close the indexer
        try:
            indexer.close()
        except Exception as e:
            logging.error(f"Error closing indexer: {e}")

        # Send final heartbeat with shutdown notification
        try:
            task_queue.send_status_message({
                'type': MSG_HEARTBEAT,
                'node_id': f"indexer-{node_id}",
                'timestamp': time.time(),
                'stats': stats,
                'status': 'shutting_down'
            })
        except Exception as e:
            logging.error(f"Error sending final heartbeat: {e}")

        # Wait for heartbeat thread to finish
        heartbeat_thread.join(timeout=2.0)

        # Shutdown cloud services
        try:
            task_queue.shutdown_queue()
        except Exception as e:
            logging.error(f"Error shutting down queue: {e}")

        logging.info("Indexer shutdown complete")

if __name__ == "__main__":
    import sys
    indexer_process(*sys.argv[1:])

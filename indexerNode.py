#!/usr/bin/env python3
import time
import logging
from bs4 import BeautifulSoup
import re
import os
import json
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh import scoring
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import socket
from cloud_queue import CloudQueue
from cloud_storage import CloudStorage

# Define message types for AWS communication
MSG_TERMINATE = 'terminate'
MSG_URLS_DISCOVERED = 'urls_discovered'
MSG_INDEX_CONTENT = 'index_content'
MSG_HEARTBEAT = 'heartbeat'
MSG_ERROR = 'error'

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/wordnet')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('stopwords')

class EnhancedIndexer:
    def __init__(self, index_dir="search_index", cloud_storage=None):
        self.index_dir = index_dir
        self.setup_index()
        self.lemmatizer = WordNetLemmatizer()
        self.stop_words = set(stopwords.words('english'))
        # Initialize cloud storage if provided
        self.cloud_storage = cloud_storage
        
    def setup_index(self):
        """Initialize or open the Whoosh index"""
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)
            
        # Define the schema for our index
        self.schema = Schema(
            url=ID(stored=True, unique=True),
            title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            content=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            keywords=TEXT(stored=True),
            summary=STORED,
            last_updated=STORED
        )
        
        # Create or open the index
        if not os.listdir(self.index_dir):
            self.ix = create_in(self.index_dir, self.schema)
        else:
            self.ix = open_dir(self.index_dir)
    
    def extract_text_from_html(self, html_content):
        """Enhanced text extraction from HTML with better cleaning"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'meta', 'link']):
            element.decompose()
            
        # Extract title
        title = soup.title.string if soup.title else ""
        
        # Extract main content
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        if main_content:
            text = main_content.get_text(separator=' ', strip=True)
        else:
            text = soup.get_text(separator=' ', strip=True)
            
        # Clean and normalize text
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[^\w\s-]', ' ', text)
        
        return title.strip(), text.strip()
    
    def process_text(self, text):
        """Process text with NLP techniques"""
        try:
            # Tokenize into sentences and words
            sentences = sent_tokenize(text.lower())
            
            # Process each sentence
            processed_words = []
            for sentence in sentences:
                words = word_tokenize(sentence)
                # Filter and lemmatize words
                words = [self.lemmatizer.lemmatize(word) for word in words 
                        if word.isalnum() and word not in self.stop_words]
                processed_words.extend(words)
                
            return processed_words
        except Exception as e:
            # Fallback to basic word splitting if NLTK fails
            logging.warning(f"NLTK processing failed, using basic tokenization: {str(e)}")
            words = text.lower().split()
            return [word.strip('.,!?()[]{}:;"\'') for word in words 
                    if word.strip('.,!?()[]{}:;"\'').isalnum() 
                    and len(word) > 3 
                    and word not in self.stop_words]
    
    def extract_keywords(self, processed_words, top_n=10):
        """Extract key terms based on frequency"""
        from collections import Counter
        word_freq = Counter(processed_words)
        return [word for word, _ in word_freq.most_common(top_n)]
    
    def generate_summary(self, title, text, max_sentences=3):
        """Generate a brief summary using key sentences"""
        sentences = sent_tokenize(text)
        if not sentences:
            return ""
            
        # Simple extractive summarization
        if len(sentences) <= max_sentences:
            return " ".join(sentences)
            
        # Use first sentence (usually most important) and last few based on max_sentences
        summary = [sentences[0]]
        if max_sentences > 1:
            summary.extend(sentences[-(max_sentences-1):])
        return " ".join(summary)
    
    def index_document(self, url, content):
        """Index a document with enhanced processing and cloud storage"""
        try:
            title, extracted_text = self.extract_text_from_html(content)
            processed_words = self.process_text(extracted_text)
            keywords = self.extract_keywords(processed_words)
            summary = self.generate_summary(title, extracted_text)
            
            # Create metadata for storage
            metadata = {
                "title": title,
                "keywords": keywords,
                "summary": summary,
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
                "word_count": len(processed_words),
                "extracted_text_length": len(extracted_text)
            }
            
            # Store the raw HTML in cloud storage if available
            if self.cloud_storage:
                raw_html_result = self.cloud_storage.store_raw_html(url, content)
                logging.info(f"Stored raw HTML for {url} with result: {raw_html_result['storage_type']}")
                
                # Store processed text and metadata
                processed_result = self.cloud_storage.store_processed_text(url, extracted_text, metadata)
                logging.info(f"Stored processed text for {url} with result: {processed_result['storage_type']}")
            
            # Add document to index
            writer = self.ix.writer()
            writer.update_document(
                url=url,
                title=title,
                content=extracted_text,
                keywords=", ".join(keywords),
                summary=summary,
                last_updated=time.strftime("%Y-%m-%d %H:%M:%S")
            )
            writer.commit()
            
            return {
                "status": "success",
                "keywords": keywords,
                "summary_length": len(summary.split()),
                "cloud_storage": bool(self.cloud_storage)
            }
            
        except Exception as e:
            logging.error(f"Error indexing document {url}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def search(self, query_string, search_fields=None, page=1, pagelen=10):
        """
        Enhanced search functionality supporting:
        - Multi-field search
        - Boolean operators (AND, OR, NOT)
        - Phrase search (using quotes)
        - Field-specific search (field:term)
        """
        try:
            search_fields = search_fields or ["title", "content", "keywords"]
            
            with self.ix.searcher(weighting=scoring.BM25F) as searcher:
                parser = MultifieldParser(search_fields, self.ix.schema)
                query = parser.parse(query_string)
                
                results = searcher.search_page(query, page, pagelen=pagelen)
                
                return {
                    "total_results": len(results),
                    "current_page": page,
                    "results": [
                        {
                            "url": result["url"],
                            "title": result["title"],
                            "summary": result["summary"],
                            "keywords": result["keywords"],
                            "score": result.score,
                            "last_updated": result["last_updated"]
                        }
                        for result in results
                    ]
                }
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }

def indexer_process(bucket=None):
    """
    Enhanced Indexer Node Process with cloud storage:
    - Uses Whoosh for robust indexing
    - Implements advanced text processing
    - Provides enhanced search capabilities
    - Stores crawled content in cloud storage for data durability
    - Communicates with other nodes via SQS queues instead of MPI
    """
    # Initialize default bucket name if not provided
    bucket = bucket or 'web-crawler-data-storage'
    
    # Generate a unique node ID for this indexer instance
    node_id = f"indexer-{socket.gethostname()}-{os.getpid()}"
    
    # Ensure logs directory exists
    log_file = os.path.join("logs", "indexer.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Indexer - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Enhanced Indexer node started with ID: {node_id}")
    
    # Initialize the cloud storage
    try:
        cloud_storage = CloudStorage(bucket_name=bucket)
        logging.info(f"CloudStorage initialized for data persistence with bucket: {bucket}")
    except Exception as e:
        logging.error(f"Failed to initialize CloudStorage: {e}. Will continue without cloud storage.")
        cloud_storage = None
    
    # Initialize the cloud queue for communication
    # This queue will be used to receive content for indexing and send status updates
    task_queue = CloudQueue(queue_name='crawler-url-queue', status_queue_name='crawler-status-queue')
    logging.info(f"Connected to task queue for indexing content")
    
    # Initialize the enhanced indexer with cloud storage
    indexer = EnhancedIndexer(cloud_storage=cloud_storage)
    processed_urls = set()
    shutdown_flag = False
    
    # Set up a heartbeat thread for the indexer
    def send_heartbeat():
        """Send periodic heartbeats to the master node via the status queue"""
        while not shutdown_flag:
            try:
                heartbeat_msg = {
                    "type": MSG_HEARTBEAT,
                    "node_id": node_id,
                    "urls_indexed": len(processed_urls),
                    "timestamp": time.time()
                }
                task_queue.send_status_update(node_id, heartbeat_msg)
                logging.debug(f"Sent heartbeat to master node")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {e}")
            
            # Sleep for a random time between 5 and 10 seconds
            time.sleep(5)
    
    # Start heartbeat thread
    import threading
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    logging.info(f"Heartbeat mechanism started")
    
    # Main processing loop
    try:
        while not shutdown_flag:
            # Poll for messages from the queue
            messages = task_queue.receive_messages(max_messages=5, wait_time=5)
            
            if not messages:
                logging.debug("No messages in queue, continuing...")
                continue
            
            for message in messages:
                try:
                    # Parse the message
                    message_body = message.get('Body', '')
                    try:
                        message_data = json.loads(message_body)
                    except (json.JSONDecodeError, TypeError):
                        logging.warning(f"Received invalid message format: {message_body[:100]}...")
                        task_queue.delete_message(message)
                        continue
                    
                    # Check if this is a termination message
                    if message_data.get('type') == MSG_TERMINATE:
                        if message_data.get('target_node') == node_id or message_data.get('target_node') == 'all':
                            logging.info(f"Received shutdown signal. Exiting.")
                            shutdown_flag = True
                            task_queue.delete_message(message)
                            break
                    
                    # Process indexing requests
                    if message_data.get('type') == MSG_INDEX_CONTENT:
                        url = message_data.get('url')
                        content = message_data.get('content')
                        
                        # Check if content was truncated
                        if message_data.get('truncated'):
                            original_size = message_data.get('original_size', 0)
                            current_size = len(content) if content else 0
                            logging.warning(f"Processing truncated content for {url}. Original: {original_size} bytes, Received: {current_size} bytes")
                        
                        if url and content and url not in processed_urls:
                            try:
                                logging.info(f"Indexing content from {url}")
                                result = indexer.index_document(url, content)
                                
                                if result["status"] == "success":
                                    processed_urls.add(url)
                                    logging.info(f"Successfully indexed {url}")
                                    logging.info(f"Extracted {len(result['keywords'])} keywords")
                                    
                                    # Add cloud storage info to the success message
                                    storage_info = "with cloud storage" if result.get("cloud_storage") else "without cloud storage"
                                    success_msg = {
                                        "type": MSG_HEARTBEAT,
                                        "node_id": node_id,
                                        "url": url,
                                        "status": "indexed",
                                        "keywords_count": len(result['keywords']),
                                        "cloud_storage": result.get("cloud_storage", False),
                                        "timestamp": time.time()
                                    }
                                    
                                    # Send success status to master
                                    task_queue.send_status_update(node_id, success_msg)
                                else:
                                    error_msg = {
                                        "type": MSG_ERROR,
                                        "node_id": node_id,
                                        "url": url,
                                        "error": f"Failed to index: {result.get('error', 'Unknown error')}",
                                        "timestamp": time.time()
                                    }
                                    task_queue.send_status_update(node_id, error_msg)
                                    logging.error(f"Failed to index {url}: {result.get('error')}")
                            
                            except Exception as e:
                                error_msg = {
                                    "type": MSG_ERROR,
                                    "node_id": node_id,
                                    "url": url,
                                    "error": f"Error indexing: {str(e)}",
                                    "timestamp": time.time()
                                }
                                task_queue.send_status_update(node_id, error_msg)
                                logging.error(f"Error while indexing {url}: {e}")
                                
                        elif url in processed_urls:
                            logging.info(f"URL already indexed: {url}")
                    
                    # Delete the message after processing
                    task_queue.delete_message(message)
                    
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
                    # Still delete the message to prevent endless retries
                    task_queue.delete_message(message)
            
            # Break out of the loop if shutdown signal received
            if shutdown_flag:
                break
    
    except Exception as e:
        logging.error(f"Unexpected error in indexer: {e}")
        import traceback
        logging.error(traceback.format_exc())
    
    finally:
        # Clean up and generate final statistics
        shutdown_flag = True
        heartbeat_thread.join(timeout=1.0)
        
        with indexer.ix.searcher() as searcher:
            doc_count = searcher.doc_count()
            
        logging.info(f"Final Index Statistics:")
        logging.info(f"Total documents indexed: {doc_count}")
        logging.info(f"Total unique URLs processed: {len(processed_urls)}")
        
        # Example searches to demonstrate functionality
        example_queries = [
            "python AND programming",
            "title:github",
            '"open source"',
            "content:machine learning"
        ]
        
        logging.info("\nExample Search Results:")
        for query in example_queries:
            results = indexer.search(query)
            logging.info(f"\nQuery: {query}")
            logging.info(f"Found {results['total_results']} results")
        
        logging.info("Indexer shutting down")

if __name__ == "__main__":
    indexer_process()

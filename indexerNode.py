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
from db_manager import DatabaseManager
from datetime import datetime

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
    Phase 3 Indexer Node:
      - Receives content from crawler nodes via cloud storage.
      - Indexes the content and stores it in a searchable format.
      - Implements fault tolerance through heartbeat monitoring.
      - Records detailed system metrics for monitoring dashboard.
      - When work is complete, sends shutdown signal to master node.
    """
    # Initialize default bucket name if not provided
    bucket = bucket or 'web-crawler-data-storage'

    # Enhanced logging for Phase 3
    log_file = os.path.join("logs", "indexer.log")
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Indexer - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Indexer node started")

    # Initialize the cloud queue, storage and database
    status_queue = CloudQueue(status_queue_name='crawler-status-queue')
    cloud_storage = CloudStorage(bucket_name=bucket)
    db_manager = DatabaseManager()
    logging.info(f"Status queue initialized in {'cloud' if status_queue.is_cloud_mode() else 'local'} mode")
    logging.info(f"Cloud storage initialized with bucket: {bucket}")

    # Create index directory if it doesn't exist
    index_dir = os.path.join("data", "index")
    os.makedirs(index_dir, exist_ok=True)

    # Monitoring metrics - for tracking system performance
    system_metrics = {
        "start_time": datetime.now().isoformat(),
        "urls_indexed": 0,
        "indexing_errors": 0,
        "last_heartbeat": datetime.now().isoformat()
    }

    # Function to update monitoring data
    def update_monitoring_data():
        # Ensure data/monitoring directory exists
        monitoring_data_path = os.path.join("data", "monitoring", "indexer_metrics.json")
        os.makedirs(os.path.dirname(monitoring_data_path), exist_ok=True)
        with open(monitoring_data_path, "w") as f:
            json.dump(system_metrics, f, indent=4)

    # Initialize monitoring data file
    update_monitoring_data()

    # Main processing loop
    while True:
        try:
            # Send heartbeat
            status_queue.send_status_message({
                'type': MSG_HEARTBEAT,
                'node_id': 'indexer',
                'timestamp': datetime.now().isoformat()
            })
            system_metrics['last_heartbeat'] = datetime.now().isoformat()
            update_monitoring_data()

            # Get unindexed URLs from database
            unindexed_urls = db_manager.get_unindexed_urls(limit=10)
            
            for url in unindexed_urls:
                try:
                    # Get content from cloud storage
                    content = cloud_storage.get_content(url)
                    if not content:
                        logging.warning(f"No content found for URL: {url}")
                        continue

                    # Index the content
                    index_file = os.path.join(index_dir, f"{hash(url)}.json")
                    with open(index_file, 'w') as f:
                        json.dump({
                            'url': url,
                            'content': content,
                            'indexed_at': datetime.now().isoformat()
                        }, f)

                    # Mark URL as indexed in database
                    db_manager.mark_url_as_indexed(url)
                    
                    # Update metrics
                    system_metrics['urls_indexed'] += 1
                    update_monitoring_data()
                    
                    logging.info(f"Indexed URL: {url}")
                    
                except Exception as e:
                    logging.error(f"Error indexing URL {url}: {e}")
                    system_metrics['indexing_errors'] += 1
                    update_monitoring_data()

            # Sleep briefly to avoid excessive CPU usage
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"Error in main processing loop: {e}")
            time.sleep(5)  # Sleep longer on error

    logging.info("Indexer node shutdown complete")

if __name__ == "__main__":
    indexer_process()

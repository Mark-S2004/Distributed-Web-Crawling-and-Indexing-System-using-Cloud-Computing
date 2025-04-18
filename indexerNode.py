from mpi4py import MPI
import time
import logging
from bs4 import BeautifulSoup
import re
import os
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh import scoring
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import json
# Import CloudStorage for data persistence
from cloud_storage import CloudStorage

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

def indexer_process():
    """
    Enhanced Indexer Node Process with cloud storage:
    - Uses Whoosh for robust indexing
    - Implements advanced text processing
    - Provides enhanced search capabilities
    - Stores crawled content in cloud storage for data durability
    """
    # Setup MPI and logging
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
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
    
    logging.info(f"Enhanced Indexer node started with rank {rank} of {size}")
    
    # Initialize the cloud storage
    try:
        cloud_storage = CloudStorage()
        logging.info(f"CloudStorage initialized for data persistence")
    except Exception as e:
        logging.error(f"Failed to initialize CloudStorage: {e}. Will continue without cloud storage.")
        cloud_storage = None
    
    # Initialize the enhanced indexer with cloud storage
    indexer = EnhancedIndexer(cloud_storage=cloud_storage)
    processed_urls = set()
    
    while True:
        status = MPI.Status()
        message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        
        # Handle shutdown signal
        if tag == 0 and message is None:
            logging.info("Indexer received shutdown signal. Generating final statistics...")
            # Generate final statistics
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
                
            break
            
        if tag == 2:
            if isinstance(message, dict) and "url" in message and "content" in message:
                url = message["url"]
                content = message["content"]
                sender = status.Get_source()
                
                if url not in processed_urls:
                    try:
                        result = indexer.index_document(url, content)
                        if result["status"] == "success":
                            processed_urls.add(url)
                            logging.info(f"Successfully indexed {url}")
                            logging.info(f"Extracted {len(result['keywords'])} keywords")
                            
                            # Add cloud storage info to the success message
                            storage_info = "with cloud storage" if result.get("cloud_storage") else "without cloud storage"
                            success_msg = f"Indexed {url} with {len(result['keywords'])} keywords {storage_info}"
                            
                            # Send success status to master
                            comm.send(success_msg, dest=0, tag=99)
                        else:
                            logging.error(f"Failed to index {url}: {result['error']}")
                            comm.send(f"Failed to index {url}: {result['error']}", dest=0, tag=999)
                            
                    except Exception as e:
                        logging.error(f"Error while indexing {url}: {e}")
                        comm.send(f"Indexer error for {url}: {e}", dest=0, tag=999)
                else:
                    logging.info(f"URL already indexed: {url}")
                    
            else:
                logging.warning("Indexer received message with unknown format")
                
        else:
            logging.warning(f"Indexer received message with unexpected tag {tag}")
            
        time.sleep(0.1)

if __name__ == "__main__":
    indexer_process()

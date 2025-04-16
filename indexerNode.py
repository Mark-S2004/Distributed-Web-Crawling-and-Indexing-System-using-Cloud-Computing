from mpi4py import MPI
import time
import logging
from bs4 import BeautifulSoup
import re

def indexer_process():
    """
    Indexer Node Process:
      - Receives content messages from crawler nodes
      - Processes content using simple text extraction
      - Builds an in-memory inverted index
      - Provides detailed logging of indexing progress
    """
    # Setup MPI and logging
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.basicConfig(level=logging.INFO,
                      format='%(asctime)s - Indexer - %(levelname)s - %(message)s')
    logging.info(f"Indexer node started with rank {rank} of {size}")

    # Initialize data structures
    index = {}  # word -> set of URLs
    processed_urls = set()
    
    # Common English stop words to filter out
    STOP_WORDS = {
        'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have',
        'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do',
        'at', 'this', 'but', 'his', 'by', 'from', 'they', 'we',
        'say', 'her', 'she', 'or', 'an', 'will', 'my', 'one', 'all',
        'would', 'there', 'their', 'what', 'so', 'up', 'out', 'if',
        'about', 'who', 'get', 'which'
    }

    def extract_text(html_content):
        """Extract and clean text from HTML content."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            # Remove script and style elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer']):
                element.decompose()
            
            # Get text and clean it
            text = soup.get_text()
            # Remove URLs
            text = re.sub(r'http[s]?://\S+', '', text)
            # Remove special characters and extra whitespace
            text = re.sub(r'[^\w\s]', ' ', text)
            return ' '.join(text.split())
        except Exception as e:
            logging.error(f"Error extracting text: {e}")
            return ""

    def update_index(url, content):
        """Process content and update the index with logging."""
        if url in processed_urls:
            logging.info(f"Skipping already processed URL: {url}")
            return 0

        try:
            # Extract text from HTML
            text = extract_text(content)
            words = text.lower().split()
            new_words = 0
            word_count = 0

            # Update index
            for word in words:
                if len(word) > 3 and word not in STOP_WORDS:
                    word_count += 1
                    if word not in index:
                        index[word] = set()
                        new_words += 1
                    index[word].add(url)

            processed_urls.add(url)
            logging.info(f"Indexed {url}: Found {word_count} total words, {new_words} new unique words")
            return new_words

        except Exception as e:
            logging.error(f"Error updating index for {url}: {e}")
            return 0

    def log_statistics():
        """Log current indexing statistics."""
        if not index:
            return

        total_words = len(index)
        total_urls = len(processed_urls)
        
        # Get most common words
        word_freq = [(word, len(urls)) for word, urls in index.items()]
        word_freq.sort(key=lambda x: x[1], reverse=True)
        
        logging.info(f"\nIndexing Statistics:")
        logging.info(f"Total unique words: {total_words}")
        logging.info(f"Total URLs processed: {total_urls}")
        logging.info("\nTop 10 most common words:")
        for word, freq in word_freq[:10]:
            logging.info(f"  {word}: appears in {freq} URLs")

    while True:
        # Block until a message arrives
        status = MPI.Status()
        message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # Check for shutdown signal
        if tag == 0 and message is None:
            logging.info("\nFinal Index Statistics:")
            log_statistics()
            logging.info("Indexer shutting down.")
            break

        if tag == 2:  # Content message
            if isinstance(message, dict) and "url" in message and "content" in message:
                url = message["url"]
                content = message["content"]
                sender = status.Get_source()
                
                try:
                    new_words = update_index(url, content)
                    # Log statistics every 5 URLs
                    if len(processed_urls) % 5 == 0:
                        log_statistics()
                    comm.send(f"Indexed {url}", dest=0, tag=99)
                except Exception as e:
                    logging.error(f"Error while indexing content from {url}: {e}")
                    comm.send(f"Error indexing {url}: {e}", dest=0, tag=999)
            else:
                logging.warning("Received message with invalid format")
        else:
            logging.warning(f"Received message with unexpected tag {tag}")

        time.sleep(0.1)  # Small delay to prevent busy loop

if __name__ == "__main__":
    indexer_process()

from mpi4py import MPI
import time
import logging
from bs4 import BeautifulSoup
import re
from collections import Counter
from nltk.corpus import stopwords
import nltk

def indexer_process():
    """
    Indexer Node Process:
      - Receives content messages from crawler nodes.
      - Processes content by performing text extraction and tokenization.
      - Builds an in-memory inverted index mapping keywords to URLs.
      - Provides statistics about the indexing process.
      - Exits when shutdown signal is received.
    """
    # Download required NLTK data
    try:
        nltk.download('stopwords', quiet=True)
        nltk.download('punkt', quiet=True)
    except:
        pass  # Continue even if download fails

    # Setup MPI and logging
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.basicConfig(level=logging.INFO,
                      format='%(asctime)s - Indexer - %(levelname)s - %(message)s')
    logging.info(f"Indexer node started with rank {rank} of {size}")

    # Initialize data structures
    index = {}  # word -> set of URLs
    url_word_counts = {}  # url -> Counter of words
    processed_urls = set()
    stop_words = set(stopwords.words('english'))
    
    def clean_text(text):
        """Extract and clean text from HTML content."""
        # Remove script and style elements
        soup = BeautifulSoup(text, 'html.parser')
        for script in soup(['script', 'style', 'nav', 'header', 'footer']):
            script.decompose()
        
        # Get text and normalize
        text = soup.get_text()
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        # Remove special characters and digits
        text = re.sub(r'[^a-zA-Z\s]', ' ', text)
        # Convert to lowercase and split
        words = text.lower().split()
        # Filter words
        words = [w for w in words if len(w) > 3 and w not in stop_words]
        return words

    def update_index(url, content):
        """Process content and update the index."""
        if url in processed_urls:
            return 0
        
        words = clean_text(content)
        word_counter = Counter(words)
        url_word_counts[url] = word_counter
        
        # Update inverted index
        for word in word_counter:
            if word not in index:
                index[word] = set()
            index[word].add(url)
        
        processed_urls.add(url)
        return len(word_counter)

    def log_statistics(final=False):
        """Log current indexing statistics."""
        if not index:
            return
            
        total_words = len(index)
        total_urls = len(processed_urls)
        
        # Get words that appear in most URLs
        word_url_counts = {word: len(urls) for word, urls in index.items()}
        most_common = sorted(word_url_counts.items(), key=lambda x: x[1], reverse=True)
        
        if final:
            logging.info("Indexer received shutdown signal. Final index statistics:")
        logging.info(f"Total unique words: {total_words}")
        logging.info(f"Total URLs processed: {total_urls}")
        
        num_words = 20 if final else 10
        logging.info(f"Top {num_words} most common words in the final index:")
        for word, count in most_common[:num_words]:
            logging.info(f"  {word}: appears in {count} URLs")

    while True:
        # Block until a message arrives
        status = MPI.Status()
        message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # Check for shutdown signal
        if tag == 0 and message is None:
            log_statistics(final=True)
            logging.info("Indexer received shutdown signal. Exiting.")
            break

        if tag == 2:
            if isinstance(message, dict) and "url" in message and "content" in message:
                url = message["url"]
                content = message["content"]
                sender = status.Get_source()
                
                try:
                    new_words = update_index(url, content)
                    logging.info(f"Indexed {url}: Added {new_words} new words to the index")
                    logging.info(f"Successfully indexed content from URL: {url}")
                    
                    # Log statistics every 5 URLs
                    if len(processed_urls) % 5 == 0:
                        log_statistics()
                    
                    # Send acknowledgement
                    comm.send(f"Indexed content from {url}", dest=0, tag=99)
                except Exception as e:
                    logging.error(f"Error while indexing content from {url}: {e}")
                    comm.send(f"Indexer error for URL {url}: {e}", dest=0, tag=999)
            else:
                logging.warning("Indexer received message with unknown format.")
        else:
            logging.warning(f"Indexer received message with unexpected tag {tag}.")
        
        time.sleep(0.1)  # Small delay to prevent busy loop

if __name__ == "__main__":
    indexer_process()

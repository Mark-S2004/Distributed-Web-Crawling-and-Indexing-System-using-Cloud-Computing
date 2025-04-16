from mpi4py import MPI
import time
import logging
from bs4 import BeautifulSoup
import re

# Common English stop words to filter out
STOP_WORDS = {
    'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i',
    'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at',
    'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her',
    'she', 'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there',
    'their', 'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get',
    'which', 'go', 'me', 'when', 'make', 'can', 'like', 'time', 'no',
    'just', 'him', 'know', 'take', 'people', 'into', 'year', 'your',
    'some', 'could', 'them', 'see', 'other', 'than', 'then', 'now',
    'look', 'only', 'come', 'its', 'over', 'think', 'also', 'back',
    'after', 'use', 'two', 'how', 'our', 'work', 'first', 'well',
    'way', 'even', 'new', 'want', 'because', 'any', 'these', 'give',
    'day', 'most', 'us', 'more', 'been', 'much', 'was', 'were', 'are',
    'had', 'has', 'may', 'such', 'many', 'must', 'those'
}

def indexer_process():
    """
    Phase 2 Indexer Node:
      - Receives content messages (with tag 2) from crawler nodes.
      - Processes each content message by performing a very basic text tokenization.
      - Builds an in-memory inverted index mapping keywords to URLs.
      - Sends acknowledgements back to the master node.
    """
    # Setup MPI and logging.
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - Indexer - %(levelname)s - %(message)s')
    logging.info(f"Indexer node started with rank {rank} of {size}")

    # In-memory inverted index: key => set of URLs containing the word.
    index = {}
    processed_urls = set()

    def extract_text_from_html(html_content):
        """Extract meaningful text from HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script, style, and nav elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()
        
        # Get text and clean it
        text = soup.get_text()
        # Remove extra whitespace and normalize
        text = ' '.join(text.split())
        return text

    def is_meaningful_word(word):
        """Check if a word is meaningful enough to index."""
        # Skip very short words, numbers only, and common HTML artifacts
        if len(word) < 4:
            return False
        if word.isnumeric():
            return False
        if re.match(r'^[<>/"\'={}()\[\]]+$', word):
            return False
        if word.startswith(('http', 'https', 'www')):
            return False
        if word in STOP_WORDS:
            return False
        return True

    def update_index(url, content):
        """
        Tokenizes the content and updates the global index.
        """
        if url in processed_urls:
            return
        
        # Extract meaningful text from HTML
        text = extract_text_from_html(content)
        words = text.split()
        new_words = 0
        
        for word in words:
            # Basic normalization: lowercase and remove punctuation
            token = word.lower().strip(",.!?\"'()[]{}:;")
            if token and is_meaningful_word(token):
                if token not in index:
                    index[token] = set()
                    new_words += 1
                index[token].add(url)
        
        processed_urls.add(url)
        if new_words > 0:
            logging.info(f"Indexed {url}: Added {new_words} new words to the index")
            # Periodically show some statistics
            if len(processed_urls) % 5 == 0:
                logging.info(f"Index statistics: {len(index)} unique words, {len(processed_urls)} URLs processed")
                # Show top 10 most common words and their frequency
                word_freq = [(word, len(urls)) for word, urls in index.items()]
                word_freq.sort(key=lambda x: x[1], reverse=True)
                logging.info("Top 10 most common words:")
                for word, freq in word_freq[:10]:
                    logging.info(f"  {word}: appears in {freq} URLs")

    while True:
        # Block until a message arrives from any source with any tag.
        status = MPI.Status()
        message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # If we received a shutdown signal, exit the loop.
        if tag == 0 and message is None:
            logging.info("Indexer received shutdown signal. Final index statistics:")
            logging.info(f"Total unique words: {len(index)}")
            logging.info(f"Total URLs processed: {len(processed_urls)}")
            # Show final top 20 words
            word_freq = [(word, len(urls)) for word, urls in index.items()]
            word_freq.sort(key=lambda x: x[1], reverse=True)
            logging.info("Top 20 most common words in the final index:")
            for word, freq in word_freq[:20]:
                logging.info(f"  {word}: appears in {freq} URLs")
            break

        if tag == 2:
            # Expecting a dictionary message with keys "url" and "content".
            if isinstance(message, dict) and "url" in message and "content" in message:
                url = message["url"]
                content = message["content"]
                sender = status.Get_source()
                try:
                    update_index(url, content)
                    logging.info(f"Successfully indexed content from URL: {url}")
                    # Send acknowledgement/status back to master.
                    comm.send(f"Indexed content from {url}", dest=0, tag=99)
                except Exception as e:
                    logging.error(f"Error while indexing content from {url}: {e}")
                    comm.send(f"Indexer error for URL {url}: {e}", dest=0, tag=999)
            else:
                logging.warning("Indexer received message with unknown format.")
        else:
            logging.warning(f"Indexer received message with unexpected tag {tag}.")
        time.sleep(0.1)  # Small delay to avoid busy loop

if __name__ == "__main__":
    indexer_process()

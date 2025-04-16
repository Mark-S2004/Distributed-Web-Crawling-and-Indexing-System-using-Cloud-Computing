from mpi4py import MPI
import time
import logging

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

    def update_index(url, content):
        """
        Tokenizes the content and updates the global index.
        For simplicity, tokenization here just splits on whitespace.
        """
        words = content.split()
        for word in words:
            # A very basic normalization: lowercase and remove punctuation.
            token = word.lower().strip(",.!?")
            if token:
                if token not in index:
                    index[token] = set()
                index[token].add(url)

    while True:
        # Block until a message arrives from any source with any tag.
        status = MPI.Status()
        message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        # If we received a shutdown signal, exit the loop.
        if tag == 0 and message is None:
            logging.info("Indexer received shutdown signal. Exiting.")
            break

        if tag == 2:
            # Expecting a dictionary message with keys "url" and "content".
            if isinstance(message, dict) and "url" in message and "content" in message:
                url = message["url"]
                content = message["content"]
                sender = status.Get_source()
                logging.info(f"Received content from crawler {sender} for URL: {url}")
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

    # For debugging or evaluation, log the final built index.
    logging.info("Final inverted index content:")
    for word, urls in index.items():
        logging.info(f"{word}: {list(urls)}")

if __name__ == "__main__":
    indexer_process()

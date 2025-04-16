from mpi4py import MPI
import time
import logging
from urllib.parse import urlparse
from collections import deque

def master_process():
    """
    Master Node Process:
      - Distributes crawling tasks (URLs) to crawler nodes.
      - Manages the URL queue and tracks crawled URLs.
      - Ensures even distribution of work among crawlers.
      - Monitors system progress and handles errors.
      - Coordinates shutdown when work is complete.
    """
    # Setup MPI and logging
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.basicConfig(level=logging.INFO,
                      format='%(asctime)s - Master - %(levelname)s - %(message)s')
    logging.info(f"Master node started with rank {rank} of {size}")

    # Validate node configuration
    if size < 3:
        logging.error("Not enough nodes: need at least 1 master, 1 crawler, and 1 indexer.")
        return

    # Node configuration
    indexer_rank = size - 1
    crawler_ranks = list(range(1, size - 1))
    num_crawlers = len(crawler_ranks)
    
    logging.info(f"System configuration:")
    logging.info(f"- Number of crawler nodes: {num_crawlers}")
    logging.info(f"- Crawler ranks: {crawler_ranks}")
    logging.info(f"- Indexer rank: {indexer_rank}")

    # Seed URLs (real websites)
    seed_urls = [
        "https://www.python.org",
        "https://github.com",
        "https://www.wikipedia.org",
        "https://stackoverflow.com",
        "https://www.reddit.com"
    ]

    # System state
    url_queue = deque(seed_urls)
    crawled_urls = set()
    processing_urls = {}  # crawler_rank -> url
    max_urls_to_process = 20
    
    def is_valid_url(url):
        """Check if URL should be added to queue."""
        try:
            parsed = urlparse(url)
            return bool(parsed.netloc and parsed.scheme in {'http', 'https'})
        except:
            return False

    def add_urls_to_queue(urls):
        """Add new URLs to queue, avoiding duplicates."""
        added = 0
        for url in urls:
            if (url not in crawled_urls and 
                url not in url_queue and 
                url not in processing_urls.values() and 
                is_valid_url(url)):
                url_queue.append(url)
                added += 1
        return added

    # Initialize URL queue with seed URLs
    logging.info(f"Initializing with {len(seed_urls)} seed URLs")
    
    while url_queue or processing_urls:
        # Check for incoming messages from workers
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
            message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            sender = status.Get_source()
            tag = status.Get_tag()

            if tag == 1:  # New URLs discovered
                if sender in processing_urls:
                    current_url = processing_urls[sender]
                    crawled_urls.add(current_url)
                    del processing_urls[sender]
                    
                    # Add new URLs to queue
                    new_urls = message
                    added = add_urls_to_queue(new_urls)
                    logging.info(f"Received {len(new_urls)} URLs from crawler {sender}. "
                               f"Added {added} new URLs. Queue size now: {len(url_queue)}, "
                               f"Processed URLs: {len(crawled_urls)}")
            
            elif tag == 99:  # Status update
                logging.info(f"Status from node {sender}: {message}")
            
            elif tag == 999:  # Error reported
                logging.error(f"Error reported from node {sender}: {message}")
                if sender in processing_urls:
                    failed_url = processing_urls[sender]
                    crawled_urls.add(failed_url)  # Mark as processed to avoid retrying
                    del processing_urls[sender]

        # Assign new tasks to available crawlers
        for crawler in crawler_ranks:
            if (crawler not in processing_urls and 
                url_queue and 
                len(crawled_urls) < max_urls_to_process):
                
                url = url_queue.popleft()
                processing_urls[crawler] = url
                comm.send(url, dest=crawler, tag=0)
                logging.info(f"Assigned URL to crawler {crawler}: {url}")

        # Check if we're done
        if len(crawled_urls) >= max_urls_to_process:
            logging.info(f"Reached maximum URLs to process ({max_urls_to_process})")
            break

        time.sleep(0.1)  # Small delay to prevent busy waiting

    # Send shutdown signals
    logging.info("Crawling complete. Processed URLs: %d. Shutting down nodes...", len(crawled_urls))
    
    # Signal crawler nodes to exit
    for node in crawler_ranks:
        comm.send(None, dest=node, tag=0)
    
    # Signal indexer node to shut down
    comm.send(None, dest=indexer_rank, tag=0)
    
    logging.info("Master node finished URL distribution. Shutting down.")

if __name__ == "__main__":
    master_process()

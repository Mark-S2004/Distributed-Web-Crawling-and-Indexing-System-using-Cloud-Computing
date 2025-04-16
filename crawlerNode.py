from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup

def crawler_process():
    """
    Crawler Node Process (Phase 2):
      - Waits for URL tasks from the master node (tag 0).
      - For each received URL:
          1. Fetches the web page content (using requests).
          2. Parses the content using BeautifulSoup to extract additional URLs.
          3. Sends extracted URLs back to the master node (tag 1).
          4. Sends the fetched content to the indexer node (tag 2) for indexing.
          5. Sends a status update (heartbeat) back to the master node (tag 99).
      - Exits when a shutdown signal (None) is received.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - Crawler - %(levelname)s - %(message)s')
    logging.info(f"Crawler node started with rank {rank} of {size}")

    # Assume the last rank is dedicated to the indexer.
    indexer_rank = size - 1

    while True:
        status = MPI.Status()
        # Receive a URL assignment from the master.
        url_to_crawl = comm.recv(source=0, tag=0, status=status)

        # Check for shutdown signal: if url_to_crawl is None, exit the loop.
        if url_to_crawl is None:
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")

        try:
            # Fetch the webpage.
            response = requests.get(url_to_crawl, timeout=5)
            content = response.text

            # Parse the content to extract additional URLs.
            soup = BeautifulSoup(content, 'html.parser')
            extracted_urls = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                # Basic check: consider only absolute URLs.
                if href.startswith("http"):
                    extracted_urls.append(href)

            # If no URLs are extracted, simulate a couple of URLs.
            if not extracted_urls:
                extracted_urls = [f"http://example.com/page_{rank}_{i}" for i in range(2)]

            logging.info(f"Crawler {rank} crawled {url_to_crawl} and extracted {len(extracted_urls)} URLs.")

            # Send the list of newly discovered URLs to the master node (tag 1).
            comm.send(extracted_urls, dest=0, tag=1)

            # Prepare and send the fetched content for indexing:
            # Send a dictionary with the URL and content to the indexer node (tag 2).
            indexing_message = {"url": url_to_crawl, "content": content}
            comm.send(indexing_message, dest=indexer_rank, tag=2)

            # Send a status/heartbeat message to the master node (tag 99).
            comm.send(f"Crawler {rank} completed URL: {url_to_crawl}", dest=0, tag=99)

        except Exception as e:
            logging.error(f"Crawler {rank} error processing URL {url_to_crawl}: {e}")
            # Report the error to the master node (tag 999).
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)

        # Pause briefly to simulate a delay and help stagger requests.
        time.sleep(0.1)

if __name__ == "__main__":
    crawler_process() 
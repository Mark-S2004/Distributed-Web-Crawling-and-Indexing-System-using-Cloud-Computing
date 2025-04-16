from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def crawler_process():
    """
    Crawler Node Process:
      - Waits for URL tasks from the master node (tag 0).
      - For each received URL:
          1. Fetches the web page content (using requests).
          2. Parses the content using BeautifulSoup to extract additional URLs.
          3. Filters and normalizes extracted URLs.
          4. Sends extracted URLs back to the master node (tag 1).
          5. Sends the fetched content to the indexer node (tag 2) for indexing.
          6. Sends a status update back to the master node (tag 99).
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

    def normalize_url(base_url, url):
        """Normalize and filter URLs."""
        try:
            # Join relative URLs with base URL
            full_url = urljoin(base_url, url)
            parsed = urlparse(full_url)
            # Remove fragments
            return parsed._replace(fragment='').geturl()
        except:
            return None

    def filter_urls(base_url, urls):
        """Filter and normalize extracted URLs."""
        filtered_urls = set()
        base_domain = urlparse(base_url).netloc
        
        for url in urls:
            normalized = normalize_url(base_url, url)
            if normalized and len(filtered_urls) < 100:  # Limit URLs per page
                filtered_urls.add(normalized)
        
        return list(filtered_urls)

    while True:
        status = MPI.Status()
        # Receive a URL assignment from the master.
        url_to_crawl = comm.recv(source=0, tag=0, status=status)

        # Check for shutdown signal
        if url_to_crawl is None:
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")

        try:
            # Configure requests with timeout and headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url_to_crawl, timeout=5, headers=headers)
            content = response.text

            # Parse the content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract URLs
            raw_urls = [a.get('href') for a in soup.find_all('a', href=True)]
            extracted_urls = filter_urls(url_to_crawl, raw_urls)

            logging.info(f"Crawler {rank} crawled {url_to_crawl} and extracted {len(extracted_urls)} URLs.")

            # Send extracted URLs to master
            comm.send(extracted_urls, dest=0, tag=1)

            # Send content for indexing
            indexing_message = {
                "url": url_to_crawl,
                "content": content,
                "title": soup.title.string if soup.title else ""
            }
            comm.send(indexing_message, dest=indexer_rank, tag=2)

            # Send status update
            comm.send(f"Crawler {rank} completed URL: {url_to_crawl}", dest=0, tag=99)

        except Exception as e:
            logging.error(f"Crawler {rank} error processing URL {url_to_crawl}: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)

        time.sleep(0.1)  # Small delay to prevent overwhelming servers

if __name__ == "__main__":
    crawler_process() 
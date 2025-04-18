from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
import threading
import random
import os

def crawler_process():
    """
    Enhanced Crawler Node Process (Phase 3):
      - Implements regular heartbeat signals to master node
      - Provides detailed logging for monitoring
      - Handles tasks from the master node (tag 0).
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
    
    # Setup enhanced logging with file output
    log_filename = os.path.join("logs", f"crawler_{rank}.log")
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Crawler-%(process)d - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Crawler node started with rank {rank} of {size}")

    # Assume the last rank is dedicated to the indexer.
    indexer_rank = size - 1
    
    # Statistics for the crawler node
    stats = {
        "urls_processed": 0,
        "urls_extracted": 0,
        "errors": 0,
        "start_time": time.time()
    }
    
    # Heartbeat mechanism
    shutdown_flag = False
    
    def send_heartbeat():
        """Send periodic heartbeats to the master node"""
        while not shutdown_flag:
            try:
                uptime = time.time() - stats["urls_processed"]
                heartbeat_msg = {
                    "type": "heartbeat",
                    "rank": rank,
                    "uptime": uptime,
                    "urls_processed": stats["urls_processed"],
                    "errors": stats["errors"]
                }
                comm.send(f"Heartbeat from crawler {rank}: Active", dest=0, tag=99)
                logging.debug(f"Sent heartbeat to master node")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {e}")
            
            # Sleep for a random time between 2 and 5 seconds to avoid synchronized heartbeats
            time.sleep(random.uniform(2, 5))
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    logging.info(f"Heartbeat mechanism started")

    try:
        while True:
            status = MPI.Status()
            # Receive a URL assignment from the master.
            url_to_crawl = comm.recv(source=0, tag=0, status=status)

            # Check for shutdown signal: if url_to_crawl is None, exit the loop.
            if url_to_crawl is None:
                logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
                break

            logging.info(f"Crawler {rank} received URL: {url_to_crawl}")
            task_start_time = time.time()

            try:
                # Fetch the webpage.
                logging.info(f"Fetching content from {url_to_crawl}")
                response = requests.get(url_to_crawl, timeout=10)
                content = response.text
                content_size = len(content)
                logging.info(f"Fetched {content_size} bytes from {url_to_crawl}")

                # Parse the content to extract additional URLs.
                logging.info(f"Parsing content from {url_to_crawl}")
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
                    logging.info(f"No real URLs found, created {len(extracted_urls)} simulated URLs")
                
                # Update statistics
                stats["urls_processed"] += 1
                stats["urls_extracted"] += len(extracted_urls)

                # Detailed logging
                logging.info(f"Crawler {rank} crawled {url_to_crawl} and extracted {len(extracted_urls)} URLs in {time.time() - task_start_time:.2f} seconds")

                # Send the list of newly discovered URLs to the master node (tag 1).
                comm.send(extracted_urls, dest=0, tag=1)
                logging.debug(f"Sent {len(extracted_urls)} URLs to master node")

                # Prepare and send the fetched content for indexing:
                # Send a dictionary with the URL and content to the indexer node (tag 2).
                indexing_message = {"url": url_to_crawl, "content": content}
                comm.send(indexing_message, dest=indexer_rank, tag=2)
                logging.info(f"Sent content to indexer node for {url_to_crawl}")

                # Send a status/heartbeat message to the master node (tag 99).
                status_message = f"Crawler {rank} completed URL: {url_to_crawl} (found {len(extracted_urls)} URLs, {content_size} bytes)"
                comm.send(status_message, dest=0, tag=99)
                logging.debug(f"Sent completion status to master node")

                # Simulate occasional failures for testing fault tolerance
                if random.random() < 0.05:  # 5% chance of simulated failure
                    logging.warning(f"Simulating a brief node failure (testing fault tolerance)")
                    time.sleep(12)  # Sleep longer than heartbeat timeout to trigger failure detection
                
            except requests.exceptions.Timeout:
                stats["errors"] += 1
                error_msg = f"Timeout fetching {url_to_crawl}"
                logging.error(error_msg)
                comm.send(error_msg, dest=0, tag=999)
                
            except requests.exceptions.RequestException as e:
                stats["errors"] += 1
                error_msg = f"Error crawling {url_to_crawl}: {str(e)}"
                logging.error(error_msg)
                comm.send(error_msg, dest=0, tag=999)
                
            except Exception as e:
                stats["errors"] += 1
                error_msg = f"Unexpected error processing URL {url_to_crawl}: {str(e)}"
                logging.error(error_msg)
                comm.send(error_msg, dest=0, tag=999)

            # Log task completion time
            task_duration = time.time() - task_start_time
            logging.info(f"Task for {url_to_crawl} completed in {task_duration:.2f} seconds")
            
            # Pause briefly to simulate a delay and help stagger requests.
            time.sleep(0.1)
    
    except Exception as e:
        logging.critical(f"Critical error in crawler {rank}: {e}")
    finally:
        # Set shutdown flag for heartbeat thread
        shutdown_flag = True
        heartbeat_thread.join(timeout=1.0)
        
        # Log final statistics
        total_runtime = time.time() - stats["start_time"]
        logging.info(f"Crawler {rank} shutting down. Final statistics:")
        logging.info(f"  - Total runtime: {total_runtime:.2f} seconds")
        logging.info(f"  - URLs processed: {stats['urls_processed']}")
        logging.info(f"  - URLs extracted: {stats['urls_extracted']}")
        logging.info(f"  - Errors encountered: {stats['errors']}")
        logging.info(f"Crawler {rank} exiting")

if __name__ == "__main__":
    crawler_process() 
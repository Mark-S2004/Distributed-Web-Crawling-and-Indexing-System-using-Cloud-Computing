from mpi4py import MPI
import time
import logging

def master_process():
    """
    Phase 2 Master Node:
      - Distributes crawling tasks (URLs) to the crawler nodes.
      - Receives extracted URLs and status messages from crawler nodes.
      - When work is complete, sends shutdown signals to crawler and indexer nodes.
    """
    # Setup MPI and logging.
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - Master - %(levelname)s - %(message)s')
    logging.info(f"Master node started with rank {rank} of {size}")

    # Validate that we have at least 1 master, 1 crawler, and 1 indexer.
    if size < 3:
        logging.error("Not enough nodes: need at least 1 master, 1 crawler, and 1 indexer.")
        return

    # Here we assume that the last rank is dedicated to the indexer.
    indexer_rank = size - 1
    # All nodes between rank 1 and size-2 are crawler nodes.
    active_crawler_nodes = list(range(1, size - 1))
    num_crawlers = len(active_crawler_nodes)

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Indexer Node: {indexer_rank}")

    # Real seed URLs for testing
    seed_urls = [
        "https://www.python.org",
        "https://www.github.com",
        "https://www.wikipedia.org",
        "https://www.reddit.com",
        "https://www.stackoverflow.com"
    ]
    # We use a simple Python list to simulate a distributed URL queue.
    urls_to_crawl_queue = seed_urls.copy()

    task_id = 0
    # Keep track of which URLs are being processed by which crawlers
    crawler_assignments = {node: None for node in active_crawler_nodes}
    max_urls_to_process = 20  # Limit the total number of URLs to process

    processed_urls = 0
    next_crawler_index = 0  # For round-robin assignment

    while (urls_to_crawl_queue or any(assignment is not None for assignment in crawler_assignments.values())) and processed_urls < max_urls_to_process:
        # Check for incoming messages (non-blocking) from any worker node.
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
            message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            sender = status.Get_source()
            tag = status.Get_tag()

            if tag == 1:
                # Crawler completed a task and sent back extracted URLs.
                crawler_assignments[sender] = None  # Mark crawler as available
                processed_urls += 1
                # Add new URLs to the queue if we haven't reached the limit
                if processed_urls < max_urls_to_process:
                    new_urls = message
                    if new_urls:
                        urls_to_crawl_queue.extend(new_urls[:5])  # Limit to 5 new URLs per page
                logging.info(f"Received URLs from crawler {sender}. "
                             f"Queue size now: {len(urls_to_crawl_queue)}, "
                             f"Processed URLs: {processed_urls}")
            elif tag == 99:
                # Status update (heartbeat, completion messages, indexing confirmation, etc.)
                logging.info(f"Status from node {sender}: {message}")
            elif tag == 999:
                # Error message received.
                logging.error(f"Error reported from node {sender}: {message}")
                if sender in crawler_assignments:
                    crawler_assignments[sender] = None  # Mark crawler as available on error
                processed_urls += 1
            else:
                logging.warning(f"Master received a message with unexpected tag {tag} from {sender}.")

        # Assign new crawl tasks to available crawlers if there are URLs in the queue
        while urls_to_crawl_queue and any(assignment is None for assignment in crawler_assignments.values()):
            # Find next available crawler using round-robin
            while crawler_assignments[active_crawler_nodes[next_crawler_index]] is not None:
                next_crawler_index = (next_crawler_index + 1) % num_crawlers
            
            assigned_node = active_crawler_nodes[next_crawler_index]
            url = urls_to_crawl_queue.pop(0)
            comm.send(url, dest=assigned_node, tag=0)  # tag 0 for task assignment
            crawler_assignments[assigned_node] = url
            logging.info(f"Assigned task {task_id} to crawler {assigned_node} for URL: {url}")
            task_id += 1
            next_crawler_index = (next_crawler_index + 1) % num_crawlers

        time.sleep(0.1)  # Delay to prevent busy waiting

    # Once all crawling is done, send shutdown signals.
    logging.info(f"Crawling complete. Processed {processed_urls} URLs. Shutting down nodes...")
    # Signal to crawler nodes to exit (by sending "None" as the URL).
    for node in active_crawler_nodes:
        comm.send(None, dest=node, tag=0)
    # Signal indexer node to shut down (we use tag 0 here to indicate shutdown).
    comm.send(None, dest=indexer_rank, tag=0)

    logging.info("Master node finished URL distribution. Shutting down.")

if __name__ == "__main__":
    master_process()

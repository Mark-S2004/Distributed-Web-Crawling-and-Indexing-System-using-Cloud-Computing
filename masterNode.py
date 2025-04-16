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

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Indexer Node: {indexer_rank}")

    # Phase 2 seed URLs (could be replaced by user input).
    seed_urls = ["http://example.com", "http://example.org"]
    # We use a simple Python list to simulate a distributed URL queue.
    urls_to_crawl_queue = seed_urls.copy()

    task_id = 0
    crawler_tasks_assigned = 0
    tasks_in_progress = {}  # Map task_id to (crawler_rank, url) for potential future use.

    while urls_to_crawl_queue or tasks_in_progress:
        # Check for incoming messages (non-blocking) from any worker node.
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
            message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            sender = status.Get_source()
            tag = status.Get_tag()

            if tag == 1:
                # Crawler completed a task and sent back extracted URLs.
                crawler_tasks_assigned -= 1
                # (Optional) Remove task from tasks_in_progress if tracking.
                new_urls = message
                if new_urls:
                    urls_to_crawl_queue.extend(new_urls)
                logging.info(f"Received URLs from crawler {sender}. "
                             f"Queue size now: {len(urls_to_crawl_queue)}")
            elif tag == 99:
                # Status update (heartbeat, completion messages, indexing confirmation, etc.)
                logging.info(f"Status from node {sender}: {message}")
            elif tag == 999:
                # Error message received.
                logging.error(f"Error reported from node {sender}: {message}")
                crawler_tasks_assigned = max(crawler_tasks_assigned - 1, 0)
            else:
                logging.warning(f"Master received a message with unexpected tag {tag} from {sender}.")

        # Assign new crawl tasks if there are URLs in the queue and free crawler nodes.
        while urls_to_crawl_queue and crawler_tasks_assigned < len(active_crawler_nodes):
            url = urls_to_crawl_queue.pop(0)
            # Use a simple round-robin assignment.
            assigned_node = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)]
            comm.send(url, dest=assigned_node, tag=0)  # tag 0 for task assignment
            tasks_in_progress[task_id] = (assigned_node, url)
            crawler_tasks_assigned += 1
            logging.info(f"Assigned task {task_id} to crawler {assigned_node} for URL: {url}")
            task_id += 1

        time.sleep(0.5)  # Delay to prevent busy waiting

    # Once all crawling is done, send shutdown signals.
    # Signal to crawler nodes to exit (by sending "None" as the URL).
    for node in active_crawler_nodes:
        comm.send(None, dest=node, tag=0)
    # Signal indexer node to shut down (we use tag 0 here to indicate shutdown).
    comm.send(None, dest=indexer_rank, tag=0)

    logging.info("Master node finished URL distribution. Shutting down.")

if __name__ == "__main__":
    master_process()

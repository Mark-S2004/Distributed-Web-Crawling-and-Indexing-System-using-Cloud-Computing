from mpi4py import MPI
import time
import logging
import json
import os
from datetime import datetime, timedelta
from cloud_queue import CloudQueue

def master_process():
    """
    Phase 3 Master Node:
      - Distributes crawling tasks (URLs) to the crawler nodes via cloud queue.
      - Receives extracted URLs and status messages from crawler nodes.
      - Implements fault tolerance through heartbeat monitoring and task re-queueing.
      - Tracks crawler node health and re-assigns tasks from failed nodes.
      - Records detailed system metrics for monitoring dashboard.
      - When work is complete, sends shutdown signals to crawler and indexer nodes.
    """
    # Setup MPI and logging.
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    # Enhanced logging for Phase 3
    log_file = os.path.join("logs", "master.log")
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Master - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
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

    # Initialize the cloud queue
    task_queue = CloudQueue()
    logging.info(f"Task queue initialized in {'cloud' if task_queue.is_cloud_mode() else 'local'} mode")

    # Real seed URLs for testing
    seed_urls = [
        "https://www.python.org",
        "https://www.github.com",
        "https://www.wikipedia.org",
        "https://www.reddit.com",
        "https://www.stackoverflow.com"
    ]
    
    # Add seed URLs to the cloud queue
    for url in seed_urls:
        task_queue.send_message(url, {"type": "seed"})
    
    logging.info(f"Added {len(seed_urls)} seed URLs to the task queue")

    task_id = 0
    # Keep track of which URLs are being processed by which crawlers
    crawler_assignments = {node: None for node in active_crawler_nodes}
    max_urls_to_process = 20  # Limit the total number of URLs to process

    processed_urls = 0
    next_crawler_index = 0  # For round-robin assignment

    # Task timeout and heartbeat tracking - Phase 3 fault tolerance
    task_timeout_seconds = 30  # Maximum time allowed for a crawler to process a URL
    heartbeat_timeout_seconds = 10  # Maximum time without a heartbeat before considering a node failed
    heartbeat_timestamps = {node: datetime.now() for node in active_crawler_nodes}
    task_start_times = {node: None for node in active_crawler_nodes}
    
    # Monitoring metrics - for tracking system performance
    system_metrics = {
        "start_time": datetime.now().isoformat(),
        "urls_crawled": 0,
        "urls_indexed": 0,
        "urls_failed": 0,
        "crawler_status": {str(node): "active" for node in active_crawler_nodes},
        "error_count": 0,
        "task_assignments": [],
        "crawler_performance": {str(node): {"assigned": 0, "completed": 0, "failed": 0} for node in active_crawler_nodes}
    }

    # Function to update monitoring data
    def update_monitoring_data():
        # Ensure data/monitoring directory exists
        monitoring_data_path = os.path.join("data", "monitoring", "monitoring_data.json")
        os.makedirs(os.path.dirname(monitoring_data_path), exist_ok=True)
        with open(monitoring_data_path, "w") as f:
            json.dump(system_metrics, f, indent=4)

    # Initialize monitoring data file
    update_monitoring_data()

    # Main processing loop
    queue_size = task_queue.get_queue_size()
    while (queue_size > 0 or any(assignment is not None for assignment in crawler_assignments.values())) and processed_urls < max_urls_to_process:
        current_time = datetime.now()
        
        # Check for timeouts and failed nodes
        for node, assignment in list(crawler_assignments.items()):
            if assignment is not None:
                # Check for task timeout
                if task_start_times[node] and (current_time - task_start_times[node]).total_seconds() > task_timeout_seconds:
                    logging.warning(f"Task timeout for crawler {node} on URL: {assignment}")
                    # Re-queue the URL
                    task_queue.send_message(assignment, {"type": "retry", "failed_node": str(node)})
                    crawler_assignments[node] = None
                    task_start_times[node] = None
                    system_metrics["urls_failed"] += 1
                    system_metrics["crawler_performance"][str(node)]["failed"] += 1
                    system_metrics["error_count"] += 1
                    system_metrics["task_assignments"].append({
                        "time": datetime.now().isoformat(),
                        "task_id": task_id,
                        "url": assignment,
                        "crawler": node,
                        "status": "timeout"
                    })
                    update_monitoring_data()
                
                # Check for heartbeat timeout
                if (current_time - heartbeat_timestamps[node]).total_seconds() > heartbeat_timeout_seconds:
                    logging.warning(f"Heartbeat timeout for crawler {node}. Marking as failed.")
                    # Mark node as failed in metrics
                    system_metrics["crawler_status"][str(node)] = "failed"
                    # Re-queue any assigned URL
                    if assignment is not None:
                        task_queue.send_message(assignment, {"type": "retry", "failed_node": str(node)})
                        system_metrics["urls_failed"] += 1
                        system_metrics["crawler_performance"][str(node)]["failed"] += 1
                        system_metrics["task_assignments"].append({
                            "time": datetime.now().isoformat(),
                            "task_id": task_id,
                            "url": assignment,
                            "crawler": node,
                            "status": "node_failed"
                        })
                    crawler_assignments[node] = None
                    task_start_times[node] = None
                    update_monitoring_data()

        # Check for incoming messages (non-blocking) from any worker node.
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
            message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            sender = status.Get_source()
            tag = status.Get_tag()

            # Update heartbeat timestamp for the node
            if sender in heartbeat_timestamps:
                heartbeat_timestamps[sender] = datetime.now()
                # If node was previously marked as failed, mark it as active again
                if system_metrics["crawler_status"][str(sender)] == "failed":
                    system_metrics["crawler_status"][str(sender)] = "active"
                    logging.info(f"Crawler {sender} is back online.")

            if tag == 1:
                # Crawler completed a task and sent back extracted URLs.
                url_processed = crawler_assignments[sender]
                crawler_assignments[sender] = None  # Mark crawler as available
                task_start_times[sender] = None
                processed_urls += 1
                system_metrics["urls_crawled"] += 1
                system_metrics["crawler_performance"][str(sender)]["completed"] += 1
                
                # Record task completion
                system_metrics["task_assignments"].append({
                    "time": datetime.now().isoformat(),
                    "url": url_processed,
                    "crawler": sender,
                    "status": "completed",
                    "urls_extracted": len(message) if isinstance(message, list) else 0
                })
                
                # Add new URLs to the queue if we haven't reached the limit
                if processed_urls < max_urls_to_process:
                    new_urls = message
                    if new_urls:
                        # Add up to 5 new URLs to the queue
                        for url in new_urls[:5]:
                            task_queue.send_message(url, {"type": "discovered", "source_url": url_processed})
                
                logging.info(f"Received URLs from crawler {sender}. "
                             f"Queue size now: {task_queue.get_queue_size()}, "
                             f"Processed URLs: {processed_urls}")
                update_monitoring_data()
                
            elif tag == 99:
                # Status update (heartbeat, completion messages, indexing confirmation, etc.)
                if "Indexed" in str(message):
                    system_metrics["urls_indexed"] += 1
                logging.info(f"Status from node {sender}: {message}")
                update_monitoring_data()
                
            elif tag == 999:
                # Error message received.
                logging.error(f"Error reported from node {sender}: {message}")
                system_metrics["error_count"] += 1
                update_monitoring_data()

        # Assign tasks to available crawlers from the queue
        for node in active_crawler_nodes:
            if crawler_assignments[node] is None and system_metrics["crawler_status"][str(node)] == "active":
                # Retrieve a message from the queue
                messages = task_queue.receive_messages(max_messages=1, wait_time=0)
                if messages:
                    message = messages[0]
                    # Extract the URL from the message body
                    if hasattr(message, 'body'):
                        url_to_crawl = message.body
                    else:
                        url_to_crawl = message.get('MessageBody', '')
                    
                    # Delete the message from the queue
                    task_queue.delete_message(message)
                    
                    task_id += 1
                    # Assign URL to crawler
                    crawler_assignments[node] = url_to_crawl
                    task_start_times[node] = datetime.now()
                    
                    # Send the URL to the crawler for processing.
                    comm.send(url_to_crawl, dest=node, tag=0)
                    
                    # Update metrics
                    system_metrics["crawler_performance"][str(node)]["assigned"] += 1
                    system_metrics["task_assignments"].append({
                        "time": datetime.now().isoformat(),
                        "task_id": task_id,
                        "url": url_to_crawl,
                        "crawler": node,
                        "status": "assigned"
                    })
                    logging.info(f"Assigned URL {url_to_crawl} to crawler {node}.")
                    update_monitoring_data()

        # Update queue size for the next iteration
        queue_size = task_queue.get_queue_size()
        
        # Pause briefly to prevent CPU overutilization
        time.sleep(0.1)

    # Send shutdown signals to the crawlers.
    logging.info("Sending shutdown signals to crawler nodes...")
    for node in range(1, size - 1):
        comm.send(None, dest=node, tag=0)
        logging.info(f"Shutdown signal sent to crawler {node}")

    # Send shutdown signal to the indexer.
    logging.info("Sending shutdown signal to indexer node...")
    comm.send(None, dest=indexer_rank, tag=2)
    
    # Final metrics update
    system_metrics["end_time"] = datetime.now().isoformat()
    system_metrics["total_runtime_seconds"] = (datetime.now() - datetime.fromisoformat(system_metrics["start_time"])).total_seconds()
    update_monitoring_data()
    
    logging.info(f"Master node shutting down. Total URLs processed: {processed_urls}")
    logging.info(f"Crawled: {system_metrics['urls_crawled']}, Indexed: {system_metrics['urls_indexed']}, Failed: {system_metrics['urls_failed']}")

if __name__ == "__main__":
    master_process()

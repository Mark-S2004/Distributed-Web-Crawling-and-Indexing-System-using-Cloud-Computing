from mpi4py import MPI
import time
import logging
import json
import os
from datetime import datetime, timedelta

def master_process():
    """
    Phase 3 Master Node:
      - Distributes crawling tasks (URLs) to the crawler nodes.
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
    log_file = "logs/master.log"
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
        monitoring_data_path = "data/monitoring/monitoring_data.json"
        os.makedirs(os.path.dirname(monitoring_data_path), exist_ok=True)
        with open(monitoring_data_path, "w") as f:
            json.dump(system_metrics, f, indent=4)

    # Initialize monitoring data file
    update_monitoring_data()

    # Main processing loop
    while (urls_to_crawl_queue or any(assignment is not None for assignment in crawler_assignments.values())) and processed_urls < max_urls_to_process:
        current_time = datetime.now()
        
        # Check for timeouts and failed nodes
        for node, assignment in list(crawler_assignments.items()):
            if assignment is not None:
                # Check for task timeout
                if task_start_times[node] and (current_time - task_start_times[node]).total_seconds() > task_timeout_seconds:
                    logging.warning(f"Task timeout for crawler {node} on URL: {assignment}")
                    # Re-queue the URL
                    urls_to_crawl_queue.append(assignment)
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
                        urls_to_crawl_queue.append(assignment)
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
                        urls_to_crawl_queue.extend(new_urls[:5])  # Limit to 5 new URLs per page
                
                logging.info(f"Received URLs from crawler {sender}. "
                             f"Queue size now: {len(urls_to_crawl_queue)}, "
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
                
                if sender in crawler_assignments and crawler_assignments[sender] is not None:
                    url_failed = crawler_assignments[sender]
                    # Re-queue the failed URL
                    urls_to_crawl_queue.append(url_failed)
                    system_metrics["urls_failed"] += 1
                    system_metrics["crawler_performance"][str(sender)]["failed"] += 1
                    
                    # Record task failure
                    system_metrics["task_assignments"].append({
                        "time": datetime.now().isoformat(),
                        "url": url_failed,
                        "crawler": sender,
                        "status": "error",
                        "error_message": str(message)
                    })
                    
                    crawler_assignments[sender] = None  # Mark crawler as available
                    task_start_times[sender] = None
                    processed_urls += 1
                update_monitoring_data()
                
            else:
                logging.warning(f"Master received a message with unexpected tag {tag} from {sender}.")

        # Assign new crawl tasks to available crawlers if there are URLs in the queue
        while urls_to_crawl_queue and any(assignment is None for assignment in crawler_assignments.values()):
            # Find next available crawler using round-robin
            found_available_crawler = False
            tries = 0
            
            # Prevent infinite loop by limiting the number of attempts to the number of crawlers
            while not found_available_crawler and tries < num_crawlers:
                tries += 1
                
                # Check if crawler is available
                current_node = active_crawler_nodes[next_crawler_index]
                if crawler_assignments[current_node] is None and system_metrics["crawler_status"][str(current_node)] == "active":
                    found_available_crawler = True
                else:
                    # Move to next crawler
                    next_crawler_index = (next_crawler_index + 1) % num_crawlers
            
            # If no available crawler was found after checking all nodes, break the loop
            if not found_available_crawler:
                logging.warning("No available crawler nodes found. Waiting for nodes to become available.")
                break
                
            assigned_node = active_crawler_nodes[next_crawler_index]
            url = urls_to_crawl_queue.pop(0)
            comm.send(url, dest=assigned_node, tag=0)  # tag 0 for task assignment
            crawler_assignments[assigned_node] = url
            task_start_times[assigned_node] = datetime.now()
            
            # Update metrics
            system_metrics["crawler_performance"][str(assigned_node)]["assigned"] += 1
            system_metrics["task_assignments"].append({
                "time": datetime.now().isoformat(),
                "task_id": task_id,
                "url": url,
                "crawler": assigned_node,
                "status": "assigned"
            })
            
            logging.info(f"Assigned task {task_id} to crawler {assigned_node} for URL: {url}")
            task_id += 1
            # Move to next crawler for next assignment
            next_crawler_index = (next_crawler_index + 1) % num_crawlers
            update_monitoring_data()

        time.sleep(0.1)  # Delay to prevent busy waiting

    # Once all crawling is done, send shutdown signals.
    logging.info(f"Crawling complete. Processed {processed_urls} URLs. Shutting down nodes...")
    
    # Final metrics update
    system_metrics["end_time"] = datetime.now().isoformat()
    system_metrics["total_runtime_seconds"] = (datetime.now() - datetime.fromisoformat(system_metrics["start_time"])).total_seconds()
    update_monitoring_data()
    
    # Signal to crawler nodes to exit (by sending "None" as the URL).
    for node in active_crawler_nodes:
        try:
            comm.send(None, dest=node, tag=0)
        except Exception as e:
            logging.error(f"Error sending shutdown signal to crawler {node}: {e}")
            
    # Signal indexer node to shut down (we use tag 0 here to indicate shutdown).
    try:
        comm.send(None, dest=indexer_rank, tag=0)
    except Exception as e:
        logging.error(f"Error sending shutdown signal to indexer {indexer_rank}: {e}")

    logging.info("Master node finished URL distribution. Shutting down.")

if __name__ == "__main__":
    master_process()

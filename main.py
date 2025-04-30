#!/usr/bin/env python3
import os
import sys
import logging
import argparse
from masterNode import master_process
from crawlerNode import crawler_process
from indexerNode import indexer_process

def main():
    """
    Main entry point for the distributed web crawler system.
    This script runs the appropriate process based on the specified role.
    Each node runs independently with communication through AWS services.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Distributed Web Crawler')
    parser.add_argument('--role', choices=['master', 'crawler', 'indexer'], required=True,
                       help='Role of this node')
    parser.add_argument('--sqs-queue', help='SQS queue name for URL frontier')
    parser.add_argument('--status-queue', help='SQS queue name for status updates')
    parser.add_argument('--bucket', help='S3 bucket name for data storage')
    
    args = parser.parse_args()
    
    # Configure logging
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/{args.role}.log"),
            logging.StreamHandler()
        ],
        force=True
    )
    
    # Get logger instance
    logger = logging.getLogger()
    logger.info(f"Starting {args.role} node")
    
    # Set environment variable to signal that we're using explicit roles
    os.environ['EXPLICIT_ROLE'] = 'true'
    
    # Run the appropriate process based on role
    try:
        if args.role == 'master':
            # Master node
            logger.info("Starting master process")
            master_process(args.sqs_queue, args.status_queue, args.bucket)
        elif args.role == 'indexer':
            # Indexer node
            logger.info("Starting indexer process")
            indexer_process(args.bucket)
        elif args.role == 'crawler':
            # Crawler node
            logger.info("Starting crawler process")
            crawler_process(args.sqs_queue, args.status_queue, args.bucket)
        else:
            logger.error(f"Invalid role: {args.role}")
            return 1
    except Exception as e:
        logger.error(f"Error in process: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main()) 
# main.py
#!/usr/bin/env python3

import sys
import logging
import argparse
from masterNode import master_process
from crawlerNode import crawler_process
from indexerNode import indexer_process

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', required=True, choices=['master','crawler','indexer'])
    parser.add_argument('--sqs-queue', default='crawler-url-queue')
    parser.add_argument('--status-queue', default='crawler-status-queue')
    parser.add_argument('--bucket', default='web-crawler-data-storage')
    parser.add_argument('--region', default='us-east-1')
    args = parser.parse_args()

    # Clear any existing handlers to avoid duplicate logging
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(level=logging.INFO)

    if args.role == 'master':
        master_process(args.sqs_queue, args.status_queue, args.bucket, args.region)
    elif args.role == 'crawler':
        crawler_process(args.sqs_queue, args.status_queue, args.bucket, args.region)
    elif args.role == 'indexer':
        indexer_process(args.bucket, args.region, args.status_queue)
    else:
        logging.error("Unknown role")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())

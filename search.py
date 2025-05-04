#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import traceback
from indexerNode import WhooshIndexer

# Set up logging to file and console
def setup_logging():
    """Set up logging configuration."""
    os.makedirs("logs", exist_ok=True)
    log_file = os.path.join("logs", "search.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - Search - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def search_index(query, fields=None, limit=10):
    """Search the index with the given query."""
    try:
        # Initialize the indexer
        indexer = WhooshIndexer()

        # Perform search
        results = indexer.search(query, fields=fields, limit=limit)

        return results
    except Exception as e:
        logging.error(f"Error searching index: {e}")
        logging.debug(traceback.format_exc())

        # Try to recover and search again with minimal fields
        try:
            logging.info("Attempting fallback search...")
            indexer = WhooshIndexer()  # Reinitialize

            # Try with just the title field
            results = indexer.search(query, fields=["title"], limit=limit)
            return results
        except Exception as e2:
            logging.error(f"Fallback search also failed: {e2}")
            return []

def display_results(results):
    """Display search results in a formatted way."""
    if not results:
        print("No results found.")
        return

    print(f"\nFound {len(results)} results:\n")
    print("-" * 80)

    for i, result in enumerate(results, 1):
        try:
            # Handle missing fields gracefully
            title = result.get('title', 'No title')
            url = result.get('url', 'No URL')
            score = result.get('score', 0.0)

            print(f"{i}. {title}")
            print(f"   URL: {url}")
            print(f"   Score: {score:.2f}")

            if 'summary' in result and result['summary']:
                # Truncate summary if too long
                summary = result['summary']
                if len(summary) > 200:
                    summary = summary[:197] + "..."
                print(f"   Summary: {summary}")

            if 'keywords' in result and result['keywords']:
                # Truncate keywords if too long
                keywords = result['keywords']
                if len(keywords) > 100:
                    keywords = keywords[:97] + "..."
                print(f"   Keywords: {keywords}")

            print("-" * 80)
        except Exception as e:
            logging.error(f"Error displaying result {i}: {e}")
            print(f"{i}. [Error displaying result]")
            print("-" * 80)

def main():
    """Main function to handle command-line arguments and perform search."""
    parser = argparse.ArgumentParser(description='Search the web crawler index')
    parser.add_argument('query', help='Search query')
    parser.add_argument('--fields', nargs='+', help='Fields to search (default: title and content)')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of results to return')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Set up logging
    setup_logging()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Perform search
        print(f"Searching for: {args.query}")
        if args.fields:
            print(f"Fields: {', '.join(args.fields)}")

        results = search_index(args.query, fields=args.fields, limit=args.limit)

        # Display results
        display_results(results)
    except KeyboardInterrupt:
        print("\nSearch interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        logging.debug(traceback.format_exc())
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

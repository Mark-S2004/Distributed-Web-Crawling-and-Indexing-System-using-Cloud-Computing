#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import traceback
from flask import Flask, request, render_template, jsonify
from indexerNode import WhooshIndexer

# Set up logging
os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", "web_search.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - WebSearch - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Initialize Flask app
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # For proper UTF-8 handling

# Create templates directory if it doesn't exist
os.makedirs("templates", exist_ok=True)

# Create a simple HTML template for the search interface
with open("templates/search.html", "w") as f:
    f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>Web Crawler Search</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
        }
        .search-form {
            margin: 20px 0;
        }
        .search-input {
            width: 70%;
            padding: 10px;
            font-size: 16px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .search-button {
            padding: 10px 20px;
            font-size: 16px;
            background-color: #4285f4;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }
        .search-button:hover {
            background-color: #3367d6;
        }
        .result {
            margin: 20px 0;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .result h2 {
            margin-top: 0;
            color: #1a0dab;
        }
        .result a {
            color: #1a0dab;
            text-decoration: none;
        }
        .result a:hover {
            text-decoration: underline;
        }
        .result .url {
            color: #006621;
            font-size: 14px;
            margin-bottom: 5px;
        }
        .result .summary {
            color: #545454;
            font-size: 14px;
        }
        .result .keywords {
            color: #777;
            font-size: 12px;
            margin-top: 5px;
        }
        .no-results {
            margin: 20px 0;
            color: #777;
        }
        .error {
            color: red;
            margin: 20px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Web Crawler Search</h1>
        
        <div class="search-form">
            <form action="/" method="get">
                <input type="text" name="q" class="search-input" value="{{ query }}" placeholder="Enter search query...">
                <button type="submit" class="search-button">Search</button>
            </form>
        </div>
        
        {% if error %}
        <div class="error">
            <p>{{ error }}</p>
        </div>
        {% endif %}
        
        {% if results %}
        <div class="results">
            <p>Found {{ results|length }} results:</p>
            
            {% for result in results %}
            <div class="result">
                <h2><a href="{{ result.url }}" target="_blank">{{ result.title }}</a></h2>
                <div class="url">{{ result.url }}</div>
                {% if result.summary %}
                <div class="summary">{{ result.summary }}</div>
                {% endif %}
                {% if result.keywords %}
                <div class="keywords">Keywords: {{ result.keywords }}</div>
                {% endif %}
            </div>
            {% endfor %}
        </div>
        {% elif query %}
        <div class="no-results">
            <p>No results found for "{{ query }}".</p>
        </div>
        {% endif %}
    </div>
</body>
</html>
""")

def search_index(query, fields=None, limit=10):
    """Search the index with the given query."""
    try:
        # Initialize the indexer
        indexer = WhooshIndexer()
        
        # Perform search
        results = indexer.search(query, fields=fields, limit=limit)
        
        return results, None
    except Exception as e:
        logging.error(f"Error searching index: {e}")
        logging.debug(traceback.format_exc())
        
        # Try to recover and search again with minimal fields
        try:
            logging.info("Attempting fallback search...")
            indexer = WhooshIndexer()  # Reinitialize
            
            # Try with just the title field
            results = indexer.search(query, fields=["title"], limit=limit)
            return results, None
        except Exception as e2:
            logging.error(f"Fallback search also failed: {e2}")
            return [], str(e2)

@app.route('/')
def search_page():
    """Render the search page and handle search requests."""
    query = request.args.get('q', '')
    
    if not query:
        # Just show the search form
        return render_template('search.html', query='', results=None, error=None)
    
    try:
        # Perform search
        results, error = search_index(query, limit=20)
        
        # Process results for display
        processed_results = []
        for result in results:
            # Handle missing fields gracefully
            processed_result = {
                'title': result.get('title', 'No title'),
                'url': result.get('url', '#'),
                'score': result.get('score', 0.0)
            }
            
            # Add optional fields if available
            if 'summary' in result and result['summary']:
                summary = result['summary']
                if len(summary) > 300:
                    summary = summary[:297] + "..."
                processed_result['summary'] = summary
            
            if 'keywords' in result and result['keywords']:
                keywords = result['keywords']
                if len(keywords) > 150:
                    keywords = keywords[:147] + "..."
                processed_result['keywords'] = keywords
            
            processed_results.append(processed_result)
        
        return render_template('search.html', query=query, results=processed_results, error=error)
    
    except Exception as e:
        logging.error(f"Error processing search request: {e}")
        logging.debug(traceback.format_exc())
        return render_template('search.html', query=query, results=None, error=str(e))

@app.route('/api/search')
def api_search():
    """API endpoint for search."""
    query = request.args.get('q', '')
    limit = int(request.args.get('limit', 10))
    
    if not query:
        return jsonify({'error': 'No query provided'})
    
    try:
        # Perform search
        results, error = search_index(query, limit=limit)
        
        if error:
            return jsonify({'error': error})
        
        return jsonify({'results': results})
    
    except Exception as e:
        logging.error(f"Error processing API search request: {e}")
        return jsonify({'error': str(e)})

def main():
    """Main function to start the web server."""
    parser = argparse.ArgumentParser(description='Web interface for the crawler search')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    print(f"Starting web search interface on http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop the server")
    
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main()

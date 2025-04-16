from flask import Flask, render_template, request, jsonify
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser
from whoosh import scoring
import os

app = Flask(__name__)

# Initialize the search index
INDEX_DIR = "search_index"

def get_searcher():
    if not os.path.exists(INDEX_DIR):
        return None
    return open_dir(INDEX_DIR).searcher(weighting=scoring.BM25F)

@app.route('/')
def home():
    return render_template('search.html')

@app.route('/search')
def search():
    query = request.args.get('q', '')
    page = int(request.args.get('page', 1))
    
    if not query:
        return jsonify({'results': [], 'total': 0, 'page': page})
    
    try:
        with get_searcher() as searcher:
            if searcher is None:
                return jsonify({'error': 'Search index not found. Please run the crawler first.'})
                
            schema = searcher.schema
            fields = ["title", "content", "keywords"]
            parser = MultifieldParser(fields, schema)
            q = parser.parse(query)
            
            # Search with pagination (10 results per page)
            results = searcher.search_page(q, page, pagelen=10)
            
            response = {
                'results': [
                    {
                        'title': hit['title'] if hit['title'] else hit['url'],
                        'url': hit['url'],
                        'summary': hit['summary'],
                        'keywords': hit['keywords'],
                        'score': hit.score,
                        'last_updated': hit['last_updated']
                    } for hit in results
                ],
                'total': len(results),
                'page': page,
                'total_pages': (len(results) + 9) // 10
            }
            
            return jsonify(response)
            
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5000) 
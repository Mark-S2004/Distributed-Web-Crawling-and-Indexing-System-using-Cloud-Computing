import json
import os
import time
from datetime import datetime
from flask import Flask, render_template, jsonify
import threading

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Initialize global data storage
system_data = {
    "last_update": datetime.now().isoformat(),
    "master_status": "Unknown",
    "crawlers": {},
    "indexer_status": "Unknown",
    "urls_crawled": 0,
    "urls_indexed": 0,
    "urls_failed": 0,
    "error_count": 0,
    "recent_tasks": []
}

def load_monitoring_data():
    """Load and parse the monitoring data JSON file"""
    global system_data
    try:
        monitoring_data_path = "data/monitoring/monitoring_data.json"
        if os.path.exists(monitoring_data_path):
            with open(monitoring_data_path, "r") as f:
                data = json.load(f)
                
            # Update system_data with the loaded data
            system_data["last_update"] = datetime.now().isoformat()
            system_data["urls_crawled"] = data.get("urls_crawled", 0)
            system_data["urls_indexed"] = data.get("urls_indexed", 0)
            system_data["urls_failed"] = data.get("urls_failed", 0)
            system_data["error_count"] = data.get("error_count", 0)
            
            # Get crawler status
            system_data["crawlers"] = data.get("crawler_status", {})
            
            # Get crawler performance metrics
            system_data["crawler_performance"] = data.get("crawler_performance", {})
            
            # Get recent tasks (last 10)
            tasks = data.get("task_assignments", [])
            system_data["recent_tasks"] = sorted(
                tasks, 
                key=lambda x: x.get("time", ""), 
                reverse=True
            )[:10]
            
            # Calculate success rate
            total_urls = system_data["urls_crawled"] + system_data["urls_failed"]
            system_data["success_rate"] = (
                (system_data["urls_crawled"] / total_urls * 100) 
                if total_urls > 0 else 0
            )
            
            return True
    except Exception as e:
        print(f"Error loading monitoring data: {e}")
        return False

def read_log_files():
    """Read and parse the latest log entries"""
    global system_data
    
    try:
        # Check for master log
        master_log = "logs/master.log"
        if os.path.exists(master_log):
            with open(master_log, "r") as f:
                lines = f.readlines()
                if lines:
                    # Get the last 5 lines
                    system_data["master_logs"] = lines[-5:]
                    # Determine master status from last line
                    if "shutting down" in lines[-1].lower():
                        system_data["master_status"] = "Stopped"
                    else:
                        system_data["master_status"] = "Running"
        
        # Check crawler logs
        crawler_logs = {}
        for crawler_id in system_data["crawlers"].keys():
            log_file = f"logs/crawler_{crawler_id}.log"
            if os.path.exists(log_file):
                with open(log_file, "r") as f:
                    lines = f.readlines()
                    if lines:
                        crawler_logs[crawler_id] = lines[-3:]
        
        system_data["crawler_logs"] = crawler_logs
        
        return True
    except Exception as e:
        print(f"Error reading log files: {e}")
        return False

def background_data_update():
    """Background thread to update data periodically"""
    while True:
        load_monitoring_data()
        read_log_files()
        time.sleep(2)  # Update every 2 seconds

# Start the background update thread
update_thread = threading.Thread(target=background_data_update, daemon=True)
update_thread.start()

@app.route('/')
def dashboard():
    """Main dashboard view"""
    return render_template('dashboard.html')

@app.route('/api/data')
def get_data():
    """API endpoint to get the current system data"""
    return jsonify(system_data)

@app.route('/api/status')
def get_status():
    """API endpoint to get a summary of the system status"""
    active_crawlers = sum(1 for status in system_data["crawlers"].values() if status == "active")
    failed_crawlers = sum(1 for status in system_data["crawlers"].values() if status == "failed")
    
    status = {
        "master": system_data["master_status"],
        "active_crawlers": active_crawlers,
        "failed_crawlers": failed_crawlers,
        "total_crawlers": len(system_data["crawlers"]),
        "urls_crawled": system_data["urls_crawled"],
        "urls_indexed": system_data["urls_indexed"],
        "error_rate": f"{system_data.get('error_count', 0) / max(1, system_data['urls_crawled'] + system_data['urls_failed']) * 100:.1f}%"
    }
    
    return jsonify(status)

if __name__ == '__main__':
    # Initial data load
    load_monitoring_data()
    read_log_files()
    
    # Start the Flask app
    app.run(debug=True, port=5001) 
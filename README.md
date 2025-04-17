# Distributed-Web-Crawling-and-Indexing-System-using-Cloud-Computing
Develop a distributed web crawling and indexing system in Python using cloud-based virtual machines. Employ distributed task queues and storage to crawl websites, build a searchable index, and focus on fault tolerance to maintain operations despite system failures.

## Phase 3: Fault Tolerance and Monitoring

The system now includes the following fault tolerance and monitoring features:

### Fault Tolerance

1. **Heartbeat Monitoring**:
   - Crawler nodes send regular heartbeat signals to the master node
   - Master monitors crawler health and detects node failures
   - Simulates occasional failures for testing the recovery mechanisms
   
2. **Task Timeout Detection**:
   - Master tracks task processing time for each crawler
   - If a task takes too long (exceeds timeout), it's considered failed
   - Failed tasks are re-queued for processing by other crawlers

3. **Task Re-queueing**:
   - Failed tasks are automatically re-queued
   - URLs from failed crawlers are reassigned to active crawlers
   - Ensures no work is lost when a crawler fails

### Monitoring Dashboard

1. **Real-time Monitoring**:
   - Visual dashboard showing system status
   - Runs on http://localhost:5001
   - Auto-refreshes to show the latest data

2. **System Metrics**:
   - Number of URLs crawled and indexed
   - Number of active and failed crawler nodes
   - Error rates and task statuses
   - Performance statistics for each crawler

3. **Visualizations**:
   - Progress charts showing crawl/index status
   - System metrics in graphical form
   - Crawler status table
   - Recent task history
   - Live log display

### Enhanced Logging

- Detailed logging for all system components
- Log files stored for each crawler node
- Master node activity tracking
- Error reporting and tracking

## Phase 4: Data Persistence with Cloud Storage

The system now includes cloud storage capabilities for data persistence:

### AWS S3 Integration

1. **Persistent Data Storage**:
   - Raw HTML content is stored in AWS S3 for data durability
   - Processed text is also stored with metadata
   - Allows for future re-indexing without recrawling

2. **Fallback Mechanism**:
   - Gracefully falls back to local storage when cloud storage is unavailable
   - Creates a local directory structure that mirrors the cloud organization
   - Ensures no data is lost even when cloud connectivity fails

3. **Metadata Storage**:
   - Stores rich metadata alongside content
   - Includes keywords, summaries, timestamps and extraction statistics
   - Allows for advanced content analysis without reprocessing

### Storage Organization

Content is organized in the cloud using a hierarchical structure:
- `raw_html/YYYY/MM/DD/[url_hash].html` - For raw HTML content
- `processed_text/YYYY/MM/DD/[url_hash].txt` - For processed text content
- `metadata/YYYY/MM/DD/[url_hash].json` - For content metadata

### Configuration

1. **AWS Credentials Setup**:
   - A setup script helps configure your AWS credentials
   - Supports storing credentials in the standard AWS profile format
   - Automatically detects existing credentials

2. **Bucket Configuration**:
   - Automatically creates an S3 bucket if it doesn't exist
   - Uses a configurable bucket name defaulting to "web-crawler-data-storage"
   - Supports different AWS regions

## How to Run

1. Ensure you have Python 3.x and Microsoft MPI installed
2. Install dependencies from requirements.txt
3. Run the system using run_system.bat
4. You'll be prompted to set up AWS credentials if not already configured
5. Access the monitoring dashboard at http://localhost:5001

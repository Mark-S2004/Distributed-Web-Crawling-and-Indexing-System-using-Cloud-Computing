@echo off
echo Starting Distributed Web Crawling System...
set PYTHON_PATH=C:\Python312\python.exe
set MPI_PATH="C:\Program Files\Microsoft MPI\Bin\mpiexec.exe"
set PROJECT_PATH=C:\Users\acer\Distributed-Web-Crawling-and-Indexing-System-using-Cloud-Computing

cd %PROJECT_PATH%

:: Create directories for logs and data
echo Creating organized directory structure...
if not exist logs mkdir logs
if not exist data mkdir data
if not exist data\monitoring mkdir data\monitoring

echo Setting up NLTK data...
%PYTHON_PATH% setup_nltk.py

:: Check if AWS configuration is needed
if not exist aws_config.json (
    echo AWS configuration not found. Setting up AWS credentials...
    echo You can press Ctrl+C to skip this step if you don't want to use cloud storage.
    %PYTHON_PATH% setup_aws.py
) else (
    echo AWS configuration found. Using existing credentials.
)

:: Create local storage directory for fallback
if not exist crawled_data (
    echo Creating local storage directory...
    mkdir crawled_data
    mkdir crawled_data\raw_html
    mkdir crawled_data\processed_text
    mkdir crawled_data\metadata
)

:: Start the monitoring dashboard and search interface in the background
start "" %PYTHON_PATH% monitoring_dashboard.py
start "" %PYTHON_PATH% search_interface.py
echo Monitoring dashboard started on http://localhost:5001
echo Search interface started on http://localhost:5000

echo Starting the crawler system...
%MPI_PATH% -n 1 %PYTHON_PATH% masterNode.py : -n 3 %PYTHON_PATH% crawlerNode.py : -n 1 %PYTHON_PATH% indexerNode.py

echo.
echo Press any key to exit...
pause > nul 
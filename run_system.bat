@echo off
echo Starting Distributed Web Crawling System...
set PYTHON_PATH=C:\Python312\python.exe
set MPI_PATH="C:\Program Files\Microsoft MPI\Bin\mpiexec.exe"
set PROJECT_PATH=C:\Users\acer\Distributed-Web-Crawling-and-Indexing-System-using-Cloud-Computing

cd %PROJECT_PATH%

%MPI_PATH% -n 1 %PYTHON_PATH% masterNode.py : -n 3 %PYTHON_PATH% crawlerNode.py : -n 1 %PYTHON_PATH% indexerNode.py

echo.
echo Press any key to exit...
pause > nul 
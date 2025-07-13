@echo off
echo Closing DuckDB connections...
call conda activate try_request
python close_db_connections.py
pause 
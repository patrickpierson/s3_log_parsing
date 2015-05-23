S3 Log parsing via python.

Usage
--------------
Download s3 logs to "logs" folder
run ./s3parse.py 
Run 'parse s3 logs'
Task: parse s3 logs
Logs location: "./logs/*" # Default location is ./logs/*

This will create a test.csv that you can then run basic analytics on.

./s3parse.py 
Available Tasks
----------------
count bytes sent # prints total bytes in KB
show 404 count # prints 404s
show ips count # prints ip counts
show useragent count # prints useragent counts
quit # quits

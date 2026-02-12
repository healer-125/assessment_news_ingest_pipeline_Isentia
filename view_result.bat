@echo off
setlocal EnableDelayedExpansion

echo Getting shard iterator...
for /f "usebackq delims=" %%i in (`aws --endpoint-url=http://localhost:4566 kinesis get-shard-iterator --stream-name news-ingest-stream --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --region us-east-1 --query "ShardIterator" --output text`) do set "SHARD_ITER=%%i"

if "!SHARD_ITER!"=="" (
    echo Failed to get shard iterator. Is LocalStack running?
    exit /b 1
)

echo Fetching records...
aws --endpoint-url=http://localhost:4566 kinesis get-records --shard-iterator "!SHARD_ITER!" --region us-east-1

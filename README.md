# News Ingest Pipeline

A production-ready Python application that fetches news articles from NewsAPI and streams them to AWS Kinesis Data Streams.

## Features

- **NewsAPI Integration**: Fetches articles from NewsAPI Everything endpoint
- **Data Processing**: Transforms and validates article data
- **AWS Kinesis Integration**: Streams processed data to Kinesis with batching support
- **Containerized**: Docker support for easy deployment
- **Configurable**: Environment-based configuration
- **Resilient**: Error handling, retry logic, and logging

## Architecture

```
NewsAPI → Data Processor → AWS Kinesis
```

The pipeline periodically:
1. Fetches articles from NewsAPI
2. Processes and validates the data
3. Streams to AWS Kinesis in batches

## Prerequisites

- Python 3.11+
- AWS Account with Kinesis stream created
- NewsAPI API key
- Docker (optional, for containerized deployment)

## Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd assessment_news_ingest_pipeline_Isentia
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

Required variables:
- `NEWSAPI_KEY`: Your NewsAPI API key
- `KINESIS_STREAM_NAME`: Name of your Kinesis stream
- AWS credentials (via environment variables or IAM role)

### 4. Create AWS Kinesis Stream

**Real AWS:**

```bash
aws kinesis create-stream \
  --stream-name news-ingest-stream \
  --shard-count 1 \
  --region us-east-1
```

**LocalStack (local development):**

```bash
# Start LocalStack first
docker run --rm -d -p 4566:4566 --name localstack localstack/localstack

# Create the stream against LocalStack
aws --endpoint-url=http://localhost:4566 kinesis create-stream \
  --stream-name news-ingest-stream \
  --shard-count 1 \
  --region us-east-1
```

Then set in `.env`: `AWS_ENDPOINT_URL=http://localhost:4566` and use `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`. After that, `python -m src.main` will use LocalStack.

## Usage

### Local Development

```bash
python -m src.main
```

### Docker

Build the image:

```bash
docker build -t news-ingest-pipeline .
```

Run the container:

```bash
docker run --env-file .env news-ingest-pipeline
```

Or with environment variables:

```bash
docker run \
  -e NEWSAPI_KEY=your_key \
  -e KINESIS_STREAM_NAME=your_stream \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  news-ingest-pipeline
```

## Configuration

All configuration is done via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `NEWSAPI_KEY` | NewsAPI API key | Required |
| `NEWSAPI_QUERY` | Search query | `technology` |
| `NEWSAPI_PAGE_SIZE` | Results per page | `100` |
| `NEWSAPI_SORT_BY` | Sort order | `publishedAt` |
| `KINESIS_STREAM_NAME` | Kinesis stream name | Required |
| `AWS_ENDPOINT_URL` | Custom endpoint (e.g. LocalStack: `http://localhost:4566`) | — |
| `AWS_REGION` | AWS region | `us-east-1` |
| `POLL_INTERVAL_SECONDS` | Polling interval | `300` (5 min) |
| `HOURS_BACK` | Hours to look back for articles | `168` (7 days) |
| `LOG_LEVEL` | Logging level | `INFO` |

## Data Schema

Articles written to Kinesis have the following structure:

```json
{
  "article_id": "sha256_hash",
  "source_name": "Source Name",
  "title": "Article Title",
  "content": "Article content...",
  "url": "https://example.com/article",
  "author": "Author Name",
  "published_at": "2026-01-26T10:00:00",
  "ingested_at": "2026-01-26T12:00:00"
}
```

## Project Structure

```
assessment_news_ingest_pipeline_Isentia/
├── src/
│   ├── __init__.py
│   ├── main.py              # Entry point
│   ├── config.py            # Configuration
│   ├── newsapi_client.py    # NewsAPI integration
│   ├── data_processor.py    # Data transformation
│   ├── kinesis_writer.py    # Kinesis integration
│   └── scheduler.py         # Polling logic
├── tests/                   # Unit tests
├── requirements.txt         # Dependencies
├── Dockerfile               # Docker configuration
├── .env.example             # Environment template
└── README.md                # This file
```

## Error Handling

- Automatic retries for transient failures
- Graceful error handling with logging
- Validation of required fields before sending to Kinesis
- Connection testing before starting ingestion

## Logging

The application uses Python's logging module with configurable log levels:
- `DEBUG`: Detailed debugging information
- `INFO`: General informational messages
- `WARNING`: Warning messages
- `ERROR`: Error messages

## Testing

```bash
# Run tests (when implemented)
pytest tests/
```

## License

MIT

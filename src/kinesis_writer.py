import logging
import json
import time
from typing import List, Dict, Optional
import boto3
from botocore.exceptions import ClientError, BotoCoreError

from src.config import Config

logger = logging.getLogger(__name__)


class KinesisWriter:
    """Writer for streaming data to AWS Kinesis."""
    
    def __init__(
        self,
        stream_name: str,
        region: str = "us-east-1",
        endpoint_url: Optional[str] = None,
    ):
        """
        Initialize Kinesis writer.

        Args:
            stream_name: Name of the Kinesis stream
            region: AWS region
            endpoint_url: Optional custom endpoint (e.g. http://localhost:4566 for LocalStack)
        """
        self.stream_name = stream_name
        self.region = region
        client_kwargs: Dict = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        self.client = boto3.client("kinesis", **client_kwargs)
        self.batch_size = Config.KINESIS_BATCH_SIZE
    
    def _put_record(self, data: Dict, partition_key: str = None) -> bool:
        """
        Put a single record to Kinesis.
        
        Args:
            data: Data dictionary to send
            partition_key: Partition key (defaults to article_id)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            json_data = json.dumps(data)
            
            if not partition_key:
                partition_key = data.get("article_id", "default")
            
            response = self.client.put_record(
                StreamName=self.stream_name,
                Data=json_data.encode('utf-8'),
                PartitionKey=partition_key
            )
            
            logger.debug(
                f"Sent record to Kinesis: {data.get('article_id')} "
                f"(SequenceNumber: {response.get('SequenceNumber')})"
            )
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"AWS Kinesis error ({error_code}): {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error sending record to Kinesis: {str(e)}")
            return False
    
    def _put_records_batch(self, records: List[Dict]) -> tuple[int, int]:
        """
        Put multiple records to Kinesis in a batch.
        
        Args:
            records: List of data dictionaries
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not records:
            return 0, 0
        
        try:
            kinesis_records = []
            for record in records:
                json_data = json.dumps(record)
                partition_key = record.get("article_id", "default")
                
                kinesis_records.append({
                    "Data": json_data.encode('utf-8'),
                    "PartitionKey": partition_key
                })
            
            response = self.client.put_records(
                Records=kinesis_records,
                StreamName=self.stream_name
            )
            
            successful = response.get("Records", [])
            failed_count = response.get("FailedRecordCount", 0)
            success_count = len(successful) - failed_count
            
            if failed_count > 0:
                logger.warning(
                    f"Failed to send {failed_count} records to Kinesis. "
                    f"Successfully sent {success_count} records."
                )
            else:
                logger.info(f"Successfully sent {success_count} records to Kinesis")
            
            return success_count, failed_count
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.error(f"AWS Kinesis batch error ({error_code}): {str(e)}")
            return 0, len(records)
        except Exception as e:
            logger.error(f"Error sending batch to Kinesis: {str(e)}")
            return 0, len(records)
    
    def write_articles(self, articles: List[Dict], use_batching: bool = True) -> tuple[int, int]:
        """
        Write articles to Kinesis stream.
        
        Args:
            articles: List of processed article dictionaries
            use_batching: Whether to use batch writes (more efficient)
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not articles:
            logger.warning("No articles to write to Kinesis")
            return 0, 0
        
        if use_batching and len(articles) > 1:
            # Use batch writes for efficiency
            # Kinesis allows up to 500 records per batch
            total_success = 0
            total_failed = 0
            
            for i in range(0, len(articles), self.batch_size):
                batch = articles[i:i + self.batch_size]
                success, failed = self._put_records_batch(batch)
                total_success += success
                total_failed += failed
                
                # Small delay between batches to avoid throttling
                if i + self.batch_size < len(articles):
                    time.sleep(0.1)
            
            return total_success, total_failed
        else:
            # Write records individually
            success_count = 0
            failed_count = 0
            
            for article in articles:
                if self._put_record(article):
                    success_count += 1
                else:
                    failed_count += 1
                
                # Small delay to avoid throttling
                time.sleep(0.01)
            
            return success_count, failed_count
    
    def test_connection(self) -> bool:
        """
        Test connection to Kinesis stream.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self.client.describe_stream(StreamName=self.stream_name)
            stream_status = response.get("StreamDescription", {}).get("StreamStatus")
            logger.info(f"Kinesis stream '{self.stream_name}' status: {stream_status}")
            return stream_status in ["ACTIVE", "UPDATING"]
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == "ResourceNotFoundException":
                logger.error(f"Kinesis stream '{self.stream_name}' not found")
            else:
                logger.error(f"Error connecting to Kinesis: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error testing Kinesis connection: {str(e)}")
            return False

import boto3
import logging
from airflow.hooks.base_hook import BaseHook


conn = BaseHook.get_connection("aws_default")
s3 = boto3.client("s3",
                         aws_access_key_id=conn.login,
                         aws_secret_access_key=conn.password,
                         region_name="eu-west-1")

def move_s3_files(bucket_name, source_prefix, destination_prefix):
    """
    Move files from source folder to destination folder in S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        source_prefix (str): Prefix of the source folder.
        destination_prefix (str): Prefix of the destination folder.
    """
    try:
        # List objects in the source folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        if "Contents" not in response:
            logging.info(f"No files found in {source_prefix}")
            return
        
        for obj in response["Contents"]:
            source_key = obj["Key"]
            destination_key = source_key.replace(source_prefix, destination_prefix, 1)
            
            # Skip if source key is a folder
            if source_key.endswith("/"):
                continue

            # Copy object to archive
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                Key=destination_key
            )

            # Delete original object
            s3.delete_object(Bucket=bucket_name, Key=source_key)

            logging.info(f"Moved: {source_key} -> {destination_key}")

        logging.info("All files have been moved.")
    except Exception as e:
        logging.error(f"Error moving files: {e}")



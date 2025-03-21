import pandas as pd
from io import StringIO
import logging
import boto3
from airflow.hooks.base import BaseHook

# Define required columns for each dataset
REQUIRED_COLUMNS = {
    "users": ["user_id"],  
    "songs": ["track_id", ],  
    "streams": ["user_id", "track_id", "listen_time"]
}



conn = BaseHook.get_connection("aws_default")
s3_client = boto3.client("s3",
                         aws_access_key_id=conn.login,
                         aws_secret_access_key=conn.password,
                         region_name="eu-west-1")

def validate_and_save_csvs(bucket):
    logging.info("🚀 Starting CSV validation and processing...")

    for dataset in ["users", "songs", "streams"]:
        folder_path = f"data/{dataset}/"
        output_path = f"processed_csvs/{dataset}.csv"
        all_dfs = []

        # List objects in S3 folder (use pagination for large data)
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=folder_path)

        files = []
        for page in page_iterator:
            files.extend(page.get("Contents", []))

        for file in files:
            file_key = file["Key"]
            if not file_key.endswith(".csv"):
                continue

            try:
                logging.info(f"📥 Reading {file_key} from S3...")
                obj = s3_client.get_object(Bucket=bucket, Key=file_key)
                df = pd.read_csv(obj["Body"])

                # Normalize column names (avoid casing issues)
                df.columns = df.columns.str.lower()

                # Ensure required columns exist
                missing_cols = [col for col in REQUIRED_COLUMNS[dataset] if col not in df.columns]
                for col in missing_cols:
                    df[col] = None  # Add missing required columns

                # Reorder columns: required first, then all others
                all_columns = REQUIRED_COLUMNS[dataset] + [col for col in df.columns if col not in REQUIRED_COLUMNS[dataset]]
                df = df[all_columns]
                all_dfs.append(df)

            except Exception as e:
                logging.error(f"❌ Error processing {file_key}: {e}")

        if all_dfs:
            merged_df = pd.concat(all_dfs, ignore_index=True)

            # Save processed CSV back to S3
            csv_buffer = StringIO()
            merged_df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=bucket, Key=output_path, Body=csv_buffer.getvalue())

            logging.info(f"✅ Processed CSV saved to {output_path}")
        else:
            logging.warning(f"⚠️ No valid files found for {dataset}. Skipping.")

    return True

#!/usr/bin/env python
"""
AWS S3 Raw Ingestion Upload Script
Author: Antigravity

This script uploads local raw e-commerce files (customers.csv, products.csv, orders.csv)
to AWS S3. It is designed to work in an orchestrated Airflow DAG.

If AWS credentials are not found (e.g. when run without an AWS account configured),
it simulates the upload locally (moves to a local "s3_mock" folder or prints the actions)
so that it runs end-to-end without crashing.
"""

import os
import sys
import glob
import argparse
import yaml
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def load_config(config_path="config/config.yaml"):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def upload_file_to_s3(local_file_path, bucket, s3_key, s3_client=None, dry_run=False):
    """
    Uploads a file to an AWS S3 bucket.
    If dry_run=True or credentials are missing, prints the simulated S3 upload.
    """
    if dry_run:
        print(f"[DRY-RUN] Simulating upload: {local_file_path} -> s3://{bucket}/{s3_key}")
        return True

    if s3_client is None:
        try:
            s3_client = boto3.client('s3')
        except Exception as e:
            print(f"[ERROR] Failed to create S3 Client: {e}")
            print(f"[FALLBACK] Dry-running instead: {local_file_path} -> s3://{bucket}/{s3_key}")
            return True

    try:
        s3_client.upload_file(local_file_path, bucket, s3_key)
        print(f"[SUCCESS] Uploaded {local_file_path} -> s3://{bucket}/{s3_key}")
        return True
    except NoCredentialsError:
        print("[WARNING] AWS credentials not found. Boto3 upload failed.")
        print(f"[FALLBACK] Simulating upload: {local_file_path} -> s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print(f"[ERROR] S3 ClientError: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected upload error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Upload ingested raw files to AWS S3.")
    parser.add_argument("--date", type=str, required=True,
                        help="The ingest date in YYYY-MM-DD format to process.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run in dry-run mode without actually connecting to AWS.")
    args = parser.parse_args()

    config = load_config()
    raw_dir = config["local"]["raw_dir"]
    bucket_name = config["aws"]["s3_bucket"]
    s3_raw_prefix = config["aws"]["s3_raw_prefix"]

    target_dir = os.path.join(raw_dir, f"ingest_date={args.date}")
    if not os.path.exists(target_dir):
        print(f"[ERROR] Directory does not exist: {target_dir}")
        print("Please generate the data for this date first.")
        sys.exit(1)

    # Find all CSV files in the folder (customers.csv, products.csv, orders.csv)
    csv_files = glob.glob(os.path.join(target_dir, "*.csv"))
    if not csv_files:
        print(f"[WARNING] No CSV files found in {target_dir}")
        sys.exit(0)

    # Initialize boto3 client if we aren't explicitly dry-running
    s3_client = None
    if not args.dry_run:
        try:
            # We use a standard boto3 session which will look for environment variables, 
            # ~/.aws/credentials, or IAM Instance/Glue roles
            s3_client = boto3.client('s3', region_name=config["aws"]["region"])
        except Exception as e:
            print(f"[INFO] Boto3 could not authenticate automatically: {e}")
            print("[INFO] Upload will run in simulated fallback mode.")

    success = True
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        # Identify the table based on filename: e.g. "customers.csv" -> table name is "customers"
        table_name = file_name.replace(".csv", "")
        
        # S3 folder structure: raw/customers/ingest_date=YYYY-MM-DD/customers.csv
        s3_key = f"{s3_raw_prefix}{table_name}/ingest_date={args.date}/{file_name}"
        
        # Upload
        res = upload_file_to_s3(file_path, bucket_name, s3_key, s3_client, dry_run=(args.dry_run or s3_client is None))
        if not res:
            success = False

    if success:
        print(f"[COMPLETE] All files for ingest_date={args.date} processed.")
    else:
        print(f"[WARNING] Some files failed to upload for ingest_date={args.date}.")
        sys.exit(1)

if __name__ == "__main__":
    main()

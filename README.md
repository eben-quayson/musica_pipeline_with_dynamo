# **Airflow DAG for Data Validation, Processing, and Glue Job Execution**

## **1. Overview**
This project implements an **Apache Airflow DAG** to automate the data pipeline for **validating, appending, and processing CSV files** stored in an **S3 bucket**. The pipeline ensures data quality, merges multiple files, and triggers an **AWS Glue Job** for further transformation before archiving processed files.

## **2. Architecture**
The workflow consists of:
1. **S3KeySensor Tasks**: Waits for incoming CSV files in an **S3 bucket**.
2. **PythonOperator Task**: Validates and merges CSVs.
3. **GlueJobOperator Task**: Triggers an AWS Glue job for transformation.
4. **Archival Task**: Moves processed files to an archive folder in S3.

### **Technology Stack**
- **Apache Airflow** (MWAA / Astronomer)
- **AWS S3** (Storage)
- **AWS Glue** (ETL Processing)
- **Python** (Validation & Processing)
- **DynamoDB**(Database)

## **3. Installation & Setup**
### **Prerequisites**
Ensure you have the following installed:
- **Apache Airflow (2.6+ or MWAA/Astronomer)**
- **AWS CLI** (configured with IAM permissions)
- **Python 3.12**

### **Installation Steps**
1. **Set up Airflow DAG folder**
   ```sh
   mkdir -p ~/airflow/dags
   cd ~/airflow/dags
   ```
2. **Install dependencies**
   ```sh
   pip install apache-airflow apache-airflow-providers-amazon boto3 pandas
   ```
3. **Create an Airflow Connection for AWS**
   ```sh
   airflow connections add aws_default --conn-type aws      --conn-extra '{"aws_access_key_id": "YOUR_ACCESS_KEY", "aws_secret_access_key": "YOUR_SECRET_KEY", "region_name": "eu-west-1"}'
   ```

4. **Set Airflow Variables (via UI or CLI)**
   ```sh
   airflow variables set preprocessed_data_s3 "musica-data-bucket"
   airflow variables set archive_prefix "archive/"
   airflow variables set preprocessed_data_prefix "preprocessed/"
   airflow variables set script_location "s3://musica-glue-scripts/job.py"
   ```

5. **Deploy the DAG**
   - Copy the DAG script to `~/airflow/dags/`
   - Restart Airflow: `airflow scheduler restart && airflow webserver restart`

---

## **4. DAG Breakdown**
### **DAG Configuration**
```python
default_args = {
    "owner": "Eben",
    "start_date": datetime(2024, 3, 18),
    "retries": 1,
    "max_active_runs": 1,
}
```
- **Owner**: Specifies the owner of the DAG.
- **Start Date**: Defines when the DAG starts running.
- **Retries**: Number of times the task will retry in case of failure.

---

### **1️⃣ Waiting for CSV Files (S3 Sensors)**
```python
wait_for_users = S3KeySensor(
    task_id="wait_for_users_data",
    bucket_name=BUCKET_NAME,
    bucket_key="data/users/*.csv",
    wildcard_match=True,
    aws_conn_id="aws_default",
    timeout=600,
    poke_interval=30,
    mode="poke",
)
```
- **S3KeySensor** monitors an S3 bucket for files (`*.csv`).
- **Poke Mode**: The task will check every 30 seconds until the file arrives or timeout (600s).

---

### **2️⃣ Validating & Merging CSVs**
```python
validate_save_task = PythonOperator(
    task_id="validate_and_save_csvs",
    python_callable=validate_and_save_csvs,
    op_kwargs={"bucket": BUCKET_NAME},
)
```
- Calls `validate_and_save_csvs()` from `helpers.modules1`.
- Ensures the function receives the correct **bucket name**.

**Function Signature (Example)**
```python
def validate_and_save_csvs(bucket: str):
    print(f"Processing bucket: {bucket}")
    # Validation logic here
```

---

### **3️⃣ Running AWS Glue Job**
```python
run_glue_job = GlueJobOperator(
    task_id="run_glue_job",
    job_name="musica_data_job",
    script_location=script_location,
    region_name="eu-west-1",
    aws_conn_id="aws_default",
    script_args={
        "--S3_INPUT_BUCKET": "musica-data-bucket",
        "--S3_ARCHIVE_BUCKET": "musica-data-bucket/archive",
        "--DYNAMODB_GENRE_KPIS": "DYNAMODB_GENRE_KPIS",
        "--DYNAMODB_TOP_SONGS": "DYNAMODB_TOP_SONGS",
        "--DYNAMODB_TOP_GENRES": "DYNAMODB_TOP_GENRES",
    },
)
```
- Executes an **AWS Glue ETL Job**.
- Reads from **S3_INPUT_BUCKET**, writes to **S3_ARCHIVE_BUCKET**.
- Stores aggregated metrics in **DynamoDB tables**.

---

### **4️⃣ Moving Processed Files to Archive**
```python
move_files_task = PythonOperator(
    task_id="move_files_to_archive",
    python_callable=move_s3_files,
    op_kwargs={
        "bucket_name": BUCKET_NAME,
        "source_prefix": preprocessed_data_prefix,
        "destination_prefix": archive_prefix,
    },
)
```
- Moves validated files from **`preprocessed/`** to **`archive/`** in S3.

---

### **5️⃣ DAG Execution Flow**
```python
[wait_for_users, wait_for_songs, wait_for_streams] >> validate_save_task >> run_glue_job >> move_files_task
```
- Waits for all required CSVs before processing.
- Validates & appends CSVs → Runs Glue Job → Moves processed files to archive.

---

## **5. Troubleshooting**
### **Common Issues & Fixes**
| Error | Possible Cause | Solution |
|-------|---------------|----------|
| `TypeError: validate_and_save_csvs() missing 1 required positional argument: 'bucket'` | Incorrect `op_kwargs` format | Ensure it's `{ "bucket": BUCKET_NAME }`, not `{ "Bucket": BUCKET_NAME }` |
| `S3KeySensor timeout` | No CSVs uploaded | Manually upload a test file to S3 |
| `GlueJobOperator fails` | Incorrect IAM permissions | Verify IAM roles for Glue & S3 access |

---

## **6. Future Enhancements**
- **Integrate Snowflake/Redshift** for analytics.
- **Improve monitoring** with Airflow task logging.
- **Implement data validation** using `pandera` or `Great Expectations`.

---

## **7. Conclusion**
This DAG automates the **validation, processing, and transformation** of CSV data, ensuring smooth integration between **AWS S3, Glue, and Airflow**. 🚀

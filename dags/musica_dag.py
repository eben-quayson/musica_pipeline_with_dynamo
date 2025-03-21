from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime
from dags.helpers.validate_and_save import validate_and_save_csvs  
from helpers.archive_data import move_s3_files
from airflow.models import Variable

BUCKET_NAME = Variable.get("preprocessed_data_s3")
destination_prefix = Variable.get("archive_prefix")
source_prefix = Variable.get("preprocessed_data_prefix")
script_location = Variable.get("script_location")


default_args = {
    "owner": "Eben",
    "start_date": datetime(2024, 3, 18),
    "retries": 1,
    "max_active_runs": 1,
}

with DAG(
    "validate_append_and_run_glue",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # ✅ Wait for CSV files
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

    wait_for_songs = S3KeySensor(
        task_id="wait_for_songs_data",
        bucket_name=BUCKET_NAME,
        bucket_key="data/songs/*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=30,
        mode="poke",
    )

    wait_for_streams = S3KeySensor(
        task_id="wait_for_streams_data",
        bucket_name=BUCKET_NAME,
        bucket_key="data/streams/*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=30,
        mode="poke",
    )

    # ✅ Validate, Append & Save CSVs
    validate_save_task = PythonOperator(
        task_id="validate_and_save_csvs",
        python_callable=validate_and_save_csvs,
        op_kwargs={"bucket": BUCKET_NAME}
    )

    # ✅ Corrected: Glue Job with script_location
    args = {
        "--S3_INPUT_BUCKET": "musica-data-bucket",
        "--S3_ARCHIVE_BUCKET": "musica-data-bucket/archive",
        "--DYNAMODB_GENRE_KPIS": "DYNAMODB_GENRE_KPIS",
        "--DYNAMODB_TOP_SONGS": "DYNAMODB_TOP_SONGS",
        "--DYNAMODB_TOP_GENRES": "DYNAMODB_TOP_GENRES",
    }

    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="musica_data_job",
        script_location=script_location,  # ✅ Pass Glue script
        region_name="eu-west-1",
        aws_conn_id="aws_default",
        script_args=args,  # ✅ Pass Glue arguments
    )


    move_files_task = PythonOperator(
        task_id="move_files_to_archive",
        python_callable=move_s3_files,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "source_prefix": source_prefix,
            "destination_prefix": destination_prefix,
        },
    )
    

    # ✅ DAG Flow
    [wait_for_users, wait_for_songs, wait_for_streams] >> validate_save_task >> run_glue_job >> move_files_task




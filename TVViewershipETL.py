import os
import logging
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define variables for S3 bucket and paths
S3_BUCKET = Variable.get("s3_bucket", default_var="your-default-bucket-name")
RAW_DATA_PREFIX = Variable.get("raw_data_prefix", default_var="raw_data/")
PROCESSED_DATA_PREFIX = Variable.get("processed_data_prefix", default_var="processed_data/")
SCRIPTS_PREFIX = Variable.get("scripts_prefix", default_var="scripts/")
PROGRAM_MAPPING_FILE = Variable.get("program_mapping_file", default_var="program_data.txt")

# EMR cluster configuration
JOB_FLOW_OVERRIDES = {
    "Name": "TVViewershipETL",
    "ReleaseLabel": "emr-6.8.0",
    "Applications": [{"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Define the DAG
with DAG(
    dag_id="daily_tv_viewership_etl_pipeline",
    default_args=default_args,
    description="A daily ETL pipeline to process TV viewership data using PySpark and load into Redshift",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Step 1: Create EMR Cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        aws_conn_id="aws_default",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        region_name="us-east-1",
    )

    # Step 2: Wait for EMR Cluster to be ready
    wait_for_emr_cluster = EmrJobFlowSensor(
        task_id="wait_for_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    # Step 3: Run PySpark job on EMR
    run_spark_job = EmrCreateJobFlowOperator(
        task_id="run_spark_job",
        aws_conn_id="aws_default",
        steps=[
            {
                "Name": "Run PySpark Transformation",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "s3://{{ var.value.s3_bucket }}/{{ var.value.scripts_prefix }}/tv_viewership_etl_pipeline.py",
                        "--input-path", f"s3://{S3_BUCKET}/{RAW_DATA_PREFIX}",
                        "--output-path", f"s3://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}"
                    ],
                },
            }
        ],
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        region_name="us-east-1",
    )

    # Step 4: Terminate EMR Cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    # Step 5: Load transformed data into Redshift
    load_into_redshift = S3ToRedshiftOperator(
        task_id="load_into_redshift",
        schema="public",
        table="tv_viewership",
        s3_bucket=S3_BUCKET,
        s3_key=PROCESSED_DATA_PREFIX,
        copy_options=["FORMAT AS PARQUET"],
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
    )

    # DAG Workflow
    create_emr_cluster >> wait_for_emr_cluster >> run_spark_job >> terminate_emr_cluster >> load_into_redshift

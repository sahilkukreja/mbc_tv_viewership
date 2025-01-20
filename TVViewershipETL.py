from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

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
    dag_id="tv_viewership_etl_pipeline",
    default_args=default_args,
    description="An ETL pipeline to process TV viewership data using PySpark and load into Redshift",
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
                        "--class", "org.apache.spark.examples.SparkPi",
                        "s3://<your-s3-bucket>/scripts/tv_viewership_etl_pipeline.py",
                        "--input-path", "s3://<your-s3-bucket>/raw_data/",
                        "--output-path", "s3://<your-s3-bucket>/processed_data/"
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
        s3_bucket="<your-s3-bucket>",
        s3_key="processed_data/minute_level_viewership/",
        copy_options=["FORMAT AS PARQUET"],
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
    )

    # DAG Workflow
    create_emr_cluster >> wait_for_emr_cluster >> run_spark_job >> terminate_emr_cluster >> load_into_redshift

"""
This module defines an Airflow workflow (DAG) that performs
a transfer operation of information from CSV and JSON files
stored in Google Cloud Storage (GCS) to various tables in BigQuery.
Includes all necessary imports and configurations to make this task.
"""
from datetime import datetime
import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator)

# Vars
ENVIRONMENT = Variable.get("environment")
PATH_DAG_CONFIG = "path_dag_config_airflow_default"
PATH_DAG_CONFIG_JSON = f"{PATH_DAG_CONFIG}/ingestion_carousel_config.json"


# Functions
def create_table_name(project_id: str, dataset_id: str,
                      bq_table_name: str) -> str:
    """Take 3 strings and transform to one string.

    Arguments:
    project_id -- destination bigquery project id
    dataset_id -- destination bigquery dataset id
    bq_table_name -- destination bigquery table id

    Returns:
    full bigquery path string of the destination table
    """
    return f"{project_id}_{ENVIRONMENT}.{dataset_id}_{ENVIRONMENT}." \
           f"tbl_{bq_table_name}_{ENVIRONMENT}"


def parse_str_to_dt(str_date: str) -> datetime:
    """Take a string and format to date.

    Arguments:
    str_date -- string that contains a date

    Returns:
    date in format YYYY-mm-dd
    """
    return datetime.strptime(str_date, "%Y-%m-%d")


def create_url_gcs(gcs_archive_name: str, archive_type: str) -> str:
    """Take two strings and transform to one string

    Arguments:
    gcs_archive_name -- source bucket name in gcs
    archive_type -- source file name in gcs

    Returns:
    full gcs path string of the source file
    """
    return f"sources/{gcs_archive_name}.{archive_type}"


def parse_start_date(dag_kwargs: dict) -> dict:
    """Take a dict and format some string arguments to date and return a dict

    Arguments:
    dag_kwargs -- dag key arguments

    Returns:
    dag kwargs dict updated
    """
    dag_kwargs.update({
        "start_date": parse_str_to_dt(dag_kwargs["start_date"]),
        "end_date": parse_str_to_dt(dag_kwargs["end_date"])
    })
    return dag_kwargs


def create_dag_config_task(task_id: str, bucket_gcs: str,
                           url_file: str, file_type: str) -> dict:
    """Take 4 string and make a dict with config task

    Arguments:
    task_id -- airflow task name
    bucket_gcs -- google cloud storage bucket name
    url_file -- path of files in the bucket
    file_type -- file type of file in gcs

    Returns:
    dict with config task to dag operator
    """
    return {
        "task_id": task_id,
        "bucket": bucket_gcs,
        "source_objects": url_file,
        "file_type": file_type
    }


def create_dag_config_data(bq_table_name: str, source_format_dag: str,
                           bigquery_conn_id: str, partition_column: str,
                           gcs_conn_id: str) -> dict:
    """Take 5 string and make a dict with config data

    Arguments:
    bq_table_name -- destination bigquery table name
    source_format_dag -- file format to dag operator
    bigquery_conn_id -- bigquery connection id in airflow
    google_cloud_storage_conn_id -- google cloud storage id in airflow
    partition_column -- partition column in bigquery

    Returns:
    dict with config data to dag operator
    """
    return {
        "bq_table_name": bq_table_name,
        "source_format": source_format_dag,
        "bigquery_conn_id": bigquery_conn_id,
        "gcs_conn_id": gcs_conn_id,
        "time_partitioning": {'type': 'DAY',
                              'field': partition_column
                              }
    }


def create_config_dag_operator(dag_config_task: dict,
                               dag_config_data: dict) -> dict:
    """Take 2 dict and make a dict with all dag operator config

    Arguments:
    dag_config_task -- dag config task
    dag_config_data -- dag config data

    Returns:
    dict with config to dag operator
    """
    dict_config_dag_operator = {
        "task_id": dag_config_task["task_id"],
        "bucket": dag_config_task["bucket"],
        "source_objects": dag_config_task["source_objects"],
        "destination_project_dataset_table": dag_config_data["bq_table_name"],
        "source_format": dag_config_data["source_format"],
        "bigquery_conn_id": dag_config_data["bigquery_conn_id"],
        "google_cloud_storage_conn_id": dag_config_data["gcs_conn_id"],
        "time_partitioning": {'type': 'DAY',
                              'field': dag_config_data["time_partitioning"]}
    }

    if dag_config_task["file_type"] == "csv":
        update_config_csv = {
            "skip_leading_rows": 1,
            "field_delimiter": ","
        }
        dict_config_dag_operator.update(update_config_csv)
    return dict_config_dag_operator


def gcs_to_bq_job_operator(dict_config_dag_operator: dict
                           ) -> GCSToBigQueryOperator:
    """Take a dict and make a dag operator

    Arguments:
    dict_config_dag_operator -- dict with dag operator config

    Returns:
    google cloud storage to bigquery dag operator
    """
    return GCSToBigQueryOperator(**dict_config_dag_operator)


def create_dag(config_file: json) -> DAG:
    """Take a json and create a DAG

    Arguments:
    config_file -- json with all configurations to make a DAG

    Returns:
    DAG object to insert the gcs files into a bigquery table
    """
    general_config = config_file["general_config"]
    project_id = general_config["project_id"]
    dataset_id = general_config["dataset_id"]
    bucket_gcs = general_config["bucket_gcs"]
    dag_kwargs = parse_start_date(config_file["dag_kwargs"])
    config_gcs_to_bq_operator = config_file["config_gcs_to_bq_operator"]
    bigquery_conn_id = config_gcs_to_bq_operator["bigquery_conn_id"]
    gcs_conn_id = config_gcs_to_bq_operator["gcs_conn_id"]
    ingestion_tables = config_file["config_ingestion_tables"]

    with DAG(**dag_kwargs)as dag:
        start = DummyOperator(task_id="start")
        end = DummyOperator(task_id="end")
        task_dict = {}

        for ingestion_table in ingestion_tables:
            task_id = ingestion_table["task_name"]
            gcs_archive_name = ingestion_table["gcs_archive_name"]
            file_type = ingestion_table["file_type"]
            source_format_dag = ingestion_table["source_format_dag"]
            url_file = create_url_gcs(gcs_archive_name, file_type)
            bq_table_name = create_table_name(project_id,
                                              dataset_id,
                                              ingestion_table["bq_table_name"])
            partition_column = ingestion_table["partition_column"]
            dag_config_task = create_dag_config_task(task_id, bucket_gcs,
                                                     url_file, file_type)
            dag_config_data = create_dag_config_data(bq_table_name,
                                                     source_format_dag,
                                                     bigquery_conn_id,
                                                     partition_column,
                                                     gcs_conn_id)
            config_dag_operator = create_config_dag_operator(dag_config_task,
                                                             dag_config_data)
            task_dict[task_id] = gcs_to_bq_job_operator(config_dag_operator)

        start >> [task_dict["ingestion_pays_gcs_to_bq"],
                  task_dict["ingestion_prints_gcs_to_bq"],
                  task_dict["ingestion_taps_gcs_to_bq"]
                  ] >> end
    return dag


with open(PATH_DAG_CONFIG_JSON, "r", encoding="utf-8") as config_json_file:
    create_dag(json.load(config_json_file))

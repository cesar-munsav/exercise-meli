"""
This module define and configure an Airflow workflow (DAG)
to automate tasks related to the manipulation and creation
of data products in BigQuery (GCP).
Includes all necessary imports and configurations to make this task.
"""
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator)

# Vars
ENVIRONMENT = Variable.get("environment")
PATH_DAG_CONFIG = "path_dag_config_airflow_default"
PATH_SQL_FILE = "path_sql_airflow_default"
PATH_DAG_CONFIG_JSON = f"{PATH_DAG_CONFIG}/product_carousel_config.json"


# Functions
def parse_str_to_dt(str_date: str) -> datetime:
    """Take a string and format to date.

    Arguments:
    str_date -- string that contains a date

    Returns:
    date in format YYYY-mm-dd
    """
    return datetime.strptime(str_date, "%Y-%m-%d")


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


def parse_text_sql(bq_table_name: str, sql_parameters: dict) -> str:
    """Take a string and dict to format a string from a sql file

    Arguments:
    bq_table_name -- target bigquery table name
    sql_parameters -- sql parameters to format sql vars

    Returns:
    sql string to run a query in dag operator
    """
    sql_parameters["environment"] = ENVIRONMENT
    with open(f"{PATH_SQL_FILE}/{bq_table_name}.sql", "r",
              encoding="utf-8") as sql_file:
        return sql_file.read().format(**sql_parameters)


def job_operator_config(bq_table_name: str, sql_parameters: dict,
                        data_config: dict) -> dict:
    """Take a string and two dict to create a dict with job operator config

    Arguments:
    bq_table_name -- target bigquery table name
    sql_parameters -- sql parameters to format sql vars
    data_config -- data config with the project id and dataset id

    Returns:
    dict with job operator config
    """
    project_id = data_config["project_id"]
    dataset_id = data_config["dataset_id"]
    return {"query": {"query": parse_text_sql(bq_table_name, sql_parameters),
                      "destinationTable": {"projectId":
                                           f"{project_id}_{ENVIRONMENT}",
                                           "datasetId":
                                           f"{dataset_id}_{ENVIRONMENT}",
                                           "tableId": f"{bq_table_name}"
                                           }
                      }
            }


def bq_to_bq_job_operator(task_id: str, bq_table_name: str,
                          sql_parameters: dict, data_config: dict
                          ) -> BigQueryInsertJobOperator:
    """Take 2 string and 2 dict to create a BigQueryInsertJobOperator

    Arguments:
    task_id -- airflow task name
    bq_table_name -- target bigquery table name
    sql_parameters -- sql parameters to format sql vars
    data_config -- data config with the project id and dataset id

    Returns:
    BigQueryInsertJobOperator with all config to create a job operator
    """
    return BigQueryInsertJobOperator(task_id=task_id,
                                     configuration=job_operator_config(
                                          bq_table_name, sql_parameters,
                                          data_config),
                                     bigquery_conn_id=data_config[
                                          "bq_conn_id"]
                                     )


def add_task_sensors(dag_dependecy: str,
                     dag_timedelta_dependency: int) -> ExternalTaskSensor:
    """Take a string and int to create a airflow ExternalTaskSensor

    Arguments:
    dag_dependecy -- dag dependency airflow name that insert bigquery sources
    dag_timedelta_dependency -- time in minutes between dependency and dag

    Returns:
    ExternalTaskSensor with the sensor to run dag task in airflow
    """
    return ExternalTaskSensor(
        task_id=dag_dependecy,
        external_dag_id=dag_dependecy,
        execution_delta=timedelta(minutes=dag_timedelta_dependency),
        check_existence=True
                              )


def create_dag(config_file: json) -> DAG:
    """Take a json and create a DAG

    Arguments:
    config_file -- json with all configurations to create a DAG

    Returns:
    DAG object to run a query and insert result in a bigquery table
    """
    dag_kwargs = parse_start_date(config_file["dag_kwargs"])
    product_table_config = config_file["product_table_config"]
    data_config = config_file["data_config"]

    with DAG(**dag_kwargs)as dag:
        start = DummyOperator(task_id="start")
        end = DummyOperator(task_id="end")
        task_dict = {}

        task_id = product_table_config["task_name"]
        bq_table_name = product_table_config["bq_table_name"]
        dag_dependecy = product_table_config["dag_dependecy"]
        dag_timedelta_dependency = product_table_config[
            "dag_timedelta_dependency"]
        sql_parameters = product_table_config["sql_parameters"]
        task_dict[dag_dependecy] = add_task_sensors(dag_dependecy,
                                                    dag_timedelta_dependency)
        task_dict[task_id] = bq_to_bq_job_operator(task_id, bq_table_name,
                                                   sql_parameters, data_config)
        start >> task_dict[
            "ingestion-sources-carousel-mercadopago-cl"
            ] >> task_dict["product_carousel_bq"] >> end
    return dag


with open(PATH_DAG_CONFIG_JSON, "r", encoding="utf-8") as config_json_file:
    create_dag(json.load(config_json_file))

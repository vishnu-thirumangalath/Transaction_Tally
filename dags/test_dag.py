import os
import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule

PYSPARK_PACKAGES = (
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
    "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1"
)

with DAG(
    dag_id="test_dag",
    schedule_interval=None,
    start_date=None,
    catchup=False,
) as dag:

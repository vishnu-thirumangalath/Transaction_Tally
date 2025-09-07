from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess

# Config
NAMESPACE = "spark"  # 👈 change or parametrize
# SECRET_NAME = "flask-secrets"
SECRET_FILE = "/home/devopsadmin/workspace/helm/postgres/env_secrets.yaml"

def apply_manifests(namespace):
    # Apply namespace manifest (ignore error if exists)
    subprocess.run(["kubectl", "create", "ns", namespace], check=True)

    # Apply secret YAML into the namespace
    subprocess.run(["kubectl", "apply", "-f", SECRET_FILE, "-n", namespace], check=True)

with DAG(
    dag_id="transaction_tally_dag_dy",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Step 1: Apply namespace + secret
    apply_ns_secret = PythonOperator(
        task_id="apply_ns_secret",
        python_callable=apply_manifests,
        op_args=[NAMESPACE],  # 👈 namespace passed here
    )

    # Step 2: Run your Flask app pod
    run_python_app_dy = KubernetesPodOperator(
        task_id="run_transaction_tally_dy",
        name="transaction-tally_dy",
        namespace=NAMESPACE,
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/transaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=False,
        labels={"app": "transaction-tally"},
        env_from=[{"secretRef": {"name": "flask_secrets"}}],
    )

    # Step 3: Test pod to verify secrets
    flask_sensor_dy = KubernetesPodOperator(
        task_id="use_secrets_dy",
        name="flask-sensor_dy",
        namespace=NAMESPACE,
        image="alpine:3.18",
        cmds=["sh", "-c"],
        arguments=["echo DB=$POSTGRES_DB && echo KAFKA=$KAFKA_TOPIC"],
        get_logs=True,
        env_from=[{"secretRef": {"name": "flask_secrets"}}],
    )

    # DAG order
    apply_ns_secret >> [run_python_app_dy, flask_sensor_dy]

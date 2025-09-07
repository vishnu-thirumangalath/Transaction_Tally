from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.utils.dates import days_ago

# Secret: inject all keys from "flask-secrets" into env
env_secret = Secret(
    deploy_type="env",          # inject as environment vars
    deploy_target=None,         # None = all keys become env vars
    secret="flask-secrets"      # 👈 your secret name in namespace test
)

with DAG(
    dag_id="transaction_tally_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_python_app = KubernetesPodOperator(
        task_id="run_transaction_tally",
        name="transaction-tally",
        namespace="test",
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/transaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=True,
        labels={"app": "transaction-tally"},
        secrets=[env_secret],   # 👈 load env from flask-secrets
    )

    flask_sensor = KubernetesPodOperator(
        task_id="use_secrets",
        name="flask-sensor",
        namespace="test",
        image="alpine:3.18",
        cmds=["sh", "-c"],
        arguments=[
            "echo DB=$POSTGRES_DB && echo KAFKA=$KAFKA_TOPIC"
        ],
        get_logs=True,
        secrets=[env_secret],   # 👈 also inject here
    )

    [run_python_app, flask_sensor]

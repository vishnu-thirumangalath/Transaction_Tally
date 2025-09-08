from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

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
        labels={"app" : "transaction-tally", "type" : "test"},
        image="ghcr.io/vishnu-thirumangalath/docker-images/transaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=False,
        labels={"app": "transaction-tally"},
        env_from=[{"secretRef": {"name": "flask-secrets"}}],  # 👈 works across versions
    )

    flask_sensor = KubernetesPodOperator(
        task_id="use_secrets",
        name="flask-sensor",
        namespace="test",
        image="alpine:3.18",
        cmds=["sh", "-c"],
        arguments=["echo DB=$POSTGRES_DB && echo KAFKA=$KAFKA_TOPIC"],
        get_logs=True,
        env_from=[{"secretRef": {"name": "flask-secrets"}}],  # 👈 same here
    )

    flask_sensor >> run_python_app 

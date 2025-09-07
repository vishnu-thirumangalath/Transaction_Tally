from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.kubernetes.secret import Secret

# DAG definition
with DAG(
    dag_id="transaction_tally_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. Run Flask app pod (kept alive, task ends immediately after creation)
    run_python_app_old = KubernetesPodOperator(
        task_id="run_transaction_tally_old",
        name="transaction-tally",
        namespace="test",
        service_account_name="dagsvc",
        image="ghcr.io/vishnu-thirumangalath/docker-images/transaction-tally:latest",
        cmds=["python", "run.py"],
        get_logs=False,                 # don’t tail logs forever
        do_xcom_push=False,             # no log XCom
        is_delete_operator_pod=True,   # keep Flask alive after task ends
        labels={                        # add this block
            "app": "transaction-tally"
        },
        env_vars={
            "POSTGRES_HOST": "transaction-db-postgresql.test.svc.cluster.local",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "postgres",
            "POSTGRES_DB": "postgres",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka-kafka-bootstrap.kafka:9092",
            "KAFKA_TOPIC": "test-topic",
            "CHECKPOINT_LOC": "/tmp/flaskstream"

            # --- spark identity fixes --- 
            # "SPARK_LOCAL_HOSTNAME": "localhost", 
            # "SPARK_LOCAL_IP": "127.0.0.1", 
            # "SPARK_DRIVER_PORT": "7078", 
            # "SPARK_BLOCKMANAGER_PORT": "7079",
        },
        secrets=[
            Secret(
                deploy_type="env",
                deploy_target="POSTGRES_PASSWORD",
                secret="transaction-db-postgresql",
                key="postgres-password"
            )
        ]
    )

    # 2. Sensor pod (keeps checking Flask /health endpoint until ready)
    flask_sensor_old = KubernetesPodOperator(
        task_id="flask_sensor",
        name="flask-sensor-curl",
        namespace="test",
        service_account_name="dagsvc",
        image="curlimages/curl:8.2.1",
        cmds=["sh", "-c"],
        arguments=[
            """
            for i in $(seq 1 30); do
              echo "Checking Flask health... attempt $i";
              if curl -sf http://transaction-tally.test.svc.cluster.local:5000/health; then
                echo "service ok";
                exit 0;
              fi;
              sleep 5;
            done;
            echo "Flask did not become ready in time";
            exit 1
            """
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # DAG flow (sequential)
    [run_python_app_old,flask_sensor_old]

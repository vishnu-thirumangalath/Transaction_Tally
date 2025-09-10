import os
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, String
from app.utils.config import *
from pyspark.sql import SparkSession
from app.utils.functions import *
import uuid
from dotenv import load_dotenv
from pyspark.sql.functions import col, coalesce, lit, max as spark_max
from sqlalchemy import inspect
from sqlalchemy.sql import text

load_dotenv()

def load_json_query():
    PARENT_DIR = os.path.dirname(os.path.dirname(__file__))
    query_path = os.path.join(PARENT_DIR,"script", "json_query.py")
    print(query_path)
    namespace = {}
    with open(query_path) as f:
        exec(f.read(), namespace)
    return namespace.get("json_query", "")

def get_db_config():
    if TARGET_DB == "POSTGRES":
        return {
            "url": POSTGRES_URL,
            "engine_url": POSTGRES_CON_URL,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
            "package": "org.postgresql:postgresql:42.7.2",
            "main_table": POSTGRES_TABLE,
            "history_table": POSTGRES_HISTORY_TABLE,
            "pk": POSTGRESS_TABLE_PKID,
            "conn_params": POSTGRESS_CON
        }
    elif TARGET_DB == "MYSQL":
        return {
            "url": f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
            "engine_url": f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
            "user": MYSQL_USER,
            "password": MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver",
            "package": "mysql:mysql-connector-java:8.0.33",
            "main_table": MYSQL_TABLE,
            "history_table": MYSQL_HISTORY_TABLE,
            "pk": MYSQL_TABLE_PKID,
            "conn_params": {
                "host": MYSQL_HOST,
                "port": MYSQL_PORT,
                "user": MYSQL_USER,
                "password": MYSQL_PASSWORD,
                "database": MYSQL_DATABASE,
            }
        }

def create_spark_session():
    db_conf = get_db_config()
    return (
        SparkSession.builder
            .appName("KafkaToDB")
            .master("local[*]")   # ✅ Force local mode
            .config("spark.jars.packages",
                    f"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,{db_conf['package']}")
            # Optional: keep networking configs if you run spark-submit later
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
    )

def create_table_if_not_exists(df, table_name, pk_field, engine, include_version=False):
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if table_name in metadata.tables:
        return

    from sqlalchemy import String, Integer
    db_type = TARGET_DB.upper()
    columns = []

    for field in df.schema.fields:
        # add better type mapping here if needed
        if db_type == "MYSQL":
            columns.append(Column(field.name, String(255)))
        else:
            columns.append(Column(field.name, String))

    if include_version:
        if db_type == "MYSQL":
            columns.append(Column("version", String(10)))
        else:
            columns.append(Column("version", String))

    Table(table_name, metadata, *columns).create(bind=engine)

def read_table(spark, db_conf, table_name):
    return spark.read.format("jdbc") \
        .option("url", db_conf["url"]) \
        .option("dbtable", table_name) \
        .option("user", db_conf["user"]) \
        .option("password", db_conf["password"]) \
        .option("driver", db_conf["driver"]) \
        .load()

def quote_identifier(identifier: str, db_type: str) -> str:
    if db_type.upper() == "POSTGRES":
        return f'"{identifier}"'
    elif db_type.upper() == "MSSQL":
        return f'[{identifier}]'
    elif db_type.upper() == "MYSQL":
        return f'`{identifier}`'
    else:
        raise ValueError(f"Unsupported database type for quoting: {db_type}")

def write_df(df, db_conf, table_name, mode="append"):
    df.write.format("jdbc") \
        .option("url", db_conf["url"]) \
        .option("dbtable", table_name) \
        .option("user", db_conf["user"]) \
        .option("password", db_conf["password"]) \
        .option("driver", db_conf["driver"]) \
        .mode(mode) \
        .save()

def quote_identifier(identifier, db_type):
    if db_type == "POSTGRES":
        return f'"{identifier}"'
    elif db_type == "MYSQL":
        return f"`{identifier}`"
    elif db_type == "MSSQL":
        return f"[{identifier}]"
    return identifier

def get_changed_ids_and_filter(df, pk):
    """
    Extracts distinct primary key values (single or composite) from df and returns a filter function.

    Args:
        df: Spark DataFrame containing primary key columns.
        pk: str or list[str], primary key column(s).

    Returns:
        changed_ids: list of scalars (single PK) or list of tuples (composite PK).
        filter_func: callable that filters a DataFrame to only rows matching changed_ids.
    """
    # Convert single string pk to list for uniform handling
    if isinstance(pk, str):
        pk = [pk]

    if len(pk) == 1:
        pk_col = pk[0]
        changed_ids = df.select(pk_col).distinct().rdd.map(lambda row: row[0]).collect()

        def filter_func(target_df):
            return target_df.filter(col(pk_col).isin(changed_ids))

    else:
        changed_ids = df.select(*pk).distinct().rdd.map(lambda row: tuple(row)).collect()

        def filter_func(target_df):
            if not changed_ids:
                return target_df.filter(lit(False))  # Empty result
            
            # Build OR conditions for composite keys instead of using struct.isin()

            conditions = None
            for pk_values in changed_ids:
                condition = None
                for i, pk_col in enumerate(pk):
                    pk_condition = col(pk_col) == lit(pk_values[i])
                    condition = pk_condition if condition is None else condition & pk_condition
                conditions = condition if conditions is None else conditions | condition
            
            return target_df.filter(conditions)

    return changed_ids, filter_func

from pyspark.sql.functions import col, struct, coalesce, lit, max as spark_max
from sqlalchemy import create_engine, inspect

def writestream_non_empty_batches(batch_df, batch_id):
    print(f"\n🚀 Processing batch ID: {batch_id}")

    spark = batch_df.sparkSession
    db_conf = get_db_config()

    # Ensure PK is always a list for consistency (e.g., ['MsgId'] or ['MsgId', 'Sts'])
    pk = db_conf["pk"] if isinstance(db_conf["pk"], list) else [db_conf["pk"]]

    # Cast incoming value column to STRING (assuming JSON data)
    value_df = batch_df.selectExpr("CAST(value AS STRING) as value")
    if value_df.rdd.isEmpty():
        print(f"⚠️ Skipping empty batch (id: {batch_id})")
        return

    # Create temp view and run SQL extraction query on JSON
    value_df.createOrReplaceTempView("messageView")
    query_string = load_json_query()
    result_df = spark.sql(query_string)

    if result_df.rdd.isEmpty():
        print(f"⚠️ Extracted no records from JSON in batch {batch_id}.")
        return

    # Create DB engine and inspect tables
    engine = create_engine(db_conf["engine_url"])
    existing_tables = inspect(engine).get_table_names()

    if db_conf["main_table"] not in existing_tables:
        print(f"✅ Creating main table: {db_conf['main_table']}")
    else:
        print(f"✅ Table {db_conf['main_table']} already exists.")

    if db_conf["history_table"] not in existing_tables:
        print(f"✅ Creating history table: {db_conf['history_table']}")
    else:
        print(f"✅ Table {db_conf['history_table']} already exists.")

    # Create tables if not exist (you pass pk as list)
    create_table_if_not_exists(result_df, db_conf["main_table"], pk, engine)
    create_table_if_not_exists(result_df, db_conf["history_table"], pk, engine, include_version=True)

    try:
        # Read existing data from main table
        existing_df = read_table(spark, db_conf, db_conf["main_table"])

        # Extract existing PK values and get filter function
        existing_ids, existing_filter = get_changed_ids_and_filter(existing_df, pk)

        # Filter result_df into new and update sets using PK(s)
        if len(pk) == 1:
            new_records = result_df.filter(~col(pk[0]).isin(existing_ids))
            update_records = result_df.filter(col(pk[0]).isin(existing_ids))
        else:
            # Build conditions for composite keys instead of using struct.isin()

            if not existing_ids:
                new_records = result_df
                update_records = result_df.filter(lit(False))  # Empty result
            else:
                existing_conditions = None
                for pk_values in existing_ids:
                    condition = None
                    for i, pk_col in enumerate(pk):
                        pk_condition = col(pk_col) == lit(pk_values[i])
                        condition = pk_condition if condition is None else condition & pk_condition
                    existing_conditions = condition if existing_conditions is None else existing_conditions | condition
                
                new_records = result_df.filter(~existing_conditions)
                update_records = result_df.filter(existing_conditions)

        print(f"🆕 New records count: {new_records.count()}")
        print(f"♻️ Update records count: {update_records.count()}")

        # Write new records
        if not new_records.rdd.isEmpty():
            write_df(new_records, db_conf, db_conf["main_table"], mode="append")
            print(f"📥 Inserted {new_records.count()} new records into {db_conf['main_table']}.")

        # Process updated records
        if not update_records.rdd.isEmpty():
            if len(pk) == 1:
                join_cond = existing_df[pk[0]] == update_records[pk[0]]
                current_df = existing_df
                joined_df = current_df.alias("curr").join(update_records.alias("new"), join_cond, "inner")
                data_columns = [c for c in current_df.columns if c != pk[0]]
            else:
                current_struct_df = existing_df.withColumn("pk_struct", struct(*pk))
                update_struct_df = update_records.withColumn("pk_struct", struct(*pk))
                join_cond = [col(f"curr.{k}") == col(f"new.{k}") for k in pk]
                joined_df = current_struct_df.alias("curr").join(update_struct_df.alias("new"), join_cond, "inner")
                data_columns = [c for c in current_struct_df.columns if c not in pk + ["pk_struct"]]

            # For composite keys, always treat records with same key as changed (force versioning)
            # This ensures all records with matching composite keys get versioned to history
            if len(pk) == 1:
                changed_records = joined_df.select("curr.*")
            else:
                # For composite keys, explicitly select only the original columns (exclude pk_struct)
                original_columns = [f"curr.{c}" for c in existing_df.columns]
                changed_records = joined_df.select(*original_columns)

            print(f"📦 Changed record count: {changed_records.count()}")

            if not changed_records.rdd.isEmpty():

                # Archive old changed records into history with versioning
                history_df = read_table(spark, db_conf, db_conf["history_table"])
                max_versions_df = history_df.groupBy(*pk).agg(spark_max("version").alias("max_version"))

                versioned_old = changed_records.join(max_versions_df, pk, how="left") \
                    .withColumn("version", coalesce(col("max_version").cast("int"), lit(0)) + 1) \
                    .drop("max_version")

                versioned_old = versioned_old.dropDuplicates()
                write_df(versioned_old, db_conf, db_conf["history_table"], mode="append")
                print(f"📦 Archived {versioned_old.count()} changed records to history table.")

                # Prepare to delete old changed records from main table
                changed_ids, _ = get_changed_ids_and_filter(changed_records, pk)

                quoted_table = quote_identifier(db_conf["main_table"], TARGET_DB)
                if len(pk) == 1:
                    quoted_pk = quote_identifier(pk[0], TARGET_DB)
                    tuple_values = ", ".join([f"'{x}'" for x in changed_ids])
                    delete_query = f"DELETE FROM {quoted_table} WHERE {quoted_pk} IN ({tuple_values})"
                else:
                    quoted_pks = ", ".join([quote_identifier(k, TARGET_DB) for k in pk])
                    tuple_values = ", ".join(
                        ["(" + ", ".join([repr(x) for x in tup]) + ")" for tup in changed_ids]
                    )
                    delete_query = f"DELETE FROM {quoted_table} WHERE ({quoted_pks}) IN ({tuple_values})"

                # Execute delete query on target DB
                if TARGET_DB == "POSTGRES":
                    import psycopg2
                    conn = psycopg2.connect(**db_conf["conn_params"])
                elif TARGET_DB == "MSSQL":
                    import pyodbc
                    conn = pyodbc.connect(
                        f"DRIVER={db_conf['conn_params']['driver']};"
                        f"SERVER={db_conf['conn_params']['server']};"
                        f"DATABASE={db_conf['conn_params']['database']};"
                        f"UID={db_conf['conn_params']['uid']};"
                        f"PWD={db_conf['conn_params']['pwd']}"
                    )
                elif TARGET_DB == "MYSQL":
                    import mysql.connector
                    conn = mysql.connector.connect(**db_conf["conn_params"])
                else:
                    raise ValueError(f"Unsupported TARGET_DB {TARGET_DB}")

                cursor = conn.cursor()
                cursor.execute(delete_query)
                conn.commit()
                cursor.close()
                conn.close()
                print(f"🧹 Deleted old records from {db_conf['main_table']} for update.")

                # Filter update records down to changed IDs and write the updates
                _, update_filter_func = get_changed_ids_and_filter(update_records, pk)
                update_records_filtered = update_filter_func(update_records)

                write_df(update_records_filtered, db_conf, db_conf["main_table"], mode="append")
                print(f"🔄 Updated {update_records_filtered.count()} records in {db_conf['main_table']}.")

    except Exception as e:
        print(f"❌ Error in batch {batch_id}: {e}")

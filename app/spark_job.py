from app.utils.functions import *
load_dotenv()

def start_spark_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    db_conf = get_db_config()
    print("\n DB CONFIGURATION: ")
    print(db_conf)
    if SparkSession.getActiveSession():
        print("\n✅ Spark Session Is Active.")
    else:
        print("❌ No Active Spark Session Found.")

    print("\n🚀 Spark Streaming Job Started.")
    print(f"📡 Kafka-Topic: {KAFKA_TOPIC}")
    print(f"🗄️  Target DB : {TARGET_DB} - Target Trxn Table : {db_conf['main_table']} - "
          f"Target History Table : {db_conf['history_table']} ")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("multiLine", "true")
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("kafka.security.protocol", "PLAINTEXT")
        .load()
    )

    query = (
        kafka_df.writeStream
        .queryName("KafkaToDBStream")
        .foreachBatch(writestream_non_empty_batches)
        .option("checkpointLocation", CHECKPOINT_LOC)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()

def main():
    start_spark_stream()

if __name__ == '__main__':
    main()

from pyspark.sql import functions as F

landing_path = "/Volumes/real_estate/bronze/raw_files/landing/"
checkpoint_path = "/Volumes/real_estate/bronze/raw_files/_checkpoints/bronze_listings/"

target_schema = (
    spark.read.table("real_estate.bronze.raw_listings")
    .drop("ingestion_timestamp", "source_file", "_rescued_data")
    .schema
)

df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", "\\")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .schema(target_schema)
    .load(landing_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
)

query = (
    df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("real_estate.bronze.raw_listings")
)

query.awaitTermination()
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_bronze = spark.read.table("real_estate.bronze.raw_listings")

window_spec = Window.partitionBy("listing_id").orderBy(
    F.col("ingestion_timestamp").desc(),
    F.col("scraped_at").desc()
)

df_latest = (
    df_bronze
    .filter(F.col("listing_id").isNotNull())
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

df_incoming = (
    df_latest
    .withColumn("price_egp_clean", F.expr("try_cast(regexp_replace(price_egp, '[^0-9.]', '') as double)"))
    .withColumn("area_clean", F.expr("try_cast(area_value as double)"))
    .withColumn("bedrooms_clean", F.expr("try_cast(regexp_replace(bedrooms, '[^0-9]', '') as int)"))
    .withColumn("bathrooms_clean", F.expr("try_cast(regexp_replace(bathrooms, '[^0-9]', '') as int)"))
    .withColumn("lat_clean", F.expr("try_cast(lat as double)"))
    .withColumn("lon_clean", F.expr("try_cast(lon as double)"))
    .withColumn("listed_date_clean", F.expr("try_cast(listed_date as date)"))
    .withColumn("scraped_at_ts", F.expr("try_cast(scraped_at as timestamp)"))
    .filter((F.col("price_egp_clean") > 0) & (F.col("area_clean") >= 30))
    .withColumn("is_shifted", F.col("scraped_at_ts").isNull())
    .withColumn("payment_method", F.when(F.col("is_shifted"), F.lit(None)).otherwise(F.col("payment_method")))
    .withColumn("broker_name", F.when(F.col("is_shifted"), F.lit(None)).otherwise(F.col("broker_name")))
    .withColumn("agent_name", F.when(F.col("is_shifted"), F.lit(None)).otherwise(F.col("agent_name")))
    .withColumn("scraped_at", F.when(F.col("is_shifted"), F.col("ingestion_timestamp").cast("string")).otherwise(F.col("scraped_at")))
    .withColumn("price_per_sqm", F.round(F.col("price_egp_clean") / F.col("area_clean"), 2))
    .select(
        "listing_id",
        "title",
        "detail_url",
        "category",
        "property_type",
        "offering_type",
        "completion_status",
        F.col("price_egp_clean").alias("price_egp"),
        "price_period",
        F.col("area_clean").alias("area_value"),
        "area_unit",
        "price_per_sqm",
        F.col("bedrooms_clean").alias("bedrooms"),
        F.col("bathrooms_clean").alias("bathrooms"),
        "furnished",
        "city",
        "town",
        "district",
        "subdistrict",
        F.col("lat_clean").alias("lat"),
        F.col("lon_clean").alias("lon"),
        "payment_method",
        "broker_name",
        "agent_name",
        F.col("listed_date_clean").alias("listed_date"),
        "scraped_at",
        "ingestion_timestamp"
    )
)

silver_table = DeltaTable.forName(spark, "real_estate.silver.cleaned_listings")
df_target_current = silver_table.toDF().filter(F.col("is_current") == True)

staged_updates = (
    df_incoming.alias("new")
    .join(df_target_current.alias("old"), "listing_id", "inner")
    .filter(
        (F.col("new.price_egp") != F.col("old.price_egp")) |
        (F.col("new.area_value") != F.col("old.area_value"))
    )
    .select("new.*")
    .withColumn("mergeKey", F.lit(None).cast("string"))
)

staged_all = (
    df_incoming.withColumn("mergeKey", F.col("listing_id"))
    .unionByName(staged_updates)
)

(
    silver_table.alias("target")
    .merge(
        staged_all.alias("source"),
        "target.listing_id = source.mergeKey AND target.is_current = true"
    )
    .whenMatchedUpdate(
        condition="target.price_egp != source.price_egp OR target.area_value != source.area_value",
        set={
            "is_current": "false",
            "effective_to": "source.ingestion_timestamp"
        }
    )
    .whenNotMatchedInsert(
        values={
            "listing_id": "source.listing_id",
            "title": "source.title",
            "detail_url": "source.detail_url",
            "category": "source.category",
            "property_type": "source.property_type",
            "offering_type": "source.offering_type",
            "completion_status": "source.completion_status",
            "price_egp": "source.price_egp",
            "price_period": "source.price_period",
            "area_value": "source.area_value",
            "area_unit": "source.area_unit",
            "price_per_sqm": "source.price_per_sqm",
            "bedrooms": "source.bedrooms",
            "bathrooms": "source.bathrooms",
            "furnished": "source.furnished",
            "city": "source.city",
            "town": "source.town",
            "district": "source.district",
            "subdistrict": "source.subdistrict",
            "lat": "source.lat",
            "lon": "source.lon",
            "payment_method": "source.payment_method",
            "broker_name": "source.broker_name",
            "agent_name": "source.agent_name",
            "listed_date": "source.listed_date",
            "scraped_at": "source.scraped_at",
            "ingestion_timestamp": "source.ingestion_timestamp",
            "is_current": "true",
            "effective_from": "source.ingestion_timestamp",
            "effective_to": "null"
        }
    )
    .execute()
)

df_check = spark.read.table("real_estate.silver.cleaned_listings")
print(f"Current active listings: {df_check.filter(F.col('is_current') == True).count()}")
print(f"Historical (closed) rows: {df_check.filter(F.col('is_current') == False).count()}")

# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data
# MAGIC
# MAGIC **source**:  Airbnb data for London and Tokyo at http://insideairbnb.com/get-the-data

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to bronze

# COMMAND ----------

import io
import pandas as pd
import requests

def load_data_from_url_as_spark_df(url, **params):
    """Loads data from url. Optional keyword arguments are passed to pandas.read_csv."""
    response = requests.get(url)
    dx = pd.read_csv(io.BytesIO(response.content), **params)  
    return spark.createDataFrame(dx)

# COMMAND ----------

import json 
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, TimestampType
from delta.tables import DeltaTable

def update_schema_with_metadata_fields(schema):
    """Helper method to add metadata fields to contact schema."""
    return schema\
            .add("processing_datetime", TimestampType(), True)\
            .add("area", StringType(), True)
        
def add_metadata_columns(df, area: str):
    """Helper method to add metadata columns to dataframe."""
    return (
        df.withColumn("processing_datetime", f.current_timestamp())
        .withColumn("area", f.lit(area))
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Load Listings

# COMMAND ----------

# load contract schema 
with open(contracts_path + "/listing_schema.json", "r") as fin:
    listing_schema = StructType.fromJson(json.loads(fin.read()))

# update contract schema with metadata fields
raw_listing_schema = update_schema_with_metadata_fields(listing_schema)

# initialize delta table
listing_delta = (
    DeltaTable.createIfNotExists(spark)
    .addColumns(raw_listing_schema)
    .tableName('airbnb.raw.london_listings')
).execute()

# initialize delta table
listing_delta = (
    DeltaTable.createIfNotExists(spark)
    .addColumns(raw_listing_schema)
    .tableName('airbnb.raw.tokyo_listings')
).execute()

# COMMAND ----------

listings_url_london = "https://data.insideairbnb.com/united-kingdom/england/london/2025-06-10/data/listings.csv.gz"

listings_df_london = load_data_from_url_as_spark_df(
    listings_url_london, sep=',', index_col=0, quotechar='"', compression='gzip'
)

listings_url_tokyo = "https://data.insideairbnb.com/japan/kant%C5%8D/tokyo/2025-06-27/data/listings.csv.gz"

listings_df_tokyo = load_data_from_url_as_spark_df(
    listings_url_tokyo, sep=',', index_col=0, quotechar='"', compression='gzip'
)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

def ensure_types(df):
    cast_map = {
        "license": StringType(),
        "maximum_minimum_nights": LongType(),
        "minimum_minimum_nights": LongType(),
        "minimum_maximum_nights": LongType(),
        "maximum_maximum_nights": LongType(),
    }

    for col_name, col_type in cast_map.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))

    return df

_ = (
    ensure_types(listings_df_london)
    .transform(lambda x: add_metadata_columns(x, "London"))
    .writeTo("airbnb.raw.london_listings")
    .append()
)

_ = (
    ensure_types(listings_df_tokyo)
    .transform(lambda x: add_metadata_columns(x, "Tokyo"))
    .writeTo("airbnb.raw.tokyo_listings")
    .append()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Move to silver

# COMMAND ----------

# Create schema 'silver'
spark.sql("CREATE SCHEMA IF NOT EXISTS airbnb.silver")

# Copy london_listings to silver
spark.sql("""
  CREATE TABLE IF NOT EXISTS airbnb.silver.london_listings
  AS SELECT * FROM airbnb.raw.london_listings
""")

# Copy tokyo_listings to silver
spark.sql("""
  CREATE TABLE IF NOT EXISTS airbnb.silver.tokyo_listings
  AS SELECT * FROM airbnb.raw.tokyo_listings
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data cleansing

# COMMAND ----------

london_df = spark.read.table('airbnb.silver.london_listings')
display(london_df)

# COMMAND ----------

tokyo_df = spark.read.table('airbnb.silver.tokyo_listings')
display(tokyo_df)

# COMMAND ----------

combined_df = london_df.unionByName(tokyo_df)
display(combined_df)

# COMMAND ----------

columns_to_drop = ["listing_url", "scrape_id", "last_scraped", "source", "name", "description", "neighborhood_overview", "picture_url", "host_url", "host_name", "host_about", "host_thumbnail_url", "host_picture_url", "neighbourhood", "calendar_last_scraped", "processing_datetime", "host_location", "host_since", "first_review", "last_review"]
combined_df_dropped = combined_df.drop(*columns_to_drop)
display(combined_df_dropped)

# COMMAND ----------

import pyspark.sql.functions as F

null_counts = combined_df_dropped.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in combined_df_dropped.columns])
display(null_counts)

# COMMAND ----------

display(
    combined_df_dropped
    .groupBy("area", "estimated_revenue_l365d")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see almost all nulls are in London. It means that we need to drop this column because it can't give us information for comparison.

# COMMAND ----------

combined_df_dropped = combined_df_dropped.drop("estimated_revenue_l365d")

# COMMAND ----------

# MAGIC %md
# MAGIC 800k of all rows from license column are nulls so we decided to drop it

# COMMAND ----------

combined_df_dropped = combined_df_dropped.drop("license")

# COMMAND ----------

display(
    combined_df_dropped
    .groupBy("area", "has_availability")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC We filled nulls with false. 

# COMMAND ----------

from pyspark.sql.functions import when, col

combined_df_dropped = combined_df_dropped.withColumn(
    "has_availability",
    when(col("has_availability") == "t", True).otherwise(False)
)

# COMMAND ----------

combined_df_dropped = combined_df_dropped.drop("calendar_updated")

# COMMAND ----------

combined_df_dropped = combined_df_dropped.filter(col("maximum_maximum_nights").isNotNull())

# COMMAND ----------

display(
    combined_df_dropped
    .groupBy("area", "bedrooms")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

london_df.count()

# COMMAND ----------

tokyo_df.count()

# COMMAND ----------

combined_df_dropped = combined_df_dropped.drop("neighbourhood_group_cleansed")

# COMMAND ----------

combined_df_dropped = combined_df_dropped.drop("host_neighbourhood")

# COMMAND ----------

display(
    combined_df_dropped
    .groupBy("area", "host_identity_verified")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

combined_df_dropped = combined_df_dropped.withColumn(
    "host_identity_verified",
    when(col("host_identity_verified") == "t", True).otherwise(False)
)

# COMMAND ----------

combined_df_dropped = combined_df_dropped.withColumn(
    "host_has_profile_pic",
    when(col("host_has_profile_pic") == "t", True).otherwise(False)
)

# COMMAND ----------

combined_df_dropped = combined_df_dropped.withColumn(
    "host_is_superhost",
    when(col("host_is_superhost") == "t", True).otherwise(False)
)

# COMMAND ----------

combined_df_dropped = combined_df_dropped.drop("amenities")

# COMMAND ----------

import pyspark.sql.functions as F

null_counts = combined_df_dropped.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in combined_df_dropped.columns])
display(null_counts)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, when, col, lower, trim, lit

combined_df_dropped = combined_df_dropped.withColumn(
    "bathrooms",
    when(
        lower(trim(col("bathrooms_text"))).like("%half%"),
        lit(0.5)
    ).otherwise(
        regexp_extract(
            col("bathrooms_text"),
            r"(\d+(\.\d+)?)",
            1
        ).cast("double")
    )
)

display(combined_df_dropped.select("bathrooms_text", "bathrooms"))

# COMMAND ----------

display(combined_df_dropped)

# COMMAND ----------

combined_df_dropped = combined_df_dropped.withColumn(
    "instant_bookable",
    when(col("instant_bookable") == "t", True).otherwise(False)
)

# COMMAND ----------

from pyspark.sql.functions import col, translate

fixed_price_df = combined_df_dropped.withColumn("price", translate(col("price"), "$,", "").cast("double"))

display(fixed_price_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

fixed_price_df = fixed_price_df.withColumn(
    "host_response_percentage",
    regexp_replace(col("host_response_rate"), "%$", "").cast("double")
)

display(combined_df_dropped)

# COMMAND ----------

fixed_price_df = fixed_price_df.withColumn(
    "host_acceptance_percentage",
    regexp_replace(col("host_acceptance_rate"), "%$", "").cast("double")
)

# COMMAND ----------

fixed_price_df = fixed_price_df.drop("host_response_rate")
fixed_price_df = fixed_price_df.drop("host_acceptance_rate")

# COMMAND ----------

# Create schema 'silver'
spark.sql("CREATE SCHEMA IF NOT EXISTS airbnb.gold")

# COMMAND ----------

(
    fixed_price_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("airbnb.gold.combined_listings")
)


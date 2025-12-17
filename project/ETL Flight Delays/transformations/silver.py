from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, to_date, regexp_replace, upper,
    coalesce, lit
)

@dp.table(
    name="airport_delays.silver.silver_enroute_daily",
    comment="Cleaned daily en-route delays by ANSP/entity"
)
def silver_enroute_daily():
    df = spark.read.table("airport_delays.raw.enroute_dly")

    dly_cols = [c for c in df.columns if c.startswith("DLY_ERT_")]

    return (
        df
        .withColumn(
            "flt_date",
            to_date(regexp_replace(col("FLT_DATE"), "T.*", ""), "yyyy-MM-dd")
        )
        .select(
            "YEAR", "MONTH_NUM", "MONTH_MON",
            "flt_date",
            "ENTITY_NAME", "ENTITY_TYPE",
            "FLT_ERT_1", "FLT_ERT_1_DLY", "FLT_ERT_1_DLY_15",
            *[coalesce(col(c), lit(0.0)).alias(c) for c in dly_cols]
        )
    )

@dp.table(
    name="airport_delays.silver.silver_airport_arrival_daily",
    comment="Cleaned daily airport arrival delays"
)
def silver_airport_arrival_daily():
    df = spark.read.table("airport_delays.raw.apt_dly")

    delay_cols = [c for c in df.columns if c.startswith("DLY_APT_ARR_")]

    return (
        df
        .withColumn(
            "flt_date",
            to_date(regexp_replace(col("FLT_DATE"), "T.*", ""), "yyyy-MM-dd")
        )
        .withColumn("apt_icao", upper(col("APT_ICAO")))
        .select(
            "YEAR", "MONTH_NUM", "MONTH_MON",
            "flt_date", "apt_icao",
            "APT_NAME", "STATE_NAME",
            "FLT_ARR_1", "FLT_ARR_1_DLY", "FLT_ARR_1_DLY_15",
            *[coalesce(col(c), lit(0.0)).alias(c) for c in delay_cols]
        )
    )

@dp.table(
    name="airport_delays.silver.silver_airport_atc_departure_daily",
    comment="Cleaned daily ATC pre-departure delays per airport"
)
def silver_airport_atc_departure_daily():
    return (
        spark.read.table("airport_delays.raw.atc_pre_dep")
        .withColumn(
            "flt_date",
            to_date(col("FLT_DATE"), "yyyy-MM-dd")
        )
        .withColumn("apt_icao", upper(col("APT_ICAO")))
        .select(
            "YEAR", "MONTH_NUM", "MONTH_MON",
            "flt_date", "apt_icao",
            "APT_NAME", "STATE_NAME",
            "FLT_DEP_1",
            coalesce(col("FLT_DEP_IFR_2"), lit(0)).alias("flt_dep_ifr"),
            coalesce(col("DLY_ATC_PRE_2"), lit(0.0)).alias("dly_atc_pre_min")
        )
    )

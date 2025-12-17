from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, sum as _sum

@dp.table(
    name="airport_delays.gold.gold_airport_arrival_delay_reason",
    comment="Arrival delays normalized by delay reason"
)
def gold_airport_arrival_delay_reason():
    return (
        spark.read.table("airport_delays.silver.silver_airport_arrival_daily")
        .select(
            "*",
            expr("""
              stack(
                17,
                'A', DLY_APT_ARR_A_1,
                'C', DLY_APT_ARR_C_1,
                'D', DLY_APT_ARR_D_1,
                'E', DLY_APT_ARR_E_1,
                'G', DLY_APT_ARR_G_1,
                'I', DLY_APT_ARR_I_1,
                'M', DLY_APT_ARR_M_1,
                'N', DLY_APT_ARR_N_1,
                'O', DLY_APT_ARR_O_1,
                'P', DLY_APT_ARR_P_1,
                'R', DLY_APT_ARR_R_1,
                'S', DLY_APT_ARR_S_1,
                'T', DLY_APT_ARR_T_1,
                'V', DLY_APT_ARR_V_1,
                'W', DLY_APT_ARR_W_1,
                'NA', DLY_APT_ARR_NA_1
              ) as (delay_reason, delay_minutes)airport_delays.gold.gold_airport_arrival_delay_reason
            """)
        )
        .filter(col("delay_minutes") > 0)
    )

@dp.table(
    name="airport_delays.gold.gold_airport_arrival_kpis_monthly",
    comment="Monthly arrival delay KPIs per airport"
)
def gold_airport_arrival_kpis_monthly():
    return (
        spark.read.table("airport_delays.silver.silver_airport_arrival_daily")
        .groupBy(
            "YEAR", "MONTH_NUM", "apt_icao", "APT_NAME", "STATE_NAME"
        )
        .agg(
            _sum("FLT_ARR_1").alias("total_arrivals"),
            _sum("FLT_ARR_1_DLY").alias("delayed_arrivals"),
            _sum("FLT_ARR_1_DLY_15").alias("delays_over_15min")
        )
    )

@dp.table(
    name="airport_delays.gold.gold_airport_arrival_kpis_daily",
    comment="Daily arrival delay KPIs per airport"
)
def gold_airport_arrival_kpis_daily():
    df = spark.read.table("airport_delays.silver.silver_airport_arrival_daily")
    return (
        df
        .select(
            "YEAR", "MONTH_NUM", "MONTH_MON", "flt_date",
            "apt_icao", "APT_NAME", "STATE_NAME",
            col("FLT_ARR_1").alias("total_arrivals"),
            col("FLT_ARR_1_DLY").alias("delayed_arrivals"),
            col("FLT_ARR_1_DLY_15").alias("delays_over_15min"),
            (col("FLT_ARR_1_DLY") / col("FLT_ARR_1")).alias("delay_rate")
        )
        .filter(col("total_arrivals") > 0)
    )

@dp.table(
    name="airport_delays.gold.gold_enroute_kpis_daily",
    comment="Daily en-route delay KPIs per ANSP/entity"
)
def gold_enroute_kpis_daily():
    df = spark.read.table("airport_delays.silver.silver_enroute_daily")
    return (
        df
        .select(
            "YEAR","MONTH_NUM","MONTH_MON","flt_date",
            "ENTITY_NAME","ENTITY_TYPE",
            col("FLT_ERT_1").alias("total_flights"),
            col("FLT_ERT_1_DLY").alias("delayed_flights"),
            col("FLT_ERT_1_DLY_15").alias("delays_over_15min"),
            (col("FLT_ERT_1_DLY") / col("FLT_ERT_1")).alias("delay_rate")
        )
        .filter(col("total_flights") > 0)
    )

@dp.table(
    name="airport_delays.gold.gold_enroute_kpis_monthly",
    comment="Monthly en-route delay KPIs per ANSP/entity"
)
def gold_enroute_kpis_monthly():
    df = spark.read.table("airport_delays.silver.silver_enroute_daily")
    return (
        df
        .groupBy("YEAR","MONTH_NUM","ENTITY_NAME","ENTITY_TYPE")
        .agg(
            _sum("FLT_ERT_1").alias("total_flights"),
            _sum("FLT_ERT_1_DLY").alias("delayed_flights"),
            _sum("FLT_ERT_1_DLY_15").alias("delays_over_15min")
        )
    )

@dp.table(
    name="airport_delays.gold.gold_enroute_delay_by_reason",
    comment="En-route delay minutes by entity, month, and reason"
)
def gold_enroute_delay_by_reason():
    df = spark.read.table("airport_delays.silver.silver_enroute_daily")
    return (
        df
        .select(
            "YEAR","MONTH_NUM","ENTITY_NAME",
            expr("""
                stack(
                  17,
                  'A', DLY_ERT_A_1,'C', DLY_ERT_C_1,'D', DLY_ERT_D_1,'E', DLY_ERT_E_1,
                  'G', DLY_ERT_G_1,'I', DLY_ERT_I_1,'M', DLY_ERT_M_1,'N', DLY_ERT_N_1,
                  'O', DLY_ERT_O_1,'P', DLY_ERT_P_1,'R', DLY_ERT_R_1,'S', DLY_ERT_S_1,
                  'T', DLY_ERT_T_1,'V', DLY_ERT_V_1,'W', DLY_ERT_W_1,'NA', DLY_ERT_NA_1
                ) as (delay_reason, delay_minutes)
            """)
        )
        .filter(col("delay_minutes") > 0)
    )

@dp.table(
    name="airport_delays.gold.gold_airport_atc_departure_kpis_daily",
    comment="Daily ATC pre-departure delay KPIs per airport"
)
def gold_airport_atc_departure_kpis_daily():
    df = spark.read.table("airport_delays.silver.silver_airport_atc_departure_daily")
    return (
        df
        .select(
            "YEAR","MONTH_NUM","MONTH_MON","flt_date",
            "apt_icao","APT_NAME","STATE_NAME",
            col("FLT_DEP_1").alias("total_departures"),
            col("flt_dep_ifr").alias("ifr_departures"),
            col("dly_atc_pre_min").alias("atc_pre_delay_minutes"),
            (col("dly_atc_pre_min") / col("FLT_DEP_1")).alias("avg_delay_min_per_flight")
        )
        .filter(col("total_departures") > 0)
    )

@dp.table(
    name="airport_delays.gold.gold_airport_atc_departure_kpis_monthly",
    comment="Monthly ATC pre-departure delay KPIs per airport"
)
def gold_airport_atc_departure_kpis_monthly():
    df = spark.read.table("airport_delays.silver.silver_airport_atc_departure_daily")
    return (
        df
        .groupBy("YEAR","MONTH_NUM","apt_icao","APT_NAME","STATE_NAME")
        .agg(
            _sum("FLT_DEP_1").alias("total_departures"),
            _sum("flt_dep_ifr").alias("ifr_departures"),
            _sum("dly_atc_pre_min").alias("atc_pre_delay_minutes")
        )
    )

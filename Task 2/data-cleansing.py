# Databricks notebook source
# DBTITLE 0,--i18n-8c6d3ef3-e44b-4292-a0d3-1aaba0198525
# MAGIC %md 
# MAGIC
# MAGIC # Data Exploration 
# MAGIC
# MAGIC Our primary goal at this stage is to conduct data exploration to identify and resolve potential issues with the data, such as missing values, and anomalies. 
# MAGIC
# MAGIC Please run <code>load_athens_airbnb_data</code> notebook to get dataset into <code>airbnb.raw</code> schema

# COMMAND ----------

# DBTITLE 0,--i18n-969507ea-bffc-4255-9a99-2306a594625f
# MAGIC %md 
# MAGIC
# MAGIC ## Load Listings Dataset
# MAGIC
# MAGIC Let's load the Airbnb Athens listing dataset in.

# COMMAND ----------


raw_df = spark.read.table('airbnb.raw.listings')
display(raw_df)

# COMMAND ----------

raw_df.columns

# COMMAND ----------

# DBTITLE 0,--i18n-94856418-c319-4915-a73e-5728fcd44101
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC For the sake of simplicity, only keep certain columns from this dataset.

# COMMAND ----------

columns_to_keep = [
    'host_id','host_since','host_is_superhost','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights', 'number_of_reviews','review_scores_rating','license','instant_bookable','reviews_per_month'
]

base_df = raw_df.select(columns_to_keep)
display(base_df)

# COMMAND ----------

# DBTITLE 0,--i18n-a12c5a59-ad1c-4542-8695-d822ec10c4ca
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC  
# MAGIC ## Fixing Data Types
# MAGIC
# MAGIC Take a look at the schema above. You'll notice that the **`price`** field got picked up as string. For our task, we need it to be a numeric (double type) field. 
# MAGIC
# MAGIC Let's fix that.

# COMMAND ----------

# MAGIC %md
# MAGIC Checks if all values in numeric columns can be casted into double

# COMMAND ----------

print(base_df.dtypes)

from pyspark.sql.functions import col
from pyspark.sql.functions import expr

numeric_columns = [
    'latitude', 'longitude', 'accommodates', 'bathrooms', 'bedrooms', 'beds',
    'minimum_nights', 'maximum_nights', 'number_of_reviews',
    'review_scores_rating', 'reviews_per_month'
]

for col_name in numeric_columns:
    display(
        base_df
        .filter(
            expr(f"{col_name} IS NOT NULL AND try_cast({col_name} AS DOUBLE) IS NULL")
        )
        .select(col_name)
        .distinct()
    )

# COMMAND ----------

from pyspark.sql.functions import col, translate

fixed_price_df = base_df.withColumn("price", translate(col("price"), "$,", "").cast("double"))

display(fixed_price_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4ad08138-4563-4a93-b038-801832c9bc73
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Summary statistics
# MAGIC
# MAGIC Two options:
# MAGIC * **`describe`**: count, mean, stddev, min, max
# MAGIC * **`summary`**: describe + interquartile range (IQR)
# MAGIC
# MAGIC **Question:** When to use IQR/median over mean? Vice versa?

# COMMAND ----------

display(fixed_price_df.describe())

# COMMAND ----------

display(fixed_price_df.summary())

# COMMAND ----------

# DBTITLE 0,--i18n-bd55efda-86d0-4584-a6fc-ef4f221b2872
# MAGIC %md 
# MAGIC
# MAGIC ### Explore Dataset with Data Profile
# MAGIC
# MAGIC The **Data Profile** feature in Databricks notebooks offers valuable insights and benefits for data analysis and exploration. By leveraging Data Profile, users gain a comprehensive overview of their **dataset's characteristics, statistics, and data quality metrics**. This feature enables data scientists and analysts to understand the data distribution, identify missing values, detect outliers, and explore descriptive statistics efficiently.
# MAGIC
# MAGIC There are two ways of viewing Data Profiler. The first option is the UI.
# MAGIC
# MAGIC - After using `display` function to show a data frame, click **+** icon next to the *Table* in the header. 
# MAGIC - Click **Data Profile**. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC This functionality is also available through the dbutils API in Python, Scala, and R, using the dbutils.data.summarize(df) command. We can also use **`dbutils.data.summarize(df)`** to display Data Profile UI.
# MAGIC
# MAGIC Note that this features will profile the entire data set in the data frame or SQL query results, not just the portion displayed in the table

# COMMAND ----------

dbutils.data.summarize(fixed_price_df)

# COMMAND ----------

# DBTITLE 0,--i18n-e9860f92-2fbe-4d23-b728-678a7bb4734e
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Extreme values
# MAGIC
# MAGIC Let's take a look at the *min* and *max* values of the **`price`** column.

# COMMAND ----------

display(fixed_price_df.select('price'))

# COMMAND ----------

# DBTITLE 0,--i18n-4a8fe21b-1dac-4edf-a0a3-204f170b05c9
# MAGIC %md 
# MAGIC There are some super-expensive listings, and it's up to the Subject Matter Experts to decide what to do with them.
# MAGIC Let's see first how many listings we can find where the *price* is extream.

# COMMAND ----------

fixed_price_df.filter(col("price") >= 10000).count()

# COMMAND ----------

display(fixed_price_df.select("price").describe())

# COMMAND ----------

# DBTITLE 0,--i18n-bf195d9b-ea4d-4a3e-8b61-372be8eec327
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC We have 1 listing that has extream values and lets remove it.

# COMMAND ----------

pos_prices_df = fixed_price_df.filter(col("price") < 10000)

# COMMAND ----------

# DBTITLE 0,--i18n-dc8600db-ebd1-4110-bfb1-ce555bc95245
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC Let's take a look at the *min* and *max* values of the *minimum_nights* column:

# COMMAND ----------

display(pos_prices_df)

# COMMAND ----------

display(pos_prices_df.select("minimum_nights").describe())

# COMMAND ----------

display(pos_prices_df
        .groupBy("minimum_nights").count()
        .orderBy(col("count").desc(), col("minimum_nights"))
       )

# COMMAND ----------

# DBTITLE 0,--i18n-5aa4dfa8-d9a1-42e2-9060-a5dcc3513a0d
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC A minimum stay of 90 days seems to be a reasonable limit here due to . Let's filter out those records where the *minimum_nights* is greater than 90.

# COMMAND ----------

min_nights_df = pos_prices_df.filter(col("minimum_nights") <= 90)

display(min_nights_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ## Fix bathrooms
# MAGIC
# MAGIC It seems like bathroom column contains null values and information regarding bathrooms counts contained in bathrooms_text column.

# COMMAND ----------

display(min_nights_df.select('bathrooms', 'bathrooms_text'))

# COMMAND ----------

display(min_nights_df
        .groupBy("bathrooms_text").count()
        .orderBy(col("count").desc(), col("bathrooms_text"))
       )

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, when, col, lower, trim, lit

min_nights_df = min_nights_df.withColumn(
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

display(min_nights_df.select("bathrooms_text", "bathrooms"))

# COMMAND ----------

display(min_nights_df
        .groupBy("bathrooms").count()
        .orderBy(col("count").desc(), col("bathrooms"))
       )

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ## Fix boolean columns types 
# MAGIC
# MAGIC There are two columns `host_is_superhost` and `instant_bookable` that potentially may be usefull, but they are encoded as 't' or 'f' values.
# MAGIC Lets change it to boolean type. 
# MAGIC

# COMMAND ----------

boolean_df = (
  min_nights_df
  .withColumn('instant_bookable', col('instant_bookable') == 't')
  .withColumn('host_is_superhost', col('host_is_superhost') == 't')
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ## Fix amenities
# MAGIC
# MAGIC We can notice that `amenities` column contains a list of available items but it looks like string list which is not easy to work with. 
# MAGIC PySpark has a number of build-in methods to work with arrays: __array_*__
# MAGIC Lets convert amenities into propper aray of strings column.

# COMMAND ----------

from pyspark.sql.functions import explode, lower, split, translate, col

amenities_df = boolean_df.withColumn(
    'amenities',
    split(
        translate(col('amenities'), '\\][\\"', ''),
        ','
    )
)

display(
    amenities_df
    .select(explode(col('amenities')).alias('item'))
    .withColumn('item', lower(col('item')))
    .groupBy('item').count()
    .sort('count')
)

# COMMAND ----------

# DBTITLE 0,--i18n-25a35390-d716-43ad-8f51-7e7690e1c913
# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Handling Null Values
# MAGIC
# MAGIC There are a lot of different ways to handle null values. Sometimes, null can actually be a key indicator of the thing you are trying to predict (e.g. if you don't fill in certain portions of a form, probability of it getting approved decreases).
# MAGIC
# MAGIC Some ways to handle nulls:
# MAGIC * Drop any records that contain nulls
# MAGIC * Numeric:
# MAGIC   * Replace them with mean/median/zero/etc.
# MAGIC * Categorical:
# MAGIC   * Replace them with the mode
# MAGIC   * Create a special category for null
# MAGIC   
# MAGIC **If you do ANY imputation techniques for categorical/numerical features, you MUST include an additional field specifying that field was imputed.**

# COMMAND ----------

from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import NumericType

df_fixed = amenities_df
for c, dtype in amenities_df.dtypes:
    if isinstance(amenities_df.schema[c].dataType, NumericType):
        df_fixed = df_fixed.withColumn(
            c,
            when(col(c).isNull(), lit(float("nan"))).otherwise(col(c))
        )

df_missing_pd = df_fixed.toPandas()

# COMMAND ----------

df_missing_pd['bathrooms'].unique()

# COMMAND ----------

cols_with_nans = [c for c in df_missing_pd.columns if df_missing_pd[c].isna().any()]
df_missing_pd = df_missing_pd[cols_with_nans]

# COMMAND ----------

import numpy as np
df_missing_pd["bathrooms"] = df_missing_pd["bathrooms"].apply(
    lambda x: np.nan if (isinstance(x, float) and (x != x)) else x
)


# COMMAND ----------

df_missing_pd['beds'].value_counts(dropna=False)


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno

custom_cmap = sns.color_palette([
    "#fff0f5", "#f3cce3", "#f5a9c0", "#e68fac", "#c47195", "#b24b85"
])

plt.style.use("default")

plt.figure(figsize=(10, 7), dpi=140)
ax = msno.heatmap(
    df_missing_pd,
    figsize=(10, 7),
    cmap=custom_cmap,
    cbar=True
)

plt.title("Correlation of Missing Values Between Columns",
          fontsize=20, fontweight="bold", pad=20, color="#b24b85")

plt.xlabel("")
plt.ylabel("")
plt.xticks(rotation=30, ha="right", fontsize=11, color="#555555")
plt.yticks(rotation=0, fontsize=11, color="#555555")
plt.grid(False)

for spine in plt.gca().spines.values():
    spine.set_visible(False)

plt.gcf().set_facecolor("#fff8fa")
plt.gca().set_facecolor("#fff8fa")

cbar = ax.collections[0].colorbar
cbar.set_label("Correlation", fontsize=12, color="#b24b85", labelpad=10)
cbar.ax.tick_params(labelsize=10, colors="#555555")

plt.tight_layout()
plt.show()


# COMMAND ----------

plt.figure(figsize=(10,6))
msno.dendrogram(df_missing_pd, figsize=(10,6))
plt.title("Hierarchical Clustering of Missing Data", fontsize=16, fontweight="bold")
plt.show()

# COMMAND ----------

import pandas as pd

missing = df_missing_pd.isnull().sum().sort_values(ascending=False)
missing_percent = (missing / len(df_missing_pd)) * 100
missing_df = pd.DataFrame({
    'Feature': missing.index,
    'Missing %': missing_percent.round(3)
})

bar_colors = sns.color_palette([
    "#fff0f5", "#f3cce3", "#f5a9c0", "#e68fac", "#c47195", "#b24b85"
])

plt.style.use("default")

plt.figure(figsize=(10, 6), dpi=140)
bars = plt.barh(
    missing_df['Feature'],
    missing_df['Missing %'],
    color=bar_colors,
    edgecolor="#b24b85",
    linewidth=1.0
)

plt.title("Missing Values by Feature",
          fontsize=22, fontweight="bold", color="#b24b85", pad=15)

plt.xlabel("Missing Values (%)", fontsize=13, color="#555555", labelpad=10)
plt.ylabel("")
plt.gca().invert_yaxis()
plt.grid(False)

for bar in bars:
    plt.text(
        bar.get_width() + 0.5,
        bar.get_y() + bar.get_height()/2,
        f"{bar.get_width():.3f}%",
        va='center',
        fontsize=10,
        color="#a94472"
    )

for spine in plt.gca().spines.values():
    spine.set_visible(False)

plt.gcf().set_facecolor("#fff8fa")
plt.gca().set_facecolor("#fff8fa")

plt.xticks(color="#555555", fontsize=11)
plt.yticks(color="#555555", fontsize=11)

plt.tight_layout()
plt.show()

# COMMAND ----------

import plotly.graph_objects as go

missing_total = df_missing_pd.isnull().sum().sum()
available_total = df_missing_pd.size - missing_total

colors = ["#f3cce3", "#b24b85"]

fig = go.Figure(data=[go.Pie(
    labels=["Available Data", "Missing Data"],
    values=[available_total, missing_total],
    hole=0.6,
    marker=dict(colors=colors, line=dict(color="#fff8fa", width=2)),
    textinfo='label+percent',
    textfont=dict(size=16, color="#4a4a4a"),
)])

fig.update_layout(
    title={
        "text": "💗 Overall Data Completeness 💗",
        "x": 0.5,
        "y": 0.97,
        "xanchor": "center",
        "yanchor": "top",
        "font": dict(size=22, color="#b24b85", family="sans-serif")
    },
    showlegend=False,
    paper_bgcolor="#fff8fa",
    plot_bgcolor="#fff8fa",
    margin=dict(t=120, b=30, l=20, r=20)
)

fig.show()


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# ensure date type
df_missing_pd['host_since'] = pd.to_datetime(df_missing_pd['host_since'], errors='coerce')

# group by year
df_missing_pd['year'] = df_missing_pd['host_since'].dt.year
rating_missing_by_year = (
    df_missing_pd.groupby('year')['review_scores_rating']
    .apply(lambda x: x.isnull().mean() * 100)
    .reset_index(name='Missing %')
)

plt.figure(figsize=(10,6), dpi=140)
sns.lineplot(
    data=rating_missing_by_year,
    x='year', y='Missing %',
    marker='o', color="#b24b85", linewidth=2.5
)

plt.title("Trend of Missing Ratings Over Time", fontsize=18, fontweight="bold", color="#b24b85")
plt.xlabel("Host Since (Year)", fontsize=12, color="#555555")
plt.ylabel("% Missing Review Scores", fontsize=12, color="#555555")
plt.grid(False)
plt.gcf().set_facecolor("#fff8fa")
plt.gca().set_facecolor("#fff8fa")
for spine in plt.gca().spines.values():
    spine.set_visible(False)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The trend plot reveals a clear temporal pattern: missing review_scores_rating values are increasingly common among newer hosts. This likely reflects the rapid growth of Airbnb’s user base — with many new hosts and listings appearing in recent years, a larger portion of them have not yet received any guest reviews. Thus, missingness in ratings is time-dependent and partly driven by platform expansion rather than random data loss.

# COMMAND ----------

plt.figure(figsize=(8,5), dpi=140)
sns.barplot(
    data=df_missing_pd,
    x='host_is_superhost',
    y=df_missing_pd['reviews_per_month'].isnull().astype(int),
    estimator=lambda x: x.mean() * 100,
    palette=["#f3cce3", "#b24b85"]
)

plt.title("Missing 'reviews_per_month' by Superhost Status", fontsize=18, fontweight="bold", color="#b24b85")
plt.ylabel("% Missing", fontsize=12, color="#555555")
plt.xlabel("Superhost", fontsize=12, color="#555555")
plt.grid(False)
plt.gcf().set_facecolor("#fff8fa")
plt.gca().set_facecolor("#fff8fa")
for spine in plt.gca().spines.values():
    spine.set_visible(False)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC This pattern suggests that more active and experienced hosts (superhosts) consistently receive guest reviews, so their data is more complete.
# MAGIC In contrast, regular hosts may be new to the platform or rent out their listings infrequently, which leads to missing review information.
# MAGIC
# MAGIC Therefore, missingness in reviews_per_month reflects host activity and engagement levels, indicating that data completeness is tied to behavioral factors rather than data quality issues.

# COMMAND ----------

plt.figure(figsize=(8,5), dpi=140)
sns.barplot(
    data=df_missing_pd,
    x='bedrooms', y='reviews_per_month',
    palette="RdPu"
)
plt.title("Average Reviews per Month by Bedrooms", fontsize=16, fontweight="bold", color="#b24b85")
plt.xlabel("Bedrooms", fontsize=12, color="#555555")
plt.ylabel("Reviews per Month", fontsize=12, color="#555555")
plt.gcf().set_facecolor("#fff8fa")
plt.gca().set_facecolor("#fff8fa")
for spine in plt.gca().spines.values():
    spine.set_visible(False)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC This pattern suggests that smaller units are booked more frequently, likely because they are cheaper and more accessible for short stays.
# MAGIC In contrast, listings with many bedrooms are usually rented less often or target larger groups, leading to fewer bookings and, consequently, more missing values in reviews_per_month.
# MAGIC
# MAGIC Therefore, the missingness in review frequency is influenced by listing size and booking behavior.

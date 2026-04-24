# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbts-fd/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)


bronze_table = f"(catalog).(bronze_schema).{data_source}"
silver_table = f"[catalog). [silver_schema). {data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True) \
    .csv(f"{landing_path}/*.csv") \
    .withColumn("read_timestamp", F.current_timestamp()) \
    .select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("append") \
    .saveAsTable("fmcg.bronze.orders")

# COMMAND ----------

files=dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True

    )

# COMMAND ----------

df_orders = spark.sql("SELECT * FROM fmcg.bronze.orders")
df_orders.show()

# COMMAND ----------

# 1. Remove null order_qty
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# 2. Clean customer_id (trim + keep only numeric)
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(
        F.trim(F.col("customer_id")).rlike("^[0-9]+$"),
        F.trim(F.col("customer_id"))
    ).otherwise("999999").cast("string")
)

# 3. Remove weekday text from date (handles spaces also)
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Convert to proper date format
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy")
    )
)

# 5. Remove rows where date conversion failed (optional but recommended)
df_orders = df_orders.filter(F.col("order_placement_date").isNotNull())

# 6. Cast product_id before deduplication
df_orders = df_orders.withColumn(
    "product_id",
    F.col("product_id").cast("string")
)

# 7. Drop duplicates
df_orders = df_orders.dropDuplicates([
    "order_id", "order_placement_date", "customer_id", "product_id", "order_qty"
])

# 8. Final check
df_orders.printSchema()


# COMMAND ----------

df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

df_products=spark.table("fmcg.silver.products")
display(df_products)

# COMMAND ----------

df_joined = df_orders. join(df_products, on="product_id", how="inner") . select(df_orders["*"], df_products
["product_code"])

display(df_joined.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fmcg.silver.orders;

# COMMAND ----------

from delta.tables import DeltaTable

# ----------------------------------------
# 1. Create Joined DataFrame (IMPORTANT)
# ----------------------------------------
df_joined = df_orders.join(
    df_products,
    on="product_id",
    how="inner"
).select(
    df_orders["*"],
    df_products["product_code"]
)

# ----------------------------------------
# 2. Silver Table Name
# ----------------------------------------
silver_table = "fmcg.silver.orders"

# ----------------------------------------
# 3. Check if table exists
# ----------------------------------------
if not spark.catalog.tableExists(silver_table):

    # Create Silver Table
    df_joined.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema","true")\
        .mode("overwrite") \
        .saveAsTable(silver_table)

    print("Silver table created")

else:

    # Load existing Delta table
    silver_delta = DeltaTable.forName(spark, silver_table)

    # ----------------------------------------
    # 4. Merge Condition
    # ----------------------------------------
    merge_condition = """
        silver.order_id = bronze.order_id AND
        silver.product_id = bronze.product_id AND
        silver.customer_id = bronze.customer_id
    """

    # ----------------------------------------
    # 5. Merge Data
    # ----------------------------------------
    silver_delta.alias("silver").merge(
        df_joined.alias("bronze"),
        merge_condition
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("Merge completed successfully")

# COMMAND ----------

df_orders = df_orders.withColumn(
    "product_id",
    F.col("product_id").cast("string")
)

# COMMAND ----------

df_orders = df_orders.filter(
    F.col("order_qty").cast("int").isNotNull()
)

# COMMAND ----------

df_gold = df_orders.select(
    "order_id",
    F.col("order_placement_date").alias("date"),
    F.col("customer_id").alias("customer_code"),
    "product_id",
    
    F.col("order_qty").alias("sold_quantity")
)

df_gold.show(10, truncate=False)

# COMMAND ----------

df_gold = df_joined.select(
    "order_id",
    "customer_id",
    "product_id",
    "product_code",
    "order_qty"
)

# COMMAND ----------

display(df_gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS fmcg.gold.orders;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ----------------------------
# Ensure df_gold has correct columns
# ----------------------------
df_gold = df_joined.select(
    F.col("order_placement_date").alias("date"),
    F.col("customer_id").alias("customer_code"),
    "product_id",
    "product_code",
    F.col("order_qty").alias("sold_quantity")
)

# ----------------------------
# Gold table
# ----------------------------
gold_table = "fmcg.gold.sales"

if not spark.catalog.tableExists(gold_table):

    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(gold_table)

    print("Gold table created")

else:
    gold_delta = DeltaTable.forName(spark, gold_table)

    merge_condition = """
    silver.date = bronze.date AND
    silver.customer_code = bronze.customer_code AND
    silver.product_id = bronze.product_id
    """

    gold_delta.alias("silver").merge(
        df_gold.alias("bronze"),
        merge_condition
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("Gold merge completed")

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show(10)


# COMMAND ----------

df_monthly=(
    df_child

    .withColumn("month_start", F.trunc("date", "MM") )
    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )
    .withColumnRenamed("month_start", "date")
)
display(df_monthly.limit(10))


# COMMAND ----------

from delta.tables import DeltaTable

gold_parent_delta = DeltaTable.forName(
    spark,
    f"{catalog}.{gold_schema}.fact_orders"
)

gold_parent_delta.alias("parent_gold").merge(
    df_monthly.alias("child_gold"),
    """
    parent_gold.product_code = child_gold.product_code AND
    parent_gold.customer_code = child_gold.customer_code
    """
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
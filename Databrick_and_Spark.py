# Databricks notebook source
# MAGIC %md
# MAGIC ## Recommended resources
# MAGIC 
# MAGIC All resource : https://learn.microsoft.com/en-us/azure/databricks/  
# MAGIC Azure Databrick concept : https://learn.microsoft.com/en-us/azure/databricks/getting-started/concepts  
# MAGIC SparkDataFrame : https://learn.microsoft.com/en-us/azure/databricks/getting-started/dataframes-python  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databrick Environment (Live Demo)
# MAGIC 
# MAGIC 1. Workspace
# MAGIC     - Create Notebook, Folder
# MAGIC 2. Data
# MAGIC     - Table : focus on `tdm_seg`
# MAGIC     - DBFS : `FileStore`, `mnt` (advance `/user/hive/warehouse/tdm_seg.db`)
# MAGIC 3. Compute
# MAGIC     - Meta, 01, 02
# MAGIC 4. Workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Spark, PySpark
# MAGIC 1. Importing PySpark SQL module : **not recommend** `from pyspark.sql.function import * `, better to import only function needed or `from pyspark.sql import function as F`.
# MAGIC 2. Lazy evaluation : Only action command like `.display()` `.count()` then spark start computation. 
# MAGIC 3. Always filter with desired data range based on partition for better performance.
# MAGIC 4. Beware of joining key with `NULL` : better salting or split join then combine.
# MAGIC 5. Create small SparkDataFrame for code testing.
# MAGIC 5. Duplicate join key will yield duplication (including `Left join`, different from other SQL like Oracle etc.). Better `drop_duplicates()` whenever possible.

# COMMAND ----------

# 1) Importing with F
from pyspark.sql import functions as F

# Or
from pyspark.sql.functions import col, count_distinct

# COMMAND ----------

# 2) Lazy Eval
txn = spark.table("tdm.v_transaction_item")
week01_n_bask = (txn
                 .where(F.col("week_id")==202201)
                 .agg(F.count_distinct("transaction_uid"))
                )
week01_n_bask.printSchema()

# COMMAND ----------

# With action command, spark will stark computation
week01_n_bask.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This command will show the detail partitions, except view
# MAGIC DESCRIBE EXTENDED tdm.v_transaction_item

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE tdm.tdm_transaction_item_int

# COMMAND ----------

txn = spark.table("tdm.v_transaction_item")
agg_bask = txn.groupBy("week_id").agg(F.count_distinct("transaction_uid"))
week01_n_bask = agg_bask.where(F.col("week_id")==202201)

# COMMAND ----------

week01_n_bask.display()

# COMMAND ----------

week01_n_bask.explain(extended=True)

# COMMAND ----------

# Join customer_id to get household_id
itm = spark.table("tdm.v_transaction_item").where(F.col("week_id")==202201)
seg = spark.table("tdm.v_customer_dim").select("customer_id", "household_id")

# COMMAND ----------

seg.count()

# COMMAND ----------

seg.drop_duplicates().count()

# COMMAND ----------

# customer_id also have Null value
itm.where(F.col("customer_id").isNull()).count()

# COMMAND ----------

# for small data, join key with Null will yeild result. But for large data it will effect the performance
itm.join(seg, "customer_id", "inner").display()

# COMMAND ----------

# Salting customer_id
itm.fillna(999, subset=["customer_id"]).join(seg, "customer_id", "inner").display()

# COMMAND ----------

# split join and combine
txn_w_c_id = itm.where(F.col("customer_id").isNotNull())
txn_wo_c_id = itm.where(F.col("customer_id").isNull())
txn_seg = txn_w_c_id.join(seg, "customer_id", "inner")
result = txn_seg.unionByName(txn_wo_c_id, allowMissingColumns=True)
result.display()

# COMMAND ----------

# Create small SparkDataFrame
# Use pattern 
# spark.createDataFrame([(,),
#                        (,)],
#                        [,])

cars = spark.createDataFrame([(100, 'Fremont', 'Honda Civic', 10),
                            (100, 'Fremont', 'Honda Accord', 15),
                            (100, 'Fremont', 'Honda CRV', 7),
                            (200, 'Dublin', 'Honda Civic', 20),
                            (200, 'Dublin', 'Honda Accord', 10),
                            (200, 'Dublin', 'Honda CRV', 3),
                            (300, 'San Jose', 'Honda Civic', 5),
                            (300, 'San Jose', 'Honda Accord', 8)], 
                           ['country_id' , 'city', 'car_model', 'quantity'])

country = spark.createDataFrame([(100, "US"), 
                                (100, "US"), # duplicate key
                                (200, "UK")],
                               ["country_id", "country"])

# COMMAND ----------

cars.display()

# COMMAND ----------

country.display()

# COMMAND ----------

cars.join(country, "country_id", "inner").display()

# COMMAND ----------

# Even lift join, the result still duplicated.
cars.join(country, "country_id", "left").display()

# COMMAND ----------

# drop duplicated when ever possible before join
country_dedup = country.drop_duplicates()
cars.join(country_dedup, "country_id", "left").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Databrick and Spark
# MAGIC 1. Import .csv into Databrick : Tab "Data"->"DBFS"->"FileStore", then upload.
# MAGIC 2. Export small .csv, `limit 10,000 rows` only.
# MAGIC 3. Generate large single .csv and download.
# MAGIC    - use Spark with Spark API Path
# MAGIC    - use Pandas with File API Path

# COMMAND ----------

# Export small .csv
country.display()

# COMMAND ----------

# Generate large single .csv and download
car_join = cars.join(country_dedup, "country_id", "left")

# COMMAND ----------

# MAGIC %md Spark ways
# MAGIC 1) Generate single .csv with spark

# COMMAND ----------

# 1) Generate single .csv with spark
(car_join
 .coalesce(1)  #to force single file
 .write
 .format('com.databricks.spark.csv')  # save as .csv format 
 .option("header", "true")
 .save("dbfs:/FileStore/thanakrit/temp/to_export/car_join.csv")  # Spark API path
)

# COMMAND ----------

# MAGIC %md
# MAGIC 2) From tab "Data"->"DBFS"-> Copy result file (under target path)
# MAGIC 3) Paste in browser URL, between `xxx.net/`___`?o=xxx`
# MAGIC 4) Change `dbfs:/FileStore` -> `files` and "Enter"

# COMMAND ----------

# MAGIC %md Pandas ways
# MAGIC 1) Convert to Pandas , save .csv with location in File API format.

# COMMAND ----------

car_join_df = car_join.toPandas()

# COMMAND ----------

# Use File API Path
car_join_df.to_csv("/dbfs/FileStore/thanakrit/temp/to_export/car_join_df.csv")

# COMMAND ----------

# MAGIC %md then use step 2) - 4) from above to load to local

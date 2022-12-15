# Databricks notebook source
# MAGIC %md
# MAGIC ## Training on Databrick community version
# MAGIC apply link : https://community.cloud.databricks.com/

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

"""
Creat Spark entry point : SparkSession
----
Databrick notebook will auto create `spark` as entry point
"""
ss = SparkSession.builder.appName("helper").getOrCreate()

# COMMAND ----------

"""
Read .csv as SparkDataFrame
"""
sf = spark.read.csv("/FileStore/Udamy_training/sales_info.csv", header=True, inferSchema=True)

# COMMAND ----------

"""
Explore Spark DataFrame 
"""
sf.show(5)
sf.printSchema()
sf.columns
sf.describe().show()
# Databricks only
sf.display(5)

# COMMAND ----------

"""
Create toy spark dataframe
----
spark.createDataFrame( [List of data in tuple ( , )] , Column name in tuple ( , ))
"""
sf = spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ('k','v'))
# for one column still keep the tuple pair
one_col = spark.createDataFrame([('a', ), ('b', ), ('c', )], ('k', ))

# COMMAND ----------

"""
Selection of column
----
With .select()
1) List of column name or column names
"""
sf.select(['k', 'v']).show()
sf.select('k', 'v').show()

# COMMAND ----------

"""
2) For select with list of column name, could apply list operation in fancy ways
"""
col = ["v", "k"]

col = ['v', 'k']
sf.select(*col, # unpack list 
          *col[::-1], # reverse
          *col*2 # unpack and repeat
         ).show()

# COMMAND ----------

"""
3) Combine with pyspark F.col( name of column ) to access Column method eg. .alias, .isNull ect.
"""
sf.select(F.col('v'), F.col('v').alias('2nd_v')).show()
F.col("v").

# COMMAND ----------

"""
4) Pandas Style 
- dot notation : sf.column_name
- list of column : sf["column_name"]
"""
sf.select(sf.v, sf.k, sf.v.alias('2nd_v'), sf["k"].alias("2nd_k")).show()

# COMMAND ----------

"""
Format number, string
----
- F.format_number()
- F.format_string()
"""
sf.select("v", F.format_number("v", 2).alias("fmt_v"), "k", F.format_string("%s with %d", "k", "v").alias("fmt_k_v")).show()

# COMMAND ----------

"""
Filter row with condition
----
Use .where with
- SparkSQL
- PySpark style
- Pandas style
"""
sf.where("v = 2").show()  # SparkSQL in condition, use = 
# or
sf.where(F.col("v")==2).show()  # PySpark condition, use ==
# or
sf.where(sf.v==2).show()  # Pandas dot style
# or
sf.where(sf["v"]==2).show()  # Pandas column lsit style

# COMMAND ----------

# combine multiple condition
sf.where( ( sf["v"]==2 ) | (sf["k"]=="a") ).show()
# ~ for negate
sf.where( ( F.col("v")==2) | (~(F.col("k")=="a") ) ).show()
sf.where( ( F.col("v")>=2) & (~(F.col("k")=="a") ) ).show()

# COMMAND ----------

"""
Add new column / Rename column
- .withColumn
- .withColumnRename
- .toDF : rename multiple columns
- .lit : literally use, prevent Spark .withColumn interpret as column name
"""
sf.withColumn("v_add_1", sf["v"]+1).show()
sf.withColumnRenamed("k","key").show()
sf.toDF("key", "value").show()
# This will error, spark interpret value as column name
# sf.withColumn("new_string", "new").show()
sf.withColumn("new_string", F.lit("new")).show()
# This also will be error
# sf.withColumn("new_value", 0).show()
sf.withColumn("new_val", F.lit(0)).show()
# This also error
# sf.withColumn("all_null", None).show()
sf.withColumn("all_null", F.lit(None) ).show()

# COMMAND ----------

"""
Case - When - Otherwise is short-circuit 
----
If the condition was met, the rest .when will not run
"""

sf.withColumn('short_circuit', 
              F.when(F.col('v') <= 2, F.lit('lessthan_2'))
               .when(F.col('v') <= 1, F.lit('lessthan_1'))  # This condition have no effect - short circuit
              .otherwise('morethan_3')
             ).show()

# COMMAND ----------

"""
Null data
- Drop null
- Fill null : Fill only column same type of data to be filled. 
----
"""
sf = spark.createDataFrame([('a', None, 1.1), (None, 2, 2.2), ('c', 3, None)], ('k','v','l'))

# COMMAND ----------

sf.show()

# COMMAND ----------

# drop Na will drop any row with NAd
sf.dropna().show()

# COMMAND ----------

# subset , specific column with Null to be drop
sf.dropna(subset=["k"]).show()

# COMMAND ----------

# fill with String type, will only fill column StringType
sf.fillna("0").show()

# COMMAND ----------

sf.fillna(0).show()

# COMMAND ----------

# Use input as dict to fill different column type in the sametime 
sf.fillna({"k":0, "v":"0", "l":0}).show()

# COMMAND ----------

"""
Aggregate & Group by
----
Aggregate style
A) Without groupby = Aggregate whole DataFrame
- .agg( function ( column to aggregate ) )
- .agg( dict of {"column name": "sql aggregate fn", "..":"..", "..":".."}), not recommended
- .select( function (column to aggregate) )
"""
sf = spark.read.csv( "/FileStore/Udamy_training/sales_info.csv", header=True, inferSchema=True)

sf.agg(F.mean("Sales").alias("mean_sales"), 
       F.count("Company").alias("n_row"), 
       F.stddev("Sales").alias("stddev_sales")).show()
# 1 column - 1 aggregation
sf.agg({"Sales":"mean", "Company":"count", "Sales":"stddev"}).show()

sf.select(F.mean("Sales"), F.count("Company"), F.stddev_samp("Sales")).show()

# COMMAND ----------

"""
Aggregate style
----
B) With groupby 
- .agg( function ( column to aggregate ) )
- .agg( dict of {"column name": "sql aggregate fn", "..":"..", "..":".."})
- .GroupBy function
"""
sf.groupBy("Company").agg(F.mean("Sales").alias("mean_sales"), 
                          F.count("Company").alias("n_row"), 
                          F.stddev("Sales").alias("stddev_sales")).show()

sf.groupBy("Company").agg({"Sales":"mean", "Company":"count", "Sales":"stddev"}).show()

sf.groupBy("Company").mean("Sales").show()

# COMMAND ----------

"""
Orderby
----
OrderBy is expensive command for Spark, if possible order the last output
OrderBy style :
- .orderBy( "column name" ) = ascending / .orderBy( "column_name", ascending=False) = decending
- .orderBy( F.col( "column name" ).{order column function} ) : .desc(), .desc_nulls_last() , .desc_nulls_first(), .asc(), .asc_nulls_last(), asc_nulls_first()
"""
# One level
sf.groupBy("Company").agg(F.mean("Sales").alias("mean_sales")).orderBy("mean_sales").show()
# decending
sf.groupBy("Company").agg(F.mean("Sales").alias("mean_sales")).orderBy("mean_sales", ascending=False).show()
# multi level order, mix ascending , decending
sf.groupBy("Company", "Person").agg(F.mean("Sales").alias("mean_sales")).orderBy(["Company", "Person"], ascending=[False, True]).show()

# use method of column to order 
sf.groupBy("Company", "Person").agg(F.mean("Sales").alias("mean_sales")).orderBy(F.col("Company").desc(), F.col("Person").asc() ).show()

# COMMAND ----------

"""
Join DataFrame
----
Join style
- left.join(right, on="join key column same name", how="") : will keep only 1 key column
- left.join(right, on=[ (left["key column"] == right["key_column"]) , ( other key column to join ) , ( ) ], how="") : good for different key column name between dataframe, will keep both columns
"""
left = spark.createDataFrame([('a', 1), ('b', 2), ('c', 3), ('b', 22)], ('k','v'))
right = spark.createDataFrame([('a', 'Ant'), ('b', "Bird"), ('c', "Cat")], ('k','name'))

left.join(right, on="k", how="inner").show()

left.join(right, on=[(left["k"]==right["k"])], how="inner").show()

left.alias("a").join(right.alias("b"), on=[(F.col("a.k") == F.col("b.k"))], how="inner").show()

# COMMAND ----------

"""
Left join with Right dataframe have duplicated key - Spark will create duplicate result
"""

right = spark.createDataFrame([('a', 'Ant'), ('b', "Bird"), ('c', "Cat"), ('c', "Crown")], ('k','name'))

left.join(right, "k", "left").show()

# COMMAND ----------

"""
Date - Time function
----
from pyspark.sql.functions import dayofmonth, dayofweek, dayofyear, hour, month, year, weekofyear, format_number, date_format
Study from API doccuments : https://spark.apache.org/docs/3.1.2/api/python/reference/pyspark.sql.html
""

# COMMAND ----------

"""
Partition & Window function
----
Try to apply as much as possible since will greatly improve code performance
"""
from pyspark.sql import Window
from pyspark.sql.types import DateType

sf = spark.read.csv("/FileStore/Udamy_training/sales_info.csv", header=True, inferSchema=True)
sf.show(5)
sf.printSchema()

# COMMAND ----------

sf.withColumn("Higher_Sales_Person", F.lead("Person").over(Window.partitionBy("Company").orderBy("Sales"))).display()

# COMMAND ----------

"""
Pivot
----
This is action command, selectively used or export and pivot in local machine.
"""


sf.groupBy("Person").pivot("Company").agg(F.mean("Sales")).display()

# COMMAND ----------



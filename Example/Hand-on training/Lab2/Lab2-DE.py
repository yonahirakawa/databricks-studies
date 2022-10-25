# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Labs World Tour for Data Engineers
# MAGIC Welcome to the Data and AI World Tour for data engineers.  Over the course of this notebook, you will use an open dataset and learn how to:
# MAGIC 1. Ingest data that has been landed into cloud storage
# MAGIC 2. Transform and store your data in the reliable and performant Delta Lake storage format
# MAGIC 3. Use Update, Delete, Merge, Schema Evolution, Time Travel Capabilities, and CDF (Change Data Feed) functionality built in the Delta Lake storage format
# MAGIC 
# MAGIC ## The Use Case
# MAGIC We will analyze United States zip code data that is made available as a public dataset online. You can learn more about the dataset [here](https://simplemaps.com/data/us-zips), but we'll download it automatically below. 

# COMMAND ----------

# DBTITLE 1,Importing PySpark functions and types
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Setting a Delta property that we'll explore later in this notebook
# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# DBTITLE 1,Download our test data and place it into temporary storage
# MAGIC %sh
# MAGIC # Pull CSV file from url
# MAGIC wget -nc https://lafkkbox.blob.core.windows.net/worldtourdata/uszips.csv
# MAGIC 
# MAGIC # Move from databricks/driver to dbfs
# MAGIC mv /databricks/driver/uszips.csv /dbfs/FileStore/uszips.csv

# COMMAND ----------

# mv: cannot move '/databricks/driver/uszips.csv' to '/dbfs/FileStore/uszips.csv': No such file or directory

# COMMAND ----------

# DBTITLE 1,Move files from driver to dbfs using dbutils
#dbutils.fs.mv("file:/databricks/driver/uszips.csv", "dbfs:/FileStore/uszips.csv", recurse=True)

# COMMAND ----------

# DBTITLE 1,Explore storage using file system commands
# MAGIC %fs ls 'dbfs:/FileStore/'

# COMMAND ----------

# DBTITLE 1,Let's take a sneak peak at the file we'll be working with
# MAGIC %fs head 'dbfs:/FileStore/uszips.csv'

# COMMAND ----------

# DBTITLE 1,Ingest Data using a more traditional PySpark pattern
# CSV schema
schema = "zip string,lat double,lng double,city string,state_id string,state_name string,zcta boolean,parent_zcta string,population integer,density double,county_fips integer,county_name string,county_weights string,county_names_all string,county_fips_all string,imprecise boolean,military boolean,timezone string"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format("csv") \
  .option("header", "true") \
  .option("sep", ",") \
  .option("escape", "\"") \
  .schema(schema) \
  .load("dbfs:/FileStore")

display(df)

# write data out in Delta format
# we'll skip writing this data for now as we'll use another ingestion pattern
# df.write.format("delta").partitionBy('state_id').save('/mnt/delta/uszips_delta')

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- #DATA ENGINEERING AND STREAMING ARCHITECTURE -->
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/DE and Streaming2.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC ####Auto Loader, COPY INTO, and Incrementally Ingesting Data
# MAGIC [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html) and [COPY INTO](https://docs.databricks.com/ingestion/copy-into/index.html) are two methods of ingesting data into a Delta Lake table from a folder in a Data Lake. “Yeah, so... Why is that so special?”, you may ask. The reason these features are special is that they make it possible to ingest data directly from a data lake incrementally, in an idempotent way, without needing a distributed streaming system like Kafka. This can considerably simplify the Incremental ETL process. It is also an extremely efficient way to ingest data since you are only ingesting new data and not reprocessing data that already exists. Below is an Incremental ETL architecture. We will focus on the left hand side, ingesting into tables from outside sources. 
# MAGIC 
# MAGIC You can incrementally ingest data either continuously or scheduled in a job. COPY INTO and Auto Loader cover both cases and we will show you how below.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/get-start-delta-blog-img-1.png" width=1000>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader
# MAGIC In this notebook we will use Auto Loader for a basic ingest use case with [schema inference and evolution](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-gen2.html#schema-inference-and-evolution).  
# MAGIC Notice how the schema is inferred, but we can provide schema hints for certain columns also.

# COMMAND ----------

# DBTITLE 1,Delete Existing Files and Create Initial Database
spark.sql("DROP DATABASE IF EXISTS population CASCADE")
dbutils.fs.rm("/mnt/delta/", recurse=True)
spark.sql("CREATE DATABASE IF NOT EXISTS population LOCATION '/mnt/delta/'")

# COMMAND ----------

# "cloudFiles" indicates the use of Auto Loader

dfBronze = spark.readStream.format("cloudFiles") \
  .option('cloudFiles.format', 'csv') \
  .option('header','true') \
  .option("cloudFiles.schemaLocation", "/mnt/delta/checkpoint/uszips_delta/") \
  .option("cloudFiles.schemaHints", "zip string, population integer, density double") \
  .load("dbfs:/FileStore")

# The stream will shut itself off when it is finished based on the trigger once feature
# The checkpoint location saves the state of the ingest when it is shut off so we know where to pick up next time
# Notice that we can partition our data as well.  Partioning isn't recommended unless our data is over a GB in size.  Use Optimize and ZOrder instead which is explained below.

# trigger once: se tirar essa opção fica rodando continuamente!
dfBronze.writeStream \
  .format("delta") \
  .partitionBy('state_id') \
  .trigger(once=True) \
  .option("checkpointLocation", "/mnt/delta/checkpoint/uszips_delta/") \
  .start("/mnt/delta/uszips_delta/")

# COMMAND ----------

# DBTITLE 1,Register the table with the metastore
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS population.uszips
# MAGIC  
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/delta/uszips_delta/'

# COMMAND ----------

# DBTITLE 1,Look at the table details
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED population.uszips 

# COMMAND ----------

# DBTITLE 1,Let's look at the tables storage. Notice the transaction log and the partitioned directories
# MAGIC %fs ls "/mnt/delta/uszips_delta/"

# COMMAND ----------

# DBTITLE 1,Query the table as a file path table
# MAGIC %sql
# MAGIC SELECT * FROM delta.`/mnt/delta/uszips_delta/`

# COMMAND ----------

# DBTITLE 1,Query the table as a regular table
# MAGIC %sql
# MAGIC SELECT * FROM population.uszips

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Additional Reporting Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS population.states 
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/delta/states_delta/'
# MAGIC AS
# MAGIC SELECT DISTINCT state_id, state_name FROM population.uszips

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM population.states 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS population.county 
# MAGIC USING DELTA 
# MAGIC LOCATION '/mnt/delta/county_delta/'
# MAGIC AS
# MAGIC SELECT DISTINCT state_id, county_fips, county_name FROM population.uszips

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM population.county 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating Silver/Gold tables
# MAGIC Create an aggregate table usign PySpark.  This could have also been done using SQL, Scala, or R.  
# MAGIC Notice that we're able to write the table to a Delta path and register the table in one line.  

# COMMAND ----------

city_county_df = spark.read.format("delta").load("/mnt/delta/uszips_delta/") \
  .groupBy("state_id", "city", "county_fips").sum('population') \
  .withColumnRenamed("sum(population)", "populationSum")

city_county_df.write.format("delta").mode('overwrite').option('path', '/mnt/delta/city_delta/').saveAsTable('population.city')

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE population.city SET TBLPROPERTIES (delta.enableChangeDataFeed=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM population.city

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform DML operations, Schema Evolution and Time Travel
# MAGIC #####Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing data engineers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %md ### DELETE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM population.city WHERE populationSum <= 10000

# COMMAND ----------

# DBTITLE 1,Let's confirm the data is gone
# MAGIC %sql
# MAGIC SELECT * FROM population.city WHERE populationSum <= 10000

# COMMAND ----------

# MAGIC %md ### UPDATE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE population.city SET populationSum = 19999 WHERE populationSum BETWEEN 10000 AND 20000

# COMMAND ----------

# DBTITLE 1,Let's confirm the data is updated
# MAGIC %sql
# MAGIC SELECT * FROM population.city WHERE populationSum BETWEEN 10000 AND 20000

# COMMAND ----------

# MAGIC %md ###MERGE Support
# MAGIC We can merge directly into a Delta Lake table and perform Inserts, Updates, and Deletes in one simple statement.  
# MAGIC This is supported in multiple languages, but we'll perform the command in SQL.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution
# MAGIC With the `autoMerge` option, you can evolve your Delta Lake table schema seamlessly inside the ETL pipeline. New columns will automatically be added to your table.

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.schema.autoMerge.enabled = true;

# COMMAND ----------

# DBTITLE 1,Create a Common Table Expression and Merge to the Target table
# MAGIC %sql
# MAGIC -- Our CTE has a new column called densityAvg
# MAGIC WITH merge_cte AS (
# MAGIC SELECT state_id, city, county_fips, SUM(population) AS populationSum, AVG(density) AS densityAvg
# MAGIC FROM population.uszips
# MAGIC WHERE state_id <> 'OH'
# MAGIC GROUP BY state_id, city, county_fips
# MAGIC )
# MAGIC 
# MAGIC MERGE INTO population.city as d
# MAGIC USING merge_cte as m
# MAGIC on d.state_id = m.state_id AND d.city = m.city AND d.county_fips = m.county_fips
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Let's confirm our deleted data is back
# MAGIC %sql
# MAGIC --Notice our new column has been seamlessly added to the table
# MAGIC SELECT * FROM population.city WHERE populationSum <= 10000

# COMMAND ----------

# DBTITLE 1,Let's confirm our updated data is in its original form
# MAGIC %sql
# MAGIC --Notice our new column has been seamlessly added to the table
# MAGIC SELECT * FROM population.city WHERE populationSum BETWEEN 10000 AND 20000

# COMMAND ----------

# MAGIC %md ###Let's travel back in time with Time Travel!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# DBTITLE 1,Review Delta Lake Table History
# MAGIC %sql
# MAGIC DESCRIBE HISTORY population.city

# COMMAND ----------

# MAGIC %md ####  Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# DBTITLE 1,Let's look at the table as of version 2
# MAGIC %sql
# MAGIC SELECT * FROM population.city VERSION AS OF 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can even roll back a table to a previous version

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE population.city TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM population.city

# COMMAND ----------

# MAGIC %md
# MAGIC ### Or even create shallow or deep clones of a table (backups or testing tables)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE population.city_bak
# MAGIC DEEP CLONE population.city
# MAGIC LOCATION '/mnt/delta/city_delta_bak'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM population.city_bak

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Simplify Your Medallion Architecture with Delta Lake’s CDF Feature
# MAGIC 
# MAGIC ### Overview
# MAGIC The medallion architecture takes raw data landed from source systems and refines the data through bronze, silver and gold tables. It is an architecture that the MERGE operation and log versioning in Delta Lake make possible. Change data capture (CDC) is a use case that we see many customers implement in Databricks. The [Change Data Feed](https://docs.databricks.com/delta/delta-change-data-feed.html) (CDF) feature in Delta Lake makes this architecture even simpler to implement!
# MAGIC 
# MAGIC CDF makes it possible to detect data changes between versions or timestamps of a Delta table. This allows us to build ETL pipelines that can be naturally incremental in nature. The CDF table_changes function lets us analyze the changes (inserts, updates, and deletes) that have occured on a Delta table which we can then use to funnel changes to downstream consumers.  
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/05/cdf-blog-img-1-rev.png" width=600>

# COMMAND ----------

# DBTITLE 1,Notice the change data folder that is above the Delta transaction log folder
# MAGIC %fs ls '/mnt/delta/city_delta'

# COMMAND ----------

# DBTITLE 1,Use the table_changes function to explore the CDF
# MAGIC %sql
# MAGIC SELECT * FROM table_changes('population.city', 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### IDENTITY COLUMNS
# MAGIC Delta Lake now supports identity columns. When you write to a Delta table that defines an identity column, and you do not provide values for that column, Delta automatically assigns a unique and statistically increasing or decreasing value.

# COMMAND ----------

# DBTITLE 1,Create a new table with an IDENTITY column
# MAGIC %sql
# MAGIC CREATE TABLE population.dim_states
# MAGIC ( state_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   state_id STRING, 
# MAGIC   state_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/delta/dim_states_delta'

# COMMAND ----------

# DBTITLE 1,Insert records into the table
# MAGIC %sql
# MAGIC INSERT INTO population.dim_states (state_id, state_name) 
# MAGIC SELECT * FROM population.states

# COMMAND ----------

# DBTITLE 1,Let's look at the identity column
# MAGIC %sql
# MAGIC SELECT * FROM population.dim_states

# COMMAND ----------

# MAGIC %md #####  OPTIMIZE
# MAGIC OPTIMIZE optimizes the layout of Delta Lake data. Optionally, optimize a subset of data or colocate data by column. If you do not specify colocation, bin-packing optimization is performed.

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE population.city ZORDER BY (state_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ###But Wait, There's Even More in Delta Lake!
# MAGIC We've explored a lot of the awesome powers of the [Delta Lake](https://docs.databricks.com/delta/index.html) storage format, but there is even more capabilities that Delta Lake provides.  
# MAGIC Check the docs to explore even more functinality like:
# MAGIC - Unified Batch and Streaming directly from Delta Lake tables
# MAGIC - Constraints
# MAGIC - Primary Keys and Foreign Keys
# MAGIC - Information Schema views
# MAGIC - Automatic capture of table statistics for data skipping
# MAGIC - Computed/generated columns

# COMMAND ----------



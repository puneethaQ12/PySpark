# Databricks notebook source
from pyspark import SparkContext, SparkConf

# COMMAND ----------

dbutils.fs.ls("FileStore/")


# COMMAND ----------

# MAGIC %md
# MAGIC ##DATA READING

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('dbfs:/FileStore/BigMart_Sales__1_.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading JSON

# COMMAND ----------

df_json=spark.read.format('json').option('inferSchema',True)\
    .option('header',True)\
    .option('multiLine',True)\
    .load('dbfs:/FileStore/drivers.json')


# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCHEMA -DDL AND STRUCTYPE()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema='''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

'''

# COMMAND ----------

df=spark.read.format('csv')\
        .schema(my_ddl_schema)\
        .option('header',True)\
        .load('dbfs:/FileStore/BigMart_Sales__1_.csv')

df.printSchema()

# COMMAND ----------

df.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema=StructType([ StructField('Item_Identifier',StringType(),True), StructField('Item_Weight',StringType(),True), StructField('Item_Fat_Content',StringType(),True), StructField('Item_Visibility',StringType(),True), StructField('Item_MRP',StringType(),True), StructField('Outlet_Identifier',StringType(),True), StructField('Outlet_Establishment_Year',StringType(),True), StructField('Outlet_Size',StringType(),True), StructField('Outlet_Location_Type',StringType(),True), StructField('Outlet_Type',StringType(),True), StructField('Item_Outlet_Sales',StringType(),True)])

# COMMAND ----------

df=spark.read.format('csv')\
            .schema(my_struct_schema)\
            .option('header',True)\
            .load('dbfs:/FileStore/BigMart_Sales__1_.csv')

# COMMAND ----------

df.display(8)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select

# COMMAND ----------

df_sel=df.select('Item_Identifier','Item_Weight','Item_Fat_Content')
df_sel.display(1)

# COMMAND ----------

df.select(col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Alias

# COMMAND ----------

df.select(col('Item_Fat_Content').alias('fc')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filter/where
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1
# MAGIC Filter the columns

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-2
# MAGIC based on the given condition

# COMMAND ----------

df.filter( (col('Item_Type')=='Fruits and Vegetables') & (col('Item_weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario 3
# MAGIC Fetch the data with Tier in (Tier1 ot Tier2) annd Outlet Size is Null

# COMMAND ----------

df.filter(
    (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2')) & 
    (col('Outlet_Size').isNull())
).display()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #withColumnRename

# COMMAND ----------

df.withColumnRenamed('Item_weight','iw').display(4)

# COMMAND ----------

# MAGIC %md
# MAGIC #withColumn
# MAGIC (modify the column and create new column)

# COMMAND ----------

df=df.withColumn('flag',lit('new')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Type Casting

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #sort
# MAGIC

# COMMAND ----------

df=spark.read.format('csv')\
            .schema(my_struct_schema)\
            .option('header',True)\
            .load('dbfs:/FileStore/BigMart_Sales__1_.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.sort(col('Item_weight').desc()).display()

# COMMAND ----------


df.sort(col('Item_Visibility').asc()).display()
     

# COMMAND ----------


df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Limit

# COMMAND ----------

df.limit(10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop_Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------


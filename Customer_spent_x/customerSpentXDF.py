from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

#Initialize the spark methodology
spark = SparkSession.builder.appName("customerSpentXDF").getOrCreate()

#Create the headers for the data
customer_schema = StructType([StructField("customer_id", IntegerType(), True), StructField("item_id", IntegerType(), True),StructField("total_spent", FloatType(), True)])

#Load in the data to create the dataframe object
customer_df = spark.read.schema(customer_schema).csv("customer-orders.csv")

#Check how much each customer spent
total_by_customer = customer_df.groupBy("customer_id").agg(func.round(func.sum("total_spent"),2).alias("total_spent"))

total_by_customer_sorted = total_by_customer.sort("total_spent")

total_by_customer_sorted.show(total_by_customer_sorted.count())

spark.stop()
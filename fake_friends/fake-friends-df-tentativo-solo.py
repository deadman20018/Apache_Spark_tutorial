from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("fake-friend-df").getOrCreate()

#Reading the csv file and telling pySpark to infer the schema based on the headers in the csv file
people = spark.read.option("header","true").option("Inferschema", "true").csv("fakefriends-header.csv")

#Printing the infered schema
print("Here is the infered schema")
people.printSchema()

#Let's display the two columns that are neccessary for our needs
people.select("age").show()

people.select("friends").show()

#Super query to show the average number of friends by age
people.groupBy("age").agg(func.round(func.avg("friends"),0)).orderBy("age").show()


spark.stop()
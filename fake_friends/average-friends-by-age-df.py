from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fake-friend-df").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

friendsByAge = people.select("friends","age")

friendsByAge.groupBy("age").avg("friends").sort("age").show()

spark.stop()
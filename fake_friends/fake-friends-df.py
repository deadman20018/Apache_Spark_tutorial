from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fake-friend-df").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

print("Here is our infered schema")
people.printSchema()

print("Let's display the name column: ")
people.select("Name").show()

print("Filter out any people over 21: ")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("Age").count().show()

print("Make everyone 10 years older")
people.select(people.name, people.age + 10).show()

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql import Row

#Create a spark session
spark = SparkSession.builder.appName("fake-friends-sql").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("UTF-8")),age=int(fields[2]), numFriends=int(fields[3]))
lines = spark.sparkContext.textFile("./fakefriends.csv")
people = lines.map(mapper)

#Infer the schema, and register the DataFrame as a table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

#SQL can be run over dataframes that have been registered as a table
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <=19")

#The result of SQL queries are RDDS and support all the normal RDD operations
for teen in teenagers.collect():
    print(teen)

#We can also use functions instead of SQL Queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

#We have to remember to stop spark
spark.stop()
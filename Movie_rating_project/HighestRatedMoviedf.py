from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("HighestRatedMoviedf").getOrCreate()

#Create schema when reading the data
schema = StructType([StructField("userID",IntegerType(), True), StructField("moviesID", IntegerType(), True), StructField("rating", IntegerType(), True), StructField("timestamp", LongType(), True)])

#Load the dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")

#Some sql style magic to sort all movies by popularity in one line!
topMoviesIDS = moviesDF.groupBy("moviesID").count().orderBy(func.desc("count"))

#Grab the top 10
topMoviesIDS.show(10)

#Stop the session
spark.stop()
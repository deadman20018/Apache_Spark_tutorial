from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("customer-orders")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return(int(fields[0]), float(fields[2]))

input = sc.textFile("customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomers = mappedInput.reduceByKey(lambda x,y: x+y)

results = totalByCustomers.collect()
for result in results:
    print(result)
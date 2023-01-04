from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customer_spent_x")
sc = SparkContext(conf = conf)
def extractCustomerPricePairs(lines):
    fields = lines.split(',')
    return(int(fields[0]), float(fields[2]))

input = sc.textFile("./customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x,y: x + y)

results = totalByCustomer.collect()
results.sort()
for result in results:
    print(result)
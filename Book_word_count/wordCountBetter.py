import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

input = sc.textFile("./Book")
words = input.flatMap(normalizeWords)
wordCounts = words.map((lambda x: x,1)).reduceByKey(lambda x,y: x+y)
wordCountSorted = wordCounts.map(lambda x,y: (y,x)).sortByKey()

results = wordCountSorted.collect()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if(cleanWord):
        print(cleanWord, count)
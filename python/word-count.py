import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWrods(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("C:/sparkData/Scala/SparkScala3/book.txt")
words = input.flatMap(normalizeWrods)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x + y)
wordCountsSorted = wordCounts.map(lambda x,y: (y,x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii','ignore')
    if(word):
        print(word + ":\t\t" + count)
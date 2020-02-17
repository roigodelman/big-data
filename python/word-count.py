
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

input = sc.textFile("C:/sparkData/Scala/SparkScala3/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count int wordCounts.items():
    cleanWrods = word.encode('ascii','ignore')
    if(cleanWrods):
        print(cleanWrods, count)
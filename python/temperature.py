
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def printResult(results):
    for result in results.collect():
        print(result)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

line = sc.textFile("C:/sparkData/Scala/SparkScala3/1800.csv")
rdd = line.map(parseLine)

maxTemps = rdd.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
a = stationTemps.reduceByKey(lambda x,y: max(x,y))

printResult(a)

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customers")
sc = SparkContext(conf = conf)

def parsLine(line):
    splitLine = line.split(',')
    customerId = int(splitLine[0])
    amount = float(splitLine[1])
    return (customerId, amount)

def printResults(results):
    for result in results:
        print(result)

input = sc.textFile("C:/sparkData/Scala/SparkScala3/customer-orders.csv")
rdd = input.map(parsLine)
sum = rdd.reduceByKey(lambda x,y: x+y)
flipped = sum.map(lambda x_y: (x_y[1],x_y[0]))
totalByCustomerSorted = flipped.sortByKey()
results = totalByCustomerSorted.collect()

printResults(results)



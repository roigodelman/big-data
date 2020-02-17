package com.frends.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Ecommers {
  
   def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    var filePath = "C:\\sparkData\\Scala\\SparkScala3\\customer-orders.csv";
    val lines = sc.textFile(filePath);
    var customer = lines.map(setLine)
    val  a=  customer.reduceByKey((x,y) => x + y).sortBy(x => x._2);
    a.collect().foreach(println)
  }
   
   def setLine(data: String ) ={
     val split = data.split(",");
     
     val id = split(0).toInt;
     val amount = split(2).toFloat
     
     (id, amount)
   }
}
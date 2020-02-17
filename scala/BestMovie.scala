package com.bbb.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max;

object BestMovie {
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    var filePath = "C:\\sparkData\\Scala\\ml-100k\\u.data";
    val lines = sc.textFile(filePath);//("../ml-100k/u.data")
    
    var mapData = lines.map(setData);
    val onlyFive = mapData.filter(x => x._2 == 5);
    val m = onlyFive.map(x => (x._1, 1));
    val a = m.reduceByKey((x,y) => x+y);
    val f= a.reduceByKey((x,y) => max(x,y));
    f.collect().foreach(println)
    
  }
  
  def setData(data: String) = {
     val split = data.split("\t");
     val moveId = split(1).toInt;
     val rating = split(2).toInt;
    (moveId, rating);
  }
}
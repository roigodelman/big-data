package com.frends.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max;

object MinTemp {
   def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    var filePath = "C:\\sparkData\\Scala\\SparkScala3\\1800.csv";
    val lines = sc.textFile(filePath);//("../ml-100k/u.data")
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val temp = lines.map(cleanData)
    val onlyMin = temp.filter(x => x._2 == "TMAX");
    val stationTemps = onlyMin.map(x => (x._1, x._3)); 
    val minTemp = stationTemps.reduceByKey((x,y) => max(x,y));
   minTemp.collect().sorted.foreach(println)
   
    //friends.foreach(println);
  }
    
    def cleanData(line: String) = {
      val fields = line.split(",");
      val id = fields(0);
      val entryType = fields(2);
      val temp = fields(3).toFloat * 0.1f*(9.0f/5.0f) + 32.0f;
      
      (id , entryType, temp);
    }
}
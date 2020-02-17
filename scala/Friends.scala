package com.frends.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Friends {
    def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    var filePath = "C:\\sparkData\\Scala\\SparkScala3\\fakefriends.csv";
    val lines = sc.textFile(filePath);//("../ml-100k/u.data")
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val friends = lines.map(cleanData)
    
    val totoalsByAge = friends.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2));
    val avrByAge = totoalsByAge.mapValues((x) => x._1/x._2);
    var res = avrByAge.filter(x => x._1 > 21);
    // Count up how many times each value (rating) occurs
    //val results = friends.gr
    var results = res.collect();
    // Sort the resulting map of (rating, count) tuples
   // val sortedResults = results.toSeq.sortBy(_._1)
    
    // Print each result on its own line.
    results.sorted.foreach(println)
    //friends.foreach(println);
  }
    
    def cleanData(line: String) = {
      val fields = line.split(",");
      val age = fields(2).toInt;
      val numFriends = fields(3).toInt;
      
      (age , numFriends);
    }
}
package com.frends.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CountWords {
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    // Load up each line of the ratings data into an RDD
    var filePath = "C:\\sparkData\\Scala\\SparkScala3\\book.txt";
    val lines = sc.textFile(filePath);
    var words = lines.flatMap(x => x.split("\\W+"))
    
    val lowerCaseWords = words.map(x => x.toLowerCase());
    
    val wordCounts = lowerCaseWords.map(x => (x, 1)).reduceByKey((x, y) => x+y);
    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()
    //var countWords = lowerCaseWords.countByValue();
    wordCountsSorted.collect().foreach(println)
  }
}
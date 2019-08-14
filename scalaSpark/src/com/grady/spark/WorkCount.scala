package com.grady.spark

import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author rociss 
  * @version 1.0, on 20:10 2019/6/6.
  */
object WorkCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("wordcount")
    //      .setMaster("minimal126")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://minimal126:9000/spark/wordCount", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordCount => println(wordCount._1 + "  appeared " + wordCount._2 + " times."))


    val ssc = new StreamingContext(conf, Durations.seconds(3));
    val sscLine = ssc.socketTextStream("localhost", 9999)
  }

}

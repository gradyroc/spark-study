package com.grady.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author rociss 
  * @version 1.0, on 0:00 2019/6/13.
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("wordcount");

    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap {
      _.split(" ")
    }
    val pairs = words.map { word => (word, 1) }
    val wordCount = pairs.reduceByKey(_ + _)

    Thread.sleep(5000)
    wordCount.print()


    ssc.start()
    ssc.awaitTermination()


  }

}





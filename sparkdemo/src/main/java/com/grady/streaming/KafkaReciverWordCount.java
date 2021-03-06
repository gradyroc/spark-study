package com.grady.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author rociss
 * @version 1.0, on 19:53 2019/6/13.
 */
public class KafkaReciverWordCount {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("kafkawordcount")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        final Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put("wordCount", 1);
        JavaPairInputDStream<String, String> lines = KafkaUtils.createStream(jssc, "ip:2181", "defaultConsumer", topicThreadMap);
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<Tuple2<String, String>, String>() {

                    public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
                        return (Iterator<String>) Arrays.asList(tuple._2.split(" "));
                    }
                });


        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });


        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
















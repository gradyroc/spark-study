package com.grady.streaming;

import kafka.serializer.StringDecoder;
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
import scala.Array;
import scala.Tuple2;

import java.util.*;

/**
 * @author rociss
 * 基于kafka Direct方式的实时wordcount程序
 * @version 1.0, on 1:21 2019/6/14.
 */
public class KafkaDirectWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("kafkawordcount")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //创建一份kafka的参数map
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "ip:9092");
        //创建set保存topic name
        Set<String> topics = new HashSet<String>();
        topics.add("wordcount");


        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<Tuple2<String, String>, String>() {
                    private static final long serialVersionUID = 1L;

                    public Iterator<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        return (Iterator<String>) Arrays.asList(stringStringTuple2._2.split(" "));
                    }
                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });


        JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {

                        return v1 + v2;
                    }
                });
        wordCount.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}

























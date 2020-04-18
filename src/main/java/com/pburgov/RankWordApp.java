package com.pburgov;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class RankWordApp {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputData = sc.textFile("src/main/resources/subtitles/input-spring.txt");
        /*
        JavaRDD<String> sentences = inputData.map(s -> s.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> noBlankLines = sentences.filter(sentence -> sentence.trim().length() > 0);

        JavaRDD<String> words = noBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> blankWordRemoved = words.filter(word-> word.trim().length()>0);

        JavaRDD<String> noBoringWords = blankWordRemoved.filter(word -> Util.isNotBoring(word));

        JavaPairRDD<String, Long> pairRDD = noBoringWords.mapToPair(word -> new Tuple2<>(word, 1L));

        JavaPairRDD<String, Long> reduceRDD =    pairRDD.reduceByKey((value1, value2)->value1+value2);

        JavaPairRDD<Long,String> switchedRDD = reduceRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sortedRDD = switchedRDD.sortByKey(false);

        List<Tuple2<Long,String>> results = sortedRDD.take(10);

        results.forEach(System.out::println);
        */


        inputData
                .map(s -> s.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .filter(sentence -> sentence.trim().length() > 0)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(word -> word.trim().length() > 0)
                .filter(word -> Util.isNotBoring(word))
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10).forEach(System.out::println);

        sc.close();

    }
}

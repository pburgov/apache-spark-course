package com.pburgov;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairRDDApp {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

        JavaPairRDD<String, Long> pairRDD = originalLogMessages.mapToPair(rawValue -> {
            String[] columns = rawValue.split(":");
            String level = columns[0].trim();
            return new Tuple2<>(level, 1L);
        });

        JavaPairRDD<String, Long> sumRDD = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
        sumRDD.collect().forEach(tuple -> System.out.println("La frecuencia de " + tuple._1  + " es " + tuple._2));

        sc.close();

    }
}

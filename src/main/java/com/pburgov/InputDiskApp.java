package com.pburgov;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class InputDiskApp {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputData = sc.textFile("src/main/resources/subtitles/input.txt");

        inputData
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> Util.isNotBoring(word))
                .collect().forEach(System.out::println);


        sc.close();

    }
}

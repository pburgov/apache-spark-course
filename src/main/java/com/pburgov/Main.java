package com.pburgov;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> myRDD = sc.parallelize(inputData);

        //// Reduce
        Integer result = myRDD.reduce((a, b) -> a + b);
        System.out.println("Result: " + result);

        //// Map
        JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));
        sqrtRDD.collect().forEach(System.out::println);

        //// Count Function
        System.out.println("sqrtRDD Size -> " + sqrtRDD.count());

        //// Count by map and reduce
        JavaRDD<Integer> countRDD = sqrtRDD.map(value -> 1);
        Integer numOfSqrt = countRDD.reduce((v1, v2) -> v1 + v2);
        System.out.println("sqrtRDD Size -> " + numOfSqrt);

        //// Tuples
        JavaRDD<Tuple2<Integer,Double>> tupleRDD = myRDD.map(value -> new Tuple2<>(value, Math.sqrt(value)));
        tupleRDD.collect().forEach(System.out::println);


        sc.close();

    }
}

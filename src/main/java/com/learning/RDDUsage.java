package com.learning;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RDDUsage {

    public static void main(String[] args) {
        SparkSession sparkSession =
                SparkUtils.createDefaultSparkSessionFor(RDDUsage.class);
        List<Integer> naturalNumbers = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        //Using low level sparkContext to create RDD
        JavaRDD<Integer> rddNaturalNumbes = sparkContext.parallelize(naturalNumbers, 60);
        rddNaturalNumbes.foreach(i -> System.out.println(i));

        List<Integer> naturalNumbersCollected = rddNaturalNumbes.collect();
        System.out.printf("After collection %s \n", naturalNumbersCollected.toString());

        JavaRDD<Double> transformedRDD = rddNaturalNumbes.map(i -> i * 1.0);
        transformedRDD.foreach(d -> System.out.println(d));

        JavaRDD<List<Double>> partitions = transformedRDD.glom();
        partitions.foreach((List<Double> partitionNumbers) -> System.out.println(partitionNumbers.toString()));
        sparkSession.stop();
    }
}

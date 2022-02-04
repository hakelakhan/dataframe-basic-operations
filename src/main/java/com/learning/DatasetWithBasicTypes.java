package com.learning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.col;

public class DatasetWithBasicTypes {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkUtils.createDefaultSparkSessionFor(DatasetWithBasicTypes.class);
        List<Integer> naturalNumbers = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());
        Dataset<Integer> naturalNumbersDataset = sparkSession.createDataset(naturalNumbers, Encoders.INT()).filter(number -> number % 2 == 0);
        naturalNumbersDataset.show(naturalNumbers.size());
        naturalNumbersDataset.printSchema();

        List<Tuple3<Integer, String, String>> tuples = Arrays.asList(
                new Tuple3<>(1, "One", "Single"),
                new Tuple3<>(2, "Two", "Double"),
                new Tuple3<>(3, "Three", "Triple")
        );
        Dataset<Tuple3<Integer, String, String>> tuple3Dataset =
                sparkSession.createDataset(tuples, Encoders.tuple(
                        Encoders.INT(),
                        Encoders.STRING(),
                        Encoders.STRING()
                    )
                );
        tuple3Dataset.printSchema();
        tuple3Dataset.show();
        tuple3Dataset.select(col("_1"), col("_2")).where(col("_1").gt(2)).show();

        sparkSession.stop();
    }
}

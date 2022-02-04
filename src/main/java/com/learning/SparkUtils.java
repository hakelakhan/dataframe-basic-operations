package com.learning;

import org.apache.spark.sql.SparkSession;

public class SparkUtils {
    public static SparkSession createDefaultSparkSessionFor(Class clazz) {
        return SparkSession.builder()
                .master("local[4]") //working in local single node using 4 worker threads
                .appName(clazz.getSimpleName())
                .getOrCreate();
    }
}

package com.learning;

import data.DataProvider;
import org.apache.spark.sql.*;
import pojo.Location;
import pojo.Supplier;

import static org.apache.spark.sql.functions.col;

public class DatasetConversionDemo {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkUtils.createDefaultSparkSessionFor(DatasetConversionDemo.class);

        Dataset<Supplier> supplierDataset = DataProvider.getSuppliersDataset(sparkSession);
        supplierDataset.show();
        supplierDataset.printSchema();

        //convert Dataset to Dataframe;
        Dataset<Row> smallerDF = supplierDataset.select(col("city"), col("country")).filter(col("id").lt(2));
        smallerDF.show();
        smallerDF.printSchema();
        Encoder<Location> locationEncoder = Encoders.bean(Location.class);
        //convert Dataframe to another Dataset
        Dataset<Location> datasetFromDataFrame = smallerDF.as(locationEncoder);
        datasetFromDataFrame.show();
        sparkSession.stop();
    }
}

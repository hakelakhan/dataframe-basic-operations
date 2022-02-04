package com.learning;

import data.DataProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class DataFrameCreation {

    public static void main(String[] args) {
        SparkSession spark = SparkUtils.createDefaultSparkSessionFor(DataFrameCreation.class);


        //DataFrame from Dataset
        Dataset<Row> fromDatasetDF = DataProvider.getSuppliersDataset(spark).where(col("id").gt(1)).select(col("name"), col("city"));
        fromDatasetDF.show();

        //DataFrame from Data and schema
        List<Row> data = Arrays.asList(
                RowFactory.create(100, "Lakhan Hake", "ICICI Bank", java.sql.Date.valueOf(LocalDate.of(1992, 12, 8)), 20_000.0),
                RowFactory.create(101, "Renuka Hake", "HDFC Bank", java.sql.Date.valueOf(LocalDate.of(2000, 3, 13)), 50_000.0),
                RowFactory.create(102, "Omkar Hake", "IDFC Bank", java.sql.Date.valueOf(LocalDate.of(1996, 3, 13)), 40_000.0)
        );
        StructField[] structFields = {
                DataTypes.createStructField("AccountNumber", DataTypes.IntegerType, false),
                DataTypes.createStructField("FullName", DataTypes.StringType, false),
                DataTypes.createStructField("Bank Name", DataTypes.StringType, false),
                DataTypes.createStructField("Date of Birth", DataTypes.DateType, true),
                DataTypes.createStructField("Balance", DataTypes.DoubleType, false)
        };

        StructType schema= DataTypes.createStructType(structFields);

        Dataset<Row> bankUsersDF = spark.createDataFrame(data, schema);
        bankUsersDF.printSchema();
        bankUsersDF.show();

        bankUsersDF.filter(col("Bank Name").equalTo("ICICI Bank")).show();
        //TODO dataframe from csv file

        spark.stop();
    }
}

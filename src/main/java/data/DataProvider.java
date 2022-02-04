package data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import pojo.Supplier;

import java.util.Arrays;
import java.util.List;

public interface DataProvider {
    static List<Supplier> getSuppliers() {
        return Arrays.asList(
                new Supplier(1, "Exotic Liquid", "Londona", "UK"),
                new Supplier(2, "New Orleans Cajun Delights", "New Orleans", "USA"),
                new Supplier(3, "Pavlova, Ltd.", "Melbourne", "Australia")
        );
    }

    static Dataset<Supplier> getSuppliersDataset(SparkSession spark) {
        return spark.createDataset(getSuppliers(), Encoders.bean(Supplier.class));

    }
}

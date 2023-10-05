package com.cs523;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;

public class SparkSQLAnalyze {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("MyApp").setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        StructType schema = new StructType()
                .add("event_time", DataTypes.StringType)
                .add("event_type", DataTypes.StringType)
                .add("product_id", DataTypes.LongType)
                .add("category_id", DataTypes.LongType)
                .add("category_code", DataTypes.StringType)
                .add("brand", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("user_id", DataTypes.LongType)
                .add("user_session", DataTypes.StringType);
        // Read the CSV file into a DataFrame using the schema\
        Dataset<Row> df = spark.read().option("header", "true").schema(schema).csv(args[0]);

        df.printSchema();

        // Register the DataFrame as a temporary table
        df.createOrReplaceTempView("electronics");

        // Query the total price

        Dataset<Row> views_per_brand = spark.sql(
                "SELECT brand, COUNT(event_type) AS view_count FROM electronics WHERE event_type='view' GROUP BY brand ORDER BY view_count DESC");

        views_per_brand.show();

        Dataset<Row> cart_per_brand = spark.sql(
                "SELECT brand, COUNT(event_type) AS cart_count FROM electronics WHERE event_type='cart' GROUP BY brand ORDER BY cart_count DESC");

        cart_per_brand.show();

        Dataset<Row> purchase_per_brand = spark.sql(
                "SELECT brand, COUNT(event_type) AS purchase_count FROM electronics WHERE event_type='purchase' GROUP BY brand ORDER BY purchase_count DESC");

        purchase_per_brand.show();

        TableUtils utils = new TableUtils();
        utils.createAnalyticsTable();

        saveDatasetToHBase(views_per_brand, "view_count");
        saveDatasetToHBase(cart_per_brand, "cart_count");
        saveDatasetToHBase(purchase_per_brand, "purchase_count");
    }

    static void saveDatasetToHBase(Dataset<Row> result, String col) {
        result.javaRDD()
                .mapToPair((Row row) -> new Tuple2<String, Long>(row.getString(0),
                        row.getLong(1)))
                .foreachPartition(iterator -> {
                    new HBaseWriter().writeBrandReport(IteratorUtils.toList(iterator), col);
                });
    }
}

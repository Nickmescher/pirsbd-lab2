package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class OnlineRetailApp {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: OnlineRetailApp <inputCsvPath> <outputDir>");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputDir = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("OnlineRetailApp")
                .getOrCreate();

        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", "true")
                .option("escape", "\"")
                .csv(inputPath);

        // Нормализуем типы и отфильтруем отмены (InvoiceNo начинается с 'C') и строки без CustomerID.
        Dataset<Row> df = raw
                .withColumn("Quantity", col("Quantity").cast(DataTypes.IntegerType))
                .withColumn("UnitPrice", col("UnitPrice").cast(DataTypes.DoubleType))
                .withColumn("TotalPrice", col("Quantity").multiply(col("UnitPrice")))
                .filter(col("InvoiceNo").isNotNull())
                .filter(not(col("InvoiceNo").startsWith("C")))
                .filter(col("Quantity").gt(0))
                .filter(col("UnitPrice").gt(0))
                .dropDuplicates();

        // 1. Топ-5 самых популярных товаров по общему количеству проданных единиц.
        Dataset<Row> topProducts = df
                .groupBy(col("StockCode"), col("Description"))
                .agg(sum("Quantity").alias("TotalQuantity"))
                .orderBy(col("TotalQuantity").desc())
                .limit(5);

        System.out.println("=== TOP-5 PRODUCTS BY QUANTITY ===");
        topProducts.show(false);

        topProducts.coalesce(1)
                .write().mode("overwrite").option("header", "true")
                .csv(outputDir + "/top5_products");

        // 2. Статистика по клиентам: число заказов, общая сумма, средний чек.
        Dataset<Row> customersWithCheck = df
                .filter(col("CustomerID").isNotNull())
                .groupBy(col("CustomerID"), col("InvoiceNo"))
                .agg(sum("TotalPrice").alias("InvoiceTotal"));

        Dataset<Row> customerStats = customersWithCheck
                .groupBy(col("CustomerID"))
                .agg(
                        countDistinct(col("InvoiceNo")).alias("OrdersCount"),
                        sum(col("InvoiceTotal")).alias("TotalSpent"),
                        avg(col("InvoiceTotal")).alias("AverageCheck")
                )
                .orderBy(col("TotalSpent").desc());

        System.out.println("=== CUSTOMER STATS (first 20 rows) ===");
        customerStats.show(20, false);

        customerStats.coalesce(1)
                .write().mode("overwrite").option("header", "true")
                .csv(outputDir + "/customer_stats");

        spark.stop();
    }
}

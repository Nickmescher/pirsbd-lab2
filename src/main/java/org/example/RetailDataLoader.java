package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Загружает сырые данные из CSV.
 * Контракт: явная схема (без inferSchema), типы Quantity=Integer, UnitPrice=Double.
 */
public interface RetailDataLoader {

    Dataset<Row> load(SparkSession spark, String inputPath);
}

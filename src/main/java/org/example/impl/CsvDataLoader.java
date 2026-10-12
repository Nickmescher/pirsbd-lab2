package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.example.RetailDataLoader;

import static org.example.Columns.CUSTOMER_ID;
import static org.example.Columns.DESCRIPTION;
import static org.example.Columns.INVOICE_NO;
import static org.example.Columns.QUANTITY;
import static org.example.Columns.STOCK_CODE;
import static org.example.Columns.UNIT_PRICE;

/**
 * Чтение с явной схемой: минус полный проход inferSchema по файлу.
 * Файл в кодировке ISO-8859-1 (символ £), кавычки внутри полей удвоены.
 */
public class CsvDataLoader implements RetailDataLoader {

    private static final StructType SCHEMA = new StructType()
            .add(INVOICE_NO, DataTypes.StringType)
            .add(STOCK_CODE, DataTypes.StringType)
            .add(DESCRIPTION, DataTypes.StringType)
            .add(QUANTITY, DataTypes.IntegerType)
            .add("InvoiceDate", DataTypes.StringType)
            .add(UNIT_PRICE, DataTypes.DoubleType)
            .add(CUSTOMER_ID, DataTypes.StringType)
            .add("Country", DataTypes.StringType);

    @Override
    public Dataset<Row> load(SparkSession spark, String inputPath) {
        return spark.read()
                .option("header", "true")
                .schema(SCHEMA)
                .option("charset", "ISO-8859-1")
                .option("escape", "\"")
                .csv(inputPath);
    }
}

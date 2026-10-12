package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.ResultWriter;

/** Один CSV-файл на выгрузку с заголовком. */
public class CsvResultWriter implements ResultWriter {

    @Override
    public void write(Dataset<Row> result, String outputPath) {
        result.coalesce(1)
                .write().mode("overwrite").option("header", "true")
                .csv(outputPath);
    }
}

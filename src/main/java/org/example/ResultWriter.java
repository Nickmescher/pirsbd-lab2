package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Пишет результат одним CSV-файлом с заголовком (coalesce(1), overwrite). */
public interface ResultWriter {

    void write(Dataset<Row> result, String outputPath);
}

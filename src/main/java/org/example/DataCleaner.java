package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Применяет правила качества данных.
 * Контракт: отсев C-отмен, Quantity &lt;= 0, UnitPrice &lt;= 0, пустых CustomerID,
 * удаление точных дубликатов, TotalPrice = Quantity * UnitPrice,
 * проекция только нужных колонок.
 */
public interface DataCleaner {

    Dataset<Row> clean(Dataset<Row> raw);
}

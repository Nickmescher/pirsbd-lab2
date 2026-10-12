package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Топ-N товаров по проданным единицам.
 * Контракт: группировка (StockCode, Description), sum(Quantity),
 * сортировка по убыванию. Выход: StockCode, Description, TotalQuantity.
 */
public interface TopProductsAnalyzer {

    Dataset<Row> analyze(Dataset<Row> clean, int topN);
}

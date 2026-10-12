package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Статистика по клиентам.
 * Контракт: OrdersCount = число уникальных InvoiceNo,
 * TotalSpent = сумма TotalPrice, AverageCheck = среднее по суммам заказов
 * (avg(InvoiceTotal)), а не среднее по строкам.
 */
public interface CustomerStatsAnalyzer {

    Dataset<Row> analyze(Dataset<Row> clean);
}

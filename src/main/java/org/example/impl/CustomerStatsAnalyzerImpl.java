package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.CustomerStatsAnalyzer;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;
import static org.example.Columns.AVERAGE_CHECK;
import static org.example.Columns.CUSTOMER_ID;
import static org.example.Columns.INVOICE_NO;
import static org.example.Columns.INVOICE_TOTAL;
import static org.example.Columns.ORDERS_COUNT;
import static org.example.Columns.TOTAL_PRICE;
import static org.example.Columns.TOTAL_SPENT;

/** Средний чек — среднее по суммам заказов (InvoiceTotal), не по строкам. */
public class CustomerStatsAnalyzerImpl implements CustomerStatsAnalyzer {

    @Override
    public Dataset<Row> analyze(Dataset<Row> clean) {
        Dataset<Row> invoiceTotals = clean
                .groupBy(col(CUSTOMER_ID), col(INVOICE_NO))
                .agg(sum(TOTAL_PRICE).alias(INVOICE_TOTAL));

        return invoiceTotals
                .groupBy(col(CUSTOMER_ID))
                .agg(
                        countDistinct(INVOICE_NO).alias(ORDERS_COUNT),
                        sum(INVOICE_TOTAL).alias(TOTAL_SPENT),
                        avg(INVOICE_TOTAL).alias(AVERAGE_CHECK)
                )
                .orderBy(col(TOTAL_SPENT).desc());
    }
}

package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.DataCleaner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.example.Columns.CUSTOMER_ID;
import static org.example.Columns.DESCRIPTION;
import static org.example.Columns.INVOICE_NO;
import static org.example.Columns.QUANTITY;
import static org.example.Columns.STOCK_CODE;
import static org.example.Columns.TOTAL_PRICE;
import static org.example.Columns.UNIT_PRICE;

/** Правила DQ: отсев отмен и аномалий, дедупликация, TotalPrice, проекция. */
public class DqDataCleaner implements DataCleaner {

    @Override
    public Dataset<Row> clean(Dataset<Row> raw) {
        return raw
                .withColumn(TOTAL_PRICE, col(QUANTITY).multiply(col(UNIT_PRICE)))
                .filter(col(INVOICE_NO).isNotNull())
                .filter(not(col(INVOICE_NO).startsWith("C")))
                .filter(col(QUANTITY).gt(0))
                .filter(col(UNIT_PRICE).gt(0))
                .filter(col(CUSTOMER_ID).isNotNull())
                .dropDuplicates()
                .select(INVOICE_NO, STOCK_CODE, DESCRIPTION, QUANTITY,
                        CUSTOMER_ID, TOTAL_PRICE);
    }
}

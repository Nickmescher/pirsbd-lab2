package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.TopProductsAnalyzer;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.example.Columns.DESCRIPTION;
import static org.example.Columns.QUANTITY;
import static org.example.Columns.STOCK_CODE;
import static org.example.Columns.TOTAL_QUANTITY;

public class TopProductsAnalyzerImpl implements TopProductsAnalyzer {

    @Override
    public Dataset<Row> analyze(Dataset<Row> clean, int topN) {
        return clean
                .groupBy(col(STOCK_CODE), col(DESCRIPTION))
                .agg(sum(QUANTITY).alias(TOTAL_QUANTITY))
                .orderBy(col(TOTAL_QUANTITY).desc())
                .limit(topN);
    }
}

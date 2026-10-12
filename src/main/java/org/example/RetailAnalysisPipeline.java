package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Связывает этапы: load → clean → cache → analyze → write. */
public final class RetailAnalysisPipeline {

    private final RetailDataLoader loader;
    private final DataCleaner cleaner;
    private final TopProductsAnalyzer productAnalyzer;
    private final CustomerStatsAnalyzer customerAnalyzer;
    private final ResultWriter writer;

    public RetailAnalysisPipeline(RetailDataLoader loader,
                                  DataCleaner cleaner,
                                  TopProductsAnalyzer productAnalyzer,
                                  CustomerStatsAnalyzer customerAnalyzer,
                                  ResultWriter writer) {
        this.loader = loader;
        this.cleaner = cleaner;
        this.productAnalyzer = productAnalyzer;
        this.customerAnalyzer = customerAnalyzer;
        this.writer = writer;
    }

    public void run(SparkSession spark, String inputPath, String outputDir, int topN) {
        Dataset<Row> clean = cleaner.clean(loader.load(spark, inputPath)).cache();

        Dataset<Row> topProducts = productAnalyzer.analyze(clean, topN);
        topProducts.show(false);
        writer.write(topProducts, outputDir + "/top_products");

        Dataset<Row> customerStats = customerAnalyzer.analyze(clean);
        customerStats.show(20, false);
        writer.write(customerStats, outputDir + "/customer_stats");

        clean.unpersist();
    }
}

package org.example;

import org.apache.spark.sql.SparkSession;
import org.example.impl.CsvDataLoader;
import org.example.impl.CsvResultWriter;
import org.example.impl.CustomerStatsAnalyzerImpl;
import org.example.impl.DqDataCleaner;
import org.example.impl.TopProductsAnalyzerImpl;

public class OnlineRetailApp {

    private static final int DEFAULT_TOP_N = 5;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: OnlineRetailApp <inputCsvPath> <outputDir> [topN]");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputDir = args[1];
        int topN = parseTopN(args);

        SparkSession spark = SparkSession.builder()
                .appName("OnlineRetailApp")
                // На 1 воркере 200 shuffle-партиций по умолчанию избыточны.
                .config("spark.sql.shuffle.partitions", "16")
                .getOrCreate();

        new RetailAnalysisPipeline(
                new CsvDataLoader(),
                new DqDataCleaner(),
                new TopProductsAnalyzerImpl(),
                new CustomerStatsAnalyzerImpl(),
                new CsvResultWriter()
        ).run(spark, inputPath, outputDir, topN);

        spark.stop();
    }

    /** topN — необязательный третий аргумент, по умолчанию {@value #DEFAULT_TOP_N}. */
    static int parseTopN(String[] args) {
        if (args.length < 3) {
            return DEFAULT_TOP_N;
        }
        int topN = Integer.parseInt(args[2]);
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be positive, got: " + topN);
        }
        return topN;
    }
}

package org.example;

import java.io.IOException;
import java.util.Locale;

import org.apache.spark.sql.SparkSession;
import org.example.impl.CsvDataLoader;
import org.example.impl.CsvResultWriter;
import org.example.impl.CustomerStatsAnalyzerImpl;
import org.example.impl.DqDataCleaner;
import org.example.impl.TopProductsAnalyzerImpl;

public class OnlineRetailApp {

    private static final int DEFAULT_TOP_N = 5;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: OnlineRetailApp <topn|analytics> <inputCsvPath> <outputDir> [topN]");
            System.exit(1);
        }
        String mode = parseMode(args);
        String inputPath = args[1];
        String outputDir = args[2];
        int topN = parseTopN(args);

        SparkSession spark = SparkSession.builder()
                .appName("OnlineRetailApp")
                // На 1 воркере 200 shuffle-партиций по умолчанию избыточны.
                .config("spark.sql.shuffle.partitions", "16")
                .getOrCreate();

        switch (mode) {
            case "topn" -> runTopN(spark, inputPath, outputDir, topN);
            case "analytics" -> runAnalytics(spark, inputPath, outputDir);
        }

        spark.stop();
    }

    /** Режим работы: topn — топ-N товаров и статистика клиентов, analytics — отчёт о качестве данных. */
    static String parseMode(String[] args) {
        if (args.length < 1 || args[0] == null || args[0].isBlank()) {
            throw new IllegalArgumentException("mode is required: topn|analytics");
        }
        String mode = args[0].toLowerCase(Locale.ROOT);
        if (!mode.equals("topn") && !mode.equals("analytics")) {
            throw new IllegalArgumentException("unknown mode: " + args[0] + " (expected topn|analytics)");
        }
        return mode;
    }

    /** topN — необязательный четвёртый аргумент (только для режима topn), по умолчанию {@value #DEFAULT_TOP_N}. */
    static int parseTopN(String[] args) {
        if (args.length < 4) {
            return DEFAULT_TOP_N;
        }
        int topN = Integer.parseInt(args[3]);
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be positive, got: " + topN);
        }
        return topN;
    }

    // Режим topn: топ-N популярных товаров и статистика по клиентам.
    private static void runTopN(SparkSession spark, String inputPath, String outputDir, int topN) {
        new RetailAnalysisPipeline(
                new CsvDataLoader(),
                new DqDataCleaner(),
                new TopProductsAnalyzerImpl(),
                new CustomerStatsAnalyzerImpl(),
                new CsvResultWriter()
        ).run(spark, inputPath, outputDir, topN);
    }

    // Режим analytics: markdown-отчёт о качестве данных.
    private static void runAnalytics(SparkSession spark, String inputPath, String outputDir) {
        try {
            DataQualityApp.run(spark, inputPath, outputDir + "/data_quality_report.md");
        } catch (IOException e) {
            System.err.println("Failed to write data quality report: " + e.getMessage());
            System.exit(1);
        }
    }
}

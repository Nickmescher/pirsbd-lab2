package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.example.OnlineRetailAppv2.Columns.*;

public class OnlineRetailV2App {

    private static final int TOP_N = 5;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: OnlineRetailV2App <inputCsvPath> <outputDir>");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputDir = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("OnlineRetailV2App")
                // На 1 воркере 200 shuffle-партиций по умолчанию избыточны.
                .config("spark.sql.shuffle.partitions", "16")
                .getOrCreate();

        new OnlineRetailAppv2.RetailAnalysisPipeline(
                new DefaultDataLoader(),
                new DqDataCleaner(),
                new TopProductsAnalyzerImpl(),
                new CustomerStatsAnalyzerImpl(),
                new CsvResultWriter()
        ).run(spark, inputPath, outputDir, TOP_N);

        spark.stop();
    }

    /**
     * Чтение с явной схемой: минус полный проход inferSchema по файлу.
     * Файл в кодировке ISO-8859-1 (символ £), кавычки внутри полей удвоены.
     */
    static class DefaultDataLoader implements OnlineRetailAppv2.RetailDataLoader {
        private static final StructType SCHEMA = new StructType()
                .add(INVOICE_NO, DataTypes.StringType)
                .add(STOCK_CODE, DataTypes.StringType)
                .add(DESCRIPTION, DataTypes.StringType)
                .add(QUANTITY, DataTypes.IntegerType)
                .add("InvoiceDate", DataTypes.StringType)
                .add(UNIT_PRICE, DataTypes.DoubleType)
                .add(CUSTOMER_ID, DataTypes.StringType)
                .add("Country", DataTypes.StringType);

        @Override
        public Dataset<Row> load(SparkSession spark, String inputPath) {
            return spark.read()
                    .option("header", "true")
                    .schema(SCHEMA)
                    .option("charset", "ISO-8859-1")
                    .option("escape", "\"")
                    .csv(inputPath);
        }
    }

    /**
     * Правила DQ из отчёта по профилированию:
     * отсев C-отмен, Quantity <= 0, UnitPrice <= 0, пустых CustomerID,
     * удаление точных дубликатов, проекция только нужных колонок.
     */
    static class DqDataCleaner implements OnlineRetailAppv2.DataCleaner {
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

    static class TopProductsAnalyzerImpl implements OnlineRetailAppv2.TopProductsAnalyzer {
        @Override
        public Dataset<Row> analyze(Dataset<Row> clean, int topN) {
            return clean
                    .groupBy(col(STOCK_CODE), col(DESCRIPTION))
                    .agg(sum(QUANTITY).alias(TOTAL_QUANTITY))
                    .orderBy(col(TOTAL_QUANTITY).desc())
                    .limit(topN);
        }
    }

    /** Средний чек — среднее по суммам заказов (InvoiceTotal), не по строкам. */
    static class CustomerStatsAnalyzerImpl implements OnlineRetailAppv2.CustomerStatsAnalyzer {
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

    /** Один CSV-файл на выгрузку с заголовком. */
    static class CsvResultWriter implements OnlineRetailAppv2.ResultWriter {
        @Override
        public void write(Dataset<Row> result, String outputPath) {
            result.coalesce(1)
                    .write().mode("overwrite").option("header", "true")
                    .csv(outputPath);
        }
    }
}

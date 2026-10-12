package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.example.OnlineRetailApp.Columns.*;

public class OnlineRetailApp {

    private static final int TOP_N = 5;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: OnlineRetailApp <inputCsvPath> <outputDir>");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputDir = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("OnlineRetailApp")
                // На 1 воркере 200 shuffle-партиций по умолчанию избыточны.
                .config("spark.sql.shuffle.partitions", "16")
                .getOrCreate();

        new RetailAnalysisPipeline(
                new DefaultDataLoader(),
                new DqDataCleaner(),
                new TopProductsAnalyzerImpl(),
                new CustomerStatsAnalyzerImpl(),
                new CsvResultWriter()
        ).run(spark, inputPath, outputDir, TOP_N);

        spark.stop();
    }

    /** Имена колонок датасета Online Retail — единая точка истины. */
    public static final class Columns {
        public static final String INVOICE_NO = "InvoiceNo";
        public static final String STOCK_CODE = "StockCode";
        public static final String DESCRIPTION = "Description";
        public static final String QUANTITY = "Quantity";
        public static final String UNIT_PRICE = "UnitPrice";
        public static final String CUSTOMER_ID = "CustomerID";
        public static final String TOTAL_PRICE = "TotalPrice";
        public static final String TOTAL_QUANTITY = "TotalQuantity";
        public static final String INVOICE_TOTAL = "InvoiceTotal";
        public static final String ORDERS_COUNT = "OrdersCount";
        public static final String TOTAL_SPENT = "TotalSpent";
        public static final String AVERAGE_CHECK = "AverageCheck";

        private Columns() {
        }
    }

    /**
     * Загружает сырые данные из CSV.
     * Контракт: явная схема (без inferSchema), типы Quantity=Integer, UnitPrice=Double.
     */
    public interface RetailDataLoader {
        Dataset<Row> load(SparkSession spark, String inputPath);
    }

    /**
     * Применяет правила качества данных.
     * Контракт: отсев C-отмен, Quantity <= 0, UnitPrice <= 0, пустых CustomerID,
     * удаление точных дубликатов, TotalPrice = Quantity * UnitPrice,
     * проекция только нужных колонок.
     */
    public interface DataCleaner {
        Dataset<Row> clean(Dataset<Row> raw);
    }

    /**
     * Топ-N товаров по проданным единицам.
     * Контракт: группировка (StockCode, Description), sum(Quantity),
     * сортировка по убыванию. Выход: StockCode, Description, TotalQuantity.
     */
    public interface TopProductsAnalyzer {
        Dataset<Row> analyze(Dataset<Row> clean, int topN);
    }

    /**
     * Статистика по клиентам.
     * Контракт: OrdersCount = число уникальных InvoiceNo,
     * TotalSpent = сумма TotalPrice, AverageCheck = среднее по суммам заказов
     * (avg(InvoiceTotal)), а не среднее по строкам.
     */
    public interface CustomerStatsAnalyzer {
        Dataset<Row> analyze(Dataset<Row> clean);
    }

    /** Пишет результат одним CSV-файлом с заголовком (coalesce(1), overwrite). */
    public interface ResultWriter {
        void write(Dataset<Row> result, String outputPath);
    }

    /** Связывает этапы: load → clean → cache → analyze → write. */
    public static final class RetailAnalysisPipeline {

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
            writer.write(topProducts, outputDir + "/top5_products");

            Dataset<Row> customerStats = customerAnalyzer.analyze(clean);
            customerStats.show(20, false);
            writer.write(customerStats, outputDir + "/customer_stats");

            clean.unpersist();
        }
    }

    /**
     * Чтение с явной схемой: минус полный проход inferSchema по файлу.
     * Файл в кодировке ISO-8859-1 (символ £), кавычки внутри полей удвоены.
     */
    static class DefaultDataLoader implements RetailDataLoader {
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

    static class DqDataCleaner implements DataCleaner {
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

    static class TopProductsAnalyzerImpl implements TopProductsAnalyzer {
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
    static class CustomerStatsAnalyzerImpl implements CustomerStatsAnalyzer {
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
    static class CsvResultWriter implements ResultWriter {
        @Override
        public void write(Dataset<Row> result, String outputPath) {
            result.coalesce(1)
                    .write().mode("overwrite").option("header", "true")
                    .csv(outputPath);
        }
    }
}

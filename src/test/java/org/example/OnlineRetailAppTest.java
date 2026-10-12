package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.example.impl.CsvDataLoader;
import org.example.impl.CsvResultWriter;
import org.example.impl.CustomerStatsAnalyzerImpl;
import org.example.impl.DqDataCleaner;
import org.example.impl.TopProductsAnalyzerImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.example.Columns.AVERAGE_CHECK;
import static org.example.Columns.CUSTOMER_ID;
import static org.example.Columns.DESCRIPTION;
import static org.example.Columns.INVOICE_NO;
import static org.example.Columns.ORDERS_COUNT;
import static org.example.Columns.QUANTITY;
import static org.example.Columns.STOCK_CODE;
import static org.example.Columns.TOTAL_PRICE;
import static org.example.Columns.TOTAL_QUANTITY;
import static org.example.Columns.TOTAL_SPENT;
import static org.example.Columns.UNIT_PRICE;
import static org.junit.jupiter.api.Assertions.*;

class OnlineRetailAppTest {

    private static SparkSession spark;

    private static final DataCleaner cleaner = new DqDataCleaner();
    private static final TopProductsAnalyzer topAnalyzer = new TopProductsAnalyzerImpl();
    private static final CustomerStatsAnalyzer customerAnalyzer = new CustomerStatsAnalyzerImpl();
    private static final ResultWriter writer = new CsvResultWriter();

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("OnlineRetailAppTest")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    /** DataFrame в формате "как после загрузки" (без InvoiceDate/Country — cleaner их не трогает). */
    private static Dataset<Row> frame(Object[]... rows) {
        StructType schema = new StructType()
                .add(INVOICE_NO, DataTypes.StringType)
                .add(STOCK_CODE, DataTypes.StringType)
                .add(DESCRIPTION, DataTypes.StringType)
                .add(QUANTITY, DataTypes.IntegerType)
                .add(UNIT_PRICE, DataTypes.DoubleType)
                .add(CUSTOMER_ID, DataTypes.StringType);
        return spark.createDataFrame(Arrays.stream(rows).map(RowFactory::create).toList(), schema);
    }

    @Test
    void cleanerFiltersBadRows() {
        Dataset<Row> clean = cleaner.clean(frame(
                new Object[]{"C1", "A", "cancelled", 5, 2.0, "1"},
                new Object[]{"1", "A", "neg qty", -3, 2.0, "1"},
                new Object[]{"2", "A", "zero price", 3, 0.0, "1"},
                new Object[]{"3", "A", "neg price", 3, -2.0, "1"},
                new Object[]{"4", "A", "no customer", 3, 2.0, null},
                new Object[]{null, "A", "no invoice", 3, 2.0, "1"},
                new Object[]{"5", "A", "good", 3, 2.0, "1"}
        ));

        List<Row> rows = clean.collectAsList();
        assertEquals(1, rows.size());
        assertEquals("5", rows.get(0).getAs(INVOICE_NO));
        assertEquals(6.0, rows.get(0).getDouble(5), 1e-9);
    }

    @Test
    void cleanerRemovesExactDuplicates() {
        Dataset<Row> clean = cleaner.clean(frame(
                new Object[]{"1", "A", "d", 3, 2.0, "1"},
                new Object[]{"1", "A", "d", 3, 2.0, "1"}
        ));
        assertEquals(1, clean.count());
    }

    @Test
    void cleanerProjectsColumns() {
        Dataset<Row> clean = cleaner.clean(frame(new Object[]{"1", "A", "d", 4, 2.5, "1"}));

        assertArrayEquals(
                new String[]{INVOICE_NO, STOCK_CODE, DESCRIPTION, QUANTITY, CUSTOMER_ID, TOTAL_PRICE},
                clean.columns());
        assertEquals(10.0, clean.first().getDouble(5), 1e-9);
    }

    @Test
    void topProductsOrdersByQuantityAndLimits() {
        Dataset<Row> clean = cleaner.clean(frame(
                new Object[]{"1", "A", "a", 10, 1.0, "1"},
                new Object[]{"1", "A", "a", 5, 1.0, "1"},
                new Object[]{"1", "B", "b", 7, 1.0, "1"},
                new Object[]{"1", "C", "c", 3, 1.0, "1"},
                new Object[]{"1", "D", "d", 1, 1.0, "1"},
                new Object[]{"1", "E", "e", 2, 1.0, "1"}
        ));

        List<Row> top = topAnalyzer.analyze(clean, 3).collectAsList();
        assertEquals(3, top.size());
        assertEquals("A", top.get(0).getAs(STOCK_CODE));
        assertEquals(15L, top.get(0).getLong(2));
        assertEquals("B", top.get(1).getAs(STOCK_CODE));
        assertEquals("C", top.get(2).getAs(STOCK_CODE));
    }

    /** Средний чек = среднее по суммам заказов (20 и 100 → 60), а не по строкам (10, 10, 100 → 40). */
    @Test
    void customerStatsUseInvoiceAverageNotRowAverage() {
        Dataset<Row> clean = cleaner.clean(frame(
                new Object[]{"i1", "A", "d", 1, 10.0, "1"},
                new Object[]{"i1", "A", "d", 2, 5.0, "1"},
                new Object[]{"i2", "A", "d", 1, 100.0, "1"},
                new Object[]{"i3", "B", "d", 1, 1.0, "2"}
        ));

        List<Row> rows = customerAnalyzer.analyze(clean).collectAsList();

        Row c1 = rows.stream()
                .filter(r -> r.getAs(CUSTOMER_ID).equals("1"))
                .findFirst().orElseThrow();
        assertEquals(2L, c1.getLong(1));          // OrdersCount
        assertEquals(120.0, c1.getDouble(2), 1e-9); // TotalSpent = 20 + 100
        assertEquals(60.0, c1.getDouble(3), 1e-9);  // AverageCheck = (20 + 100) / 2

        Row c2 = rows.stream()
                .filter(r -> r.getAs(CUSTOMER_ID).equals("2"))
                .findFirst().orElseThrow();
        assertEquals(1L, c2.getLong(1));
        assertEquals(1.0, c2.getDouble(2), 1e-9);
        assertEquals(1.0, c2.getDouble(3), 1e-9);
    }

    @Test
    void writerProducesSinglePartFile(@TempDir Path tmp) throws Exception {
        writer.write(frame(new Object[]{"1", "A", "d", 1, 1.0, "1"}), tmp.resolve("out").toString());

        List<Path> parts;
        try (var files = Files.list(tmp.resolve("out"))) {
            parts = files.filter(f -> f.getFileName().toString().startsWith("part-")).toList();
        }
        assertEquals(1, parts.size());
    }

    @Test
    void pipelineEndToEnd(@TempDir Path tmp) {
        RetailDataLoader loader = (s, path) -> frame(
                new Object[]{"1", "A", "a", 10, 1.0, "1"},
                new Object[]{"1", "A", "a", 5, 1.0, "1"},
                new Object[]{"1", "B", "b", 7, 1.0, "1"},
                new Object[]{"2", "B", "b", 1, 1.0, "2"},
                new Object[]{"C3", "A", "cancelled", 5, 1.0, "1"}
        );

        new RetailAnalysisPipeline(loader, cleaner, topAnalyzer, customerAnalyzer, writer)
                .run(spark, "ignored", tmp.toString(), 2);

        List<Row> top = spark.read().option("header", "true")
                .csv(tmp.resolve("top_products").toString())
                .select(col(STOCK_CODE), col(TOTAL_QUANTITY).cast("string"))
                .collectAsList();
        assertEquals(2, top.size());
        assertEquals("A", top.get(0).getString(0));
        assertEquals("15", top.get(0).getString(1));

        Row c1 = spark.read().option("header", "true")
                .csv(tmp.resolve("customer_stats").toString())
                .select(col(CUSTOMER_ID),
                        col(ORDERS_COUNT).cast("string"),
                        col(TOTAL_SPENT).cast("string"))
                .filter(col(CUSTOMER_ID).equalTo("1"))
                .first();
        assertEquals("1", c1.getString(1));   // C3-отмена отброшена, остался 1 заказ
        assertEquals("22.0", c1.getString(2)); // 15 + 7
    }

    @Test
    void loaderReadsLatin1EscapedQuotesAndEmptyIds(@TempDir Path tmp) throws Exception {
        Path csv = tmp.resolve("retail.csv");
        Files.writeString(csv,
                "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
                        + "1,A,\"DESC WITH \"\"QUOTES\"\" AND £\",2,12/1/2010 8:26,2.55,17850,United Kingdom\n"
                        + "2,B,plain,1,12/1/2010 8:26,1.0,,United Kingdom\n",
                StandardCharsets.ISO_8859_1);

        List<Row> rows = new CsvDataLoader().load(spark, csv.toString()).collectAsList();

        assertEquals(2, rows.size());
        assertEquals("DESC WITH \"QUOTES\" AND £", rows.get(0).getAs(DESCRIPTION));
        assertEquals(Integer.valueOf(2), rows.get(0).getAs(QUANTITY));
        assertNull(rows.get(1).getAs(CUSTOMER_ID));
    }

    @Test
    void parseTopNUsesDefaultWhenMissing() {
        assertEquals(5, OnlineRetailApp.parseTopN(new String[]{"in.csv", "out"}));
    }

    @Test
    void parseTopNReadsUserValue() {
        assertEquals(10, OnlineRetailApp.parseTopN(new String[]{"in.csv", "out", "10"}));
    }

    @Test
    void parseTopNRejectsNonPositive() {
        assertThrows(IllegalArgumentException.class,
                () -> OnlineRetailApp.parseTopN(new String[]{"in.csv", "out", "0"}));
    }

    @Test
    void parseTopNRejectsNonNumeric() {
        assertThrows(NumberFormatException.class,
                () -> OnlineRetailApp.parseTopN(new String[]{"in.csv", "out", "abc"}));
    }
}

package org.example.steps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 9: пропуски StockCode — строку нельзя отнести ни к одному товару.
public class MissingStockStep extends DqStep {
    public MissingStockStep() {
        super(9, "Missing StockCode");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(CASE WHEN StockCode IS NULL OR TRIM(StockCode) = '' THEN 1 END) AS MissingRows
                FROM online_retail
                """).first();

        long missingStockRows = asLong(metrics.get(0));

        System.out.println("=== 9. MISSING STOCKCODE ROWS: " + missingStockRows + " ===");

        StringBuilder section = new StringBuilder()
                .append("---\n## 9. Missing StockCode\n\n")
                .append("A row without a stock code cannot be attributed to any product and is unusable. ")
                .append("The dataset contract expects every row to carry a stock code.\n\n")
                .append("- **Rows with missing `StockCode`:** ").append(missingStockRows)
                .append(" (").append(pct(missingStockRows, total)).append(" of all rows)\n\n");

        if (missingStockRows > 0) {
            Dataset<Row> sample = spark.sql("""
                    SELECT InvoiceNo, Description, Quantity
                    FROM online_retail
                    WHERE StockCode IS NULL OR TRIM(StockCode) = ''
                    LIMIT 10
                    """);
            sample.show(10, false);
            section.append("**Sample rows**\n\n").append(mdTable(sample, 10));
        }

        return new StepResult(number, title, missingStockRows, section.toString());
    }
}

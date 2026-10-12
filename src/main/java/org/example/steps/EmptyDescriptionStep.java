package org.example.steps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 5: пустые описания товаров.
public class EmptyDescriptionStep extends DqStep {
    public EmptyDescriptionStep() {
        super(5, "Empty description");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(CASE WHEN Description IS NULL OR TRIM(Description) = '' THEN 1 END) AS EmptyRows,
                       COUNT(DISTINCT CASE WHEN Description IS NULL OR TRIM(Description) = '' THEN StockCode END) AS DistinctStockCodes
                FROM online_retail
                """).first();

        Dataset<Row> sample = spark.sql("""
                SELECT InvoiceNo, StockCode, Quantity, UnitPrice
                FROM online_retail
                WHERE Description IS NULL OR TRIM(Description) = ''
                LIMIT 10
                """);

        long emptyDescriptionRows = asLong(metrics.get(0));
        long stockCodesWithoutDescription = asLong(metrics.get(1));

        System.out.println("=== 5. EMPTY DESCRIPTION ROWS ===");
        sample.show(10, false);

        StringBuilder section = new StringBuilder()
                .append("---\n## 5. Empty description\n\n")
                .append("A missing product name makes item-level reporting and product matching impossible. ")
                .append("Empty descriptions usually accompany service entries (postage, adjustments, samples).\n\n")
                .append("- **Rows with an empty `Description`:** ").append(emptyDescriptionRows)
                .append(" (").append(pct(emptyDescriptionRows, total)).append(" of all rows)\n")
                .append("- **Distinct `StockCode` values affected:** ").append(stockCodesWithoutDescription).append("\n\n")
                .append("**Sample rows**\n\n")
                .append(mdTable(sample, 10));

        return new StepResult(number, title, emptyDescriptionRows, section.toString());
    }
}

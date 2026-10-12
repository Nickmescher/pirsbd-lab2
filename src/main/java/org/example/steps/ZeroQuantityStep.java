package org.example.steps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 2: нулевые количества; отрицательные — как контекст (отмены против обычных инвойсов).
public class ZeroQuantityStep extends DqStep {
    public ZeroQuantityStep() {
        super(2, "Zero quantity");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(CASE WHEN CAST(Quantity AS INT) = 0 THEN 1 END) AS ZeroRows,
                       COUNT(CASE WHEN CAST(Quantity AS INT) < 0
                                   AND UPPER(TRIM(COALESCE(InvoiceNo, ''))) LIKE 'C%' THEN 1 END) AS NegativeCancellations,
                       COUNT(CASE WHEN CAST(Quantity AS INT) < 0
                                   AND NOT UPPER(TRIM(COALESCE(InvoiceNo, ''))) LIKE 'C%' THEN 1 END) AS NegativeRegular,
                       COUNT(CASE WHEN Quantity IS NOT NULL AND CAST(Quantity AS INT) IS NULL THEN 1 END) AS Unparseable
                FROM online_retail
                """).first();

        Dataset<Row> sample = spark.sql("""
                SELECT InvoiceNo, StockCode, Description, InvoiceDate
                FROM online_retail
                WHERE CAST(Quantity AS INT) = 0
                LIMIT 10
                """);

        long zeroRows = asLong(metrics.get(0));
        long negativeCancellations = asLong(metrics.get(1));
        long negativeRegular = asLong(metrics.get(2));
        long unparseable = asLong(metrics.get(3));

        System.out.println("=== 2. ZERO QUANTITY ROWS ===");
        sample.show(10, false);

        StringBuilder section = new StringBuilder()
                .append("---\n## 2. Zero quantity\n\n")
                .append("A line that moves zero items contributes nothing to sales analysis ")
                .append("and distorts quantity aggregates and revenue.\n\n")
                .append("- **Rows with `Quantity = 0`:** ").append(zeroRows)
                .append(" (").append(pct(zeroRows, total)).append(" of all rows)\n\n")
                .append("| Context case | Rows |\n|---|---|\n")
                .append("| Negative quantity on cancellation invoices (`C`) | ").append(negativeCancellations).append(" |\n")
                .append("| Negative quantity on regular invoices | ").append(negativeRegular).append(" |\n")
                .append("| Missing or unparseable `Quantity` | ").append(unparseable).append(" |\n\n")
                .append("Negative quantities are legitimate on cancellation invoices; ")
                .append("on regular invoices they are flaws (e.g. manual discount entries).\n\n")
                .append("**Sample rows with `Quantity = 0`**\n\n")
                .append(mdTable(sample, 10));

        return new StepResult(number, title, zeroRows, section.toString());
    }
}

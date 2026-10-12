package org.example.steps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 8: пропуски InvoiceNo — строку нельзя отнести ни к одной транзакции.
public class MissingInvoiceStep extends DqStep {
    public MissingInvoiceStep() {
        super(8, "Missing InvoiceNo");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(CASE WHEN InvoiceNo IS NULL OR TRIM(InvoiceNo) = '' THEN 1 END) AS MissingRows
                FROM online_retail
                """).first();

        long missingInvoiceRows = asLong(metrics.get(0));

        System.out.println("=== 8. MISSING INVOICENO ROWS: " + missingInvoiceRows + " ===");

        StringBuilder section = new StringBuilder()
                .append("---\n## 8. Missing InvoiceNo\n\n")
                .append("A row without an invoice number cannot be attributed to any transaction and is unusable. ")
                .append("The dataset contract expects every row to carry an invoice number.\n\n")
                .append("- **Rows with missing `InvoiceNo`:** ").append(missingInvoiceRows)
                .append(" (").append(pct(missingInvoiceRows, total)).append(" of all rows)\n\n");

        if (missingInvoiceRows > 0) {
            Dataset<Row> sample = spark.sql("""
                    SELECT StockCode, Description, Quantity
                    FROM online_retail
                    WHERE InvoiceNo IS NULL OR TRIM(InvoiceNo) = ''
                    LIMIT 10
                    """);
            sample.show(10, false);
            section.append("**Sample rows**\n\n").append(mdTable(sample, 10));
        }

        return new StepResult(number, title, missingInvoiceRows, section.toString());
    }
}

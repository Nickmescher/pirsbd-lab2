package org.example.steps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 1: дубликаты пар (InvoiceNo, StockCode) — завышают продажи и выручку.
public class DuplicatePairsStep extends DqStep {
    public DuplicatePairsStep() {
        super(1, "Duplicate (InvoiceNo, StockCode) pairs");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(*) AS Pairs, SUM(Copies - 1) AS Surplus
                FROM (
                    SELECT InvoiceNo, StockCode, COUNT(*) AS Copies
                    FROM online_retail
                    WHERE TRIM(COALESCE(InvoiceNo, '')) <> '' AND TRIM(COALESCE(StockCode, '')) <> ''
                    GROUP BY InvoiceNo, StockCode
                    HAVING COUNT(*) > 1
                )
                """).first();

        Dataset<Row> topPairs = spark.sql("""
                SELECT InvoiceNo, StockCode, COUNT(*) AS Copies, FIRST(Description, true) AS ExampleDescription
                FROM online_retail
                WHERE TRIM(COALESCE(InvoiceNo, '')) <> '' AND TRIM(COALESCE(StockCode, '')) <> ''
                GROUP BY InvoiceNo, StockCode
                HAVING COUNT(*) > 1
                ORDER BY Copies DESC
                LIMIT 10
                """);

        long pairs = asLong(metrics.get(0));
        long surplus = asLong(metrics.get(1));
        long rowsInvolved = pairs + surplus;

        System.out.println("=== 1. DUPLICATE (INVOICENO, STOCKCODE) PAIRS ===");
        topPairs.show(10, false);

        StringBuilder section = new StringBuilder()
                .append("---\n## 1. Duplicate (InvoiceNo, StockCode) pairs\n\n")
                .append("The pair (InvoiceNo, StockCode) must uniquely identify a transaction line. ")
                .append("Duplicated pairs inflate sales and revenue statistics and may indicate double booking.\n\n")
                .append("- **Duplicated pairs:** ").append(pairs).append("\n")
                .append("- **Rows involved in duplicated pairs:** ").append(rowsInvolved)
                .append(" (").append(pct(rowsInvolved, total)).append(" of all rows)\n")
                .append("- **Redundant (surplus) rows:** ").append(surplus).append("\n\n")
                .append("**Top-10 most repeated pairs**\n\n")
                .append(mdTable(topPairs, 10));

        return new StepResult(number, title, rowsInvolved, section.toString());
    }
}

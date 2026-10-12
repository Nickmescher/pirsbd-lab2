package org.example.steps;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Финальный шаг: отдельным запросом считаем строки, затронутые хотя бы одним дефектом.
public class RowsWithAnyFlawStep extends DqStep {
    public RowsWithAnyFlawStep() {
        super(10, "Rows with at least one flaw");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(*) FROM online_retail
                WHERE CAST(Quantity AS INT) = 0
                   OR (CAST(Quantity AS INT) < 0 AND NOT UPPER(TRIM(COALESCE(InvoiceNo, ''))) LIKE 'C%')
                   OR CAST(UnitPrice AS DOUBLE) <= 0
                   OR (Quantity IS NOT NULL AND CAST(Quantity AS INT) IS NULL)
                   OR (UnitPrice IS NOT NULL AND CAST(UnitPrice AS DOUBLE) IS NULL)
                   OR (CustomerID IS NOT NULL AND TRIM(CustomerID) <> ''
                       AND NOT REGEXP_REPLACE(TRIM(CustomerID), '\\\\.0$', '') RLIKE '^\\\\d{5}$')
                   OR Description IS NULL OR TRIM(Description) = ''
                   OR Country IS NULL OR TRIM(Country) NOT IN (
                        'United Kingdom', 'France', 'Australia', 'Netherlands', 'Germany',
                        'Norway', 'Switzerland', 'Spain', 'Poland', 'Portugal', 'Italy',
                        'Belgium', 'Lithuania', 'Japan', 'Iceland', 'Denmark',
                        'Czech Republic', 'Austria', 'Sweden', 'Finland', 'Israel',
                        'Hong Kong', 'Singapore', 'Malta', 'Lebanon', 'Bahrain',
                        'Brazil', 'Canada', 'Greece', 'Cyprus', 'United Arab Emirates',
                        'Saudi Arabia'
                   )
                   OR CustomerID IS NULL OR TRIM(CustomerID) = ''
                   OR InvoiceNo IS NULL OR TRIM(InvoiceNo) = ''
                   OR StockCode IS NULL OR TRIM(StockCode) = ''
                """).first();

        long rowsWithAnyFlaw = asLong(metrics.get(0));

        System.out.println("=== SUMMARY ===");
        System.out.println("Rows with at least one flaw: " + rowsWithAnyFlaw);

        String markdown = "**Rows with at least one flaw (duplicates not included):** " + rowsWithAnyFlaw
                + " (" + pct(rowsWithAnyFlaw, total) + " of all rows). "
                + "Duplicate pairs are measured by the rows involved in a duplicated pair.\n";

        return new StepResult(number, title, rowsWithAnyFlaw, markdown);
    }
}

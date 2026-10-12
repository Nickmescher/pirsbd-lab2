package org.example.steps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 4: некорректные CustomerID — по контракту это 5-значное число.
public class CustomerIdFormatStep extends DqStep {
    public CustomerIdFormatStep() {
        super(4, "Non-5-digit CustomerID");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(CASE WHEN CustomerID IS NOT NULL AND TRIM(CustomerID) <> ''
                                   AND NOT REGEXP_REPLACE(TRIM(CustomerID), '\\\\.0$', '') RLIKE '^\\\\d{5}$'
                             THEN 1 END) AS BadRows,
                       COUNT(DISTINCT CASE WHEN CustomerID IS NOT NULL AND TRIM(CustomerID) <> ''
                                   AND NOT REGEXP_REPLACE(TRIM(CustomerID), '\\\\.0$', '') RLIKE '^\\\\d{5}$'
                             THEN REGEXP_REPLACE(TRIM(CustomerID), '\\\\.0$', '') END) AS DistinctBad
                FROM online_retail
                """).first();

        Dataset<Row> badCustomerTop = spark.sql("""
                SELECT REGEXP_REPLACE(TRIM(CustomerID), '\\\\.0$', '') AS CustomerID, COUNT(*) AS RowCount
                FROM online_retail
                WHERE CustomerID IS NOT NULL AND TRIM(CustomerID) <> ''
                  AND NOT REGEXP_REPLACE(TRIM(CustomerID), '\\\\.0$', '') RLIKE '^\\\\d{5}$'
                GROUP BY REGEXP_REPLACE(TRIM(CustomerID), '\\\\.0$', '')
                ORDER BY RowCount DESC
                LIMIT 10
                """);

        long badCustomerRows = asLong(metrics.get(0));
        long badCustomerValueCount = asLong(metrics.get(1));

        System.out.println("=== 4. NON-5-DIGIT CUSTOMERID ROWS ===");
        badCustomerTop.show(10, false);

        StringBuilder section = new StringBuilder()
                .append("---\n## 4. Non-5-digit CustomerID\n\n")
                .append("The dataset contract defines CustomerID as a 5-digit integral number. ")
                .append("Malformed identifiers break joins with a customer master table.\n\n")
                .append("- **Rows with a present but non-conforming `CustomerID`:** ").append(badCustomerRows)
                .append(" (").append(pct(badCustomerRows, total)).append(" of all rows)\n")
                .append("- **Distinct offending values:** ").append(badCustomerValueCount).append("\n\n")
                .append("Values are trimmed and a trailing `.0` (floating-point export artifact) is removed ")
                .append("before validation against `^\\d{5}$`.\n\n")
                .append("**Top offending `CustomerID` values**\n\n")
                .append(mdTable(badCustomerTop, 10));

        return new StepResult(number, title, badCustomerRows, section.toString());
    }
}

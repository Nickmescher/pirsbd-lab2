package org.example.steps;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 7: пропуски CustomerID — самый массовый дефект датасета.
public class MissingCustomerStep extends DqStep {
    public MissingCustomerStep() {
        super(7, "Missing CustomerID");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(CASE WHEN CustomerID IS NULL OR TRIM(CustomerID) = '' THEN 1 END) AS MissingRows,
                       COUNT(DISTINCT CASE WHEN CustomerID IS NULL OR TRIM(CustomerID) = '' THEN InvoiceNo END) AS DistinctInvoices
                FROM online_retail
                """).first();

        long missingCustomerRows = asLong(metrics.get(0));
        long invoicesWithoutCustomer = asLong(metrics.get(1));

        System.out.println("=== 7. MISSING CUSTOMERID ROWS: " + missingCustomerRows + " ===");

        StringBuilder section = new StringBuilder()
                .append("---\n## 7. Missing CustomerID\n\n")
                .append("Rows without a customer identifier cannot participate in customer analytics ")
                .append("(RFM segmentation, retention, average check). In this dataset this is the most ")
                .append("massive single flaw: roughly a quarter of all rows, mostly non-identified (guest) sales.\n\n")
                .append("- **Rows with missing `CustomerID`:** ").append(missingCustomerRows)
                .append(" (").append(pct(missingCustomerRows, total)).append(" of all rows)\n")
                .append("- **Distinct invoices affected:** ").append(invoicesWithoutCustomer).append("\n\n");

        return new StepResult(number, title, missingCustomerRows, section.toString());
    }
}

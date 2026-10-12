package org.example.steps;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 3: отрицательные или нулевые цены — ломают расчёт выручки.
public class PriceStep extends DqStep {
    public PriceStep() {
        super(3, "Negative or zero unit price");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Row metrics = spark.sql("""
                SELECT COUNT(CASE WHEN CAST(UnitPrice AS DOUBLE) < 0 THEN 1 END) AS NegativeRows,
                       COUNT(CASE WHEN CAST(UnitPrice AS DOUBLE) = 0 THEN 1 END) AS ZeroRows,
                       COUNT(CASE WHEN UnitPrice IS NOT NULL AND CAST(UnitPrice AS DOUBLE) IS NULL THEN 1 END) AS Unparseable
                FROM online_retail
                """).first();

        long negativePriceRows = asLong(metrics.get(0));
        long zeroPriceRows = asLong(metrics.get(1));
        long unparseablePrice = asLong(metrics.get(2));

        System.out.println("=== 3. NEGATIVE OR ZERO UNIT PRICE ROWS ===");
        System.out.println("Negative: " + negativePriceRows + ", zero: " + zeroPriceRows);

        StringBuilder section = new StringBuilder()
                .append("---\n## 3. Negative or zero unit price\n\n")
                .append("`UnitPrice <= 0` breaks revenue arithmetic (`Quantity * UnitPrice`). ")
                .append("Negative prices here are debt-adjustment postings, zero prices are samples and service entries.\n\n")
                .append("- **Rows with `UnitPrice <= 0`:** ").append(negativePriceRows + zeroPriceRows)
                .append(" (").append(pct(negativePriceRows + zeroPriceRows, total)).append(" of all rows)\n\n")
                .append("| Context case | Rows |\n|---|---|\n")
                .append("| `UnitPrice < 0` | ").append(negativePriceRows).append(" |\n")
                .append("| `UnitPrice = 0` | ").append(zeroPriceRows).append(" |\n")
                .append("| Missing or unparseable `UnitPrice` | ").append(unparseablePrice).append(" |\n\n");

        return new StepResult(number, title, negativePriceRows + zeroPriceRows, section.toString());
    }
}

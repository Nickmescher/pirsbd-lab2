package org.example.steps;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Шаг 6: некатегориальные страны (сокращения, заглушки, опечатки) — сверка со справочником.
public class CountryStep extends DqStep {
    public CountryStep() {
        super(6, "Non-categorical countries");
    }

    @Override
    public StepResult execute(SparkSession spark, long total) {
        Dataset<Row> countries = spark.sql("""
                SELECT COALESCE(TRIM(Country), '(null)') AS Country, COUNT(*) AS RowCount,
                       CASE WHEN COALESCE(TRIM(Country), '(null)') IN (
                            'United Kingdom', 'France', 'Australia', 'Netherlands', 'Germany',
                            'Norway', 'Switzerland', 'Spain', 'Poland', 'Portugal', 'Italy',
                            'Belgium', 'Lithuania', 'Japan', 'Iceland', 'Denmark',
                            'Czech Republic', 'Austria', 'Sweden', 'Finland', 'Israel',
                            'Hong Kong', 'Singapore', 'Malta', 'Lebanon', 'Bahrain',
                            'Brazil', 'Canada', 'Greece', 'Cyprus', 'United Arab Emirates',
                            'Saudi Arabia'
                       ) THEN 'ok' ELSE 'NON-CATEGORICAL' END AS Verdict
                FROM online_retail
                GROUP BY COALESCE(TRIM(Country), '(null)')
                ORDER BY RowCount DESC
                """).cache();

        // Забираем весь результат шага на драйвер и считаем метрики по нему.
        List<Row> rows = countries.collectAsList();
        long countryValueCount = rows.size();
        long nonCategoricalCountryValues = 0;
        long nonCategoricalCountryRows = 0;
        for (Row row : rows) {
            if ("NON-CATEGORICAL".equals(row.getString(2))) {
                nonCategoricalCountryValues++;
                nonCategoricalCountryRows += asLong(row.get(1));
            }
        }

        System.out.println("=== 6. COUNTRY VALUES ===");
        countries.show((int) countryValueCount, false);

        StringBuilder section = new StringBuilder()
                .append("---\n## 6. Non-categorical countries (typos, placeholders, abbreviations)\n\n")
                .append("`Country` is expected to be a controlled category of proper country names. ")
                .append("Placeholders (`Unspecified`), abbreviations (`EIRE`, `RSA`, `USA`), regions ")
                .append("(`European Community`, `Channel Islands`) and any typos are flagged ")
                .append("against an embedded whitelist of proper country names.\n\n")
                .append("- **Rows with non-categorical country values:** ").append(nonCategoricalCountryRows)
                .append(" (").append(pct(nonCategoricalCountryRows, total)).append(" of all rows)\n")
                .append("- **Distinct values in data:** ").append(countryValueCount)
                .append(", of which flagged: ").append(nonCategoricalCountryValues).append("\n\n")
                .append("**All country values**\n\n")
                .append(mdTable(countries, 100));

        return new StepResult(number, title, nonCategoricalCountryRows, section.toString());
    }
}

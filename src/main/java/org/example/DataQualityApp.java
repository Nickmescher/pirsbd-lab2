package org.example;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.example.steps.CountryStep;
import org.example.steps.CustomerIdFormatStep;
import org.example.steps.DqStep;
import org.example.steps.DuplicatePairsStep;
import org.example.steps.EmptyDescriptionStep;
import org.example.steps.MissingCustomerStep;
import org.example.steps.MissingInvoiceStep;
import org.example.steps.MissingStockStep;
import org.example.steps.PriceStep;
import org.example.steps.RowsWithAnyFlawStep;
import org.example.steps.StepResult;
import org.example.steps.ZeroQuantityStep;

public class DataQualityApp {

    // Строит отчёт о качестве данных и пишет его в markdown-файл.
    // Вызывается из OnlineRetailApp.main (режим analytics), та же SparkSession переиспользуется.
    // Работа устроена как конвейер шагов: каждый шаг выполняет свой SQL-запрос,
    // забирает результат на драйвер и возвращает готовую секцию отчёта.
    static void run(SparkSession spark, String inputPath, String outputPath) throws IOException {
        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .option("multiLine", "true")
                .option("escape", "\"")
                .csv(inputPath);

        // Читаем все колонки как строки, чтобы схема не "поправила" дефектные значения за нас,
        // и регистрируем DataFrame как временную вьюху — все шаги делают SQL-запросы к ней.
        raw.createOrReplaceTempView("online_retail");
        raw.cache();

        long total = raw.count();

        StringBuilder report = new StringBuilder();
        report.append("# Data Quality Report — Online Retail (UCI dataset 352)\n\n")
                .append("- **Input CSV:** `").append(inputPath).append("`\n")
                .append("- **Generated:** ").append(LocalDateTime.now()
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n")
                .append("- **Total rows:** ").append(total).append("\n")
                .append("- **Columns:** ").append(String.join(", ", raw.columns())).append("\n\n")
                .append("Checks are ordered from the most significant to the least significant. ")
                .append("All columns are read as raw strings and every check is a Spark SQL query ")
                .append("over the `online_retail` view. A row can be counted by several checks at once.\n\n");

        // Список шагов проверок — в порядке от самых значимых к менее значимым.
        List<DqStep> steps = List.of(
                new DuplicatePairsStep(),
                new ZeroQuantityStep(),
                new PriceStep(),
                new CustomerIdFormatStep(),
                new EmptyDescriptionStep(),
                new CountryStep(),
                new MissingCustomerStep(),
                new MissingInvoiceStep(),
                new MissingStockStep());

        // Каждый шаг выполняет отдельный запрос и возвращает секцию отчёта + число затронутых строк.
        List<StepResult> results = new ArrayList<>();
        for (DqStep step : steps) {
            results.add(step.execute(spark, total));
        }

        // Сводка собирается из результатов, которые вернули шаги.
        report.append("---\n## Summary\n\n")
                .append("| # | Check | Rows affected | % of total |\n|---|---|---|---|\n");
        for (StepResult result : results) {
            report.append("| ").append(result.number()).append(" | ").append(result.title())
                    .append(" | ").append(result.rowsAffected())
                    .append(" | ").append(DqStep.pct(result.rowsAffected(), total)).append(" |\n");
        }
        report.append("\n");

        // Финальный шаг: отдельным запросом считаем строки, затронутые хотя бы одним дефектом.
        report.append(new RowsWithAnyFlawStep().execute(spark, total).markdown());

        Path out = Path.of(outputPath);
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        Files.writeString(out, report.toString(), StandardCharsets.UTF_8);

        System.out.println("=== DONE ===");
        System.out.println("Report written to " + out.toAbsolutePath());
    }
}

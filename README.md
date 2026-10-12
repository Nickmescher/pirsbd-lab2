# pirsbd-lab2 — Online Retail на Apache Spark

Spark Java-приложение для анализа датасета [Online Retail (UCI)](https://archive.ics.uci.edu/dataset/352/online+retail).

## Что считает приложение

1. **Топ-5 самых популярных товаров** по общему количеству проданных единиц (`sum(Quantity)` по `StockCode` + `Description`).
2. **Статистика по каждому клиенту**:
   - общее число заказов (`countDistinct(InvoiceNo)`);
   - общая потраченная сумма (`sum(Quantity * UnitPrice)`);
   - средний чек — средняя сумма одного `InvoiceNo`.

Отмены (`InvoiceNo`, начинающиеся с `C`) и строки без `CustomerID` отфильтрованы.

Результаты пишутся как CSV в `output/top5_products/` и `output/customer_stats/`.

## Подготовка

1. Скачать CSV с датасетом (`Online Retail.csv`) и положить в `data/online_retail.csv`.
2. Поднять Spark-кластер:
   ```bash
   docker compose up -d
   ```
3. Собрать fat-jar:
   ```bash
   mvn clean package
   ```

## Запуск

```bash
docker cp target/pirsbd-lab2.jar spark-master:/opt/spark/pirsbd-lab2.jar

docker exec spark-master /opt/spark/bin/spark-submit \
  --class org.example.OnlineRetailApp \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/pirsbd-lab2.jar \
  /opt/spark/data/online_retail.csv \
  /opt/spark/output
```

CSV-выгрузки появятся в `./output/top5_products/` и `./output/customer_stats/` на хосте (каталоги проброшены в контейнер через volumes).

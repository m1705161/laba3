# Лаба 3. Потоковая обработка: CDC и streaming-загрузка в Iceberg

Лабораторная работа по потоковой обработке данных. Строим lakehouse на связке
**MinIO (Silo) + Lakekeeper + Trino**, а поверх — поток изменений из PostgreSQL
(через Debezium + Kafka) либо поток погоды из публичного API, и пишем результат
в таблицы **Apache Iceberg**. В конце — **Time Travel** по снапшотам.

## Стек

```
PostgreSQL → Debezium → Kafka → Spark Structured Streaming / Flink → Iceberg → Trino
Open-Meteo → Python-продюсер → Kafka → Spark Structured Streaming → Iceberg → Trino
```

## Гайды

| Файл | Тема | Движок |
|---|---|---|
| `гайд-4-scd2-cdc.md` / `-win.md` | SCD Type 2 на CDC-потоке | Spark Structured Streaming |
| `гайд-4б-flink-cdc.md` / `-win.md` | То же на Flink, exactly-once | Apache Flink |
| `гайд-5-streaming-ingest.md` / `-win.md` | Streaming-загрузка из Open-Meteo (append-only) | Spark Structured Streaming |

Каждый гайд есть в версиях для **macOS** (`*.md`) и **Windows** (`*-win.md`, PowerShell).
Windows-версии самодостаточны (включают поднятие базового стека), macOS-версии
ссылаются на стек из гайдов 1–3.

## Сопутствующие файлы лабы

| Файл | Что это |
|---|---|
| `producer.py` | продюсер погоды Open-Meteo → Kafka (гайд 5) |
| `spark/ingest.py` | Spark-джоба: Kafka → Iceberg append (гайд 5) |
| `spark/scd2.py` | Spark-джоба SCD Type 2 (гайд 4) |
| `spark/conf/spark-defaults.conf` | конфиг Spark (каталог Iceberg + jar'ы) |
| `connect/customers-connector.json` | коннектор Debezium (гайд 4/4б) |
| `create-warehouse-scd.json` | регистрация warehouse `scd` в Lakekeeper |
| `scd.properties` | каталог `scd` для Trino |

## Требования

- Docker Desktop (macOS или Windows, режим Linux-контейнеров).
- Доступ в интернет (скачивание образов; гайду 5 нужен ещё и доступ к `api.open-meteo.com`).
- ~10 ГБ свободного места под образы.

## Быстрый старт

Порядок — по номерам: сначала гайд 4 (или облегчённый гайд 5), затем гайд 4б
для сравнения Spark и Flink. Подробности и план занятия — в начале каждого гайда.

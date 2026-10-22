# Гайд 4б. Flink вместо Spark: Debezium → Kafka → Flink → Iceberg (+ exactly-once, Time Travel) — Windows

Стек: **PostgreSQL → Debezium → Kafka → Flink → Iceberg (Lakekeeper/Silo) → Trino**.

Это продолжение гайдов 4–5. Тот же CDC-источник, тот же Kafka, тот же lakehouse (Silo + Lakekeeper + Trino) — но **стриминг-движком теперь Apache Flink, а не Spark Structured Streaming**. Цель — увидеть разницу: Flink обрабатывает поток **событие за событием** (не микробатчами), понимает CDC из коробки и пишет в Iceberg **ровно один раз** (exactly-once через checkpoint).

> ⚠️ **Важно перед стартом (обновление 2026): образы MinIO.** Официальные образы больше не раздаются бесплатно: `minio/minio` удалён с Docker Hub, `quay.io/minio/minio` и `quay.io/minio/mc` отвечают `401`, а AIStor требует лицензию. Используй свободный форк **Silo** — команды, порты и переменные окружения те же, меняется только имя образа:
> - сервер: `pgsty/silo:latest` (вместо `minio/minio` / `quay.io/minio/minio`);
> - клиент `mc`: `pgsty/mc:latest` (вместо `minio/mc` / `quay.io/minio/mc`).

> 🪟 **Windows (Docker Desktop, режим Linux-контейнеров).** Версия для macOS — в соседнем файле `гайд-4б-flink-cdc.md`.

> 📌 **Как читать Windows-версию.** Все команды — для **PowerShell** (Windows PowerShell 5.1 или PowerShell 7), Docker Desktop в режиме **Linux-контейнеров**. Отличия от macOS-версии:
> - `curl` в PowerShell — это алиас на `Invoke-WebRequest`, а **не** настоящий curl. Поэтому настоящий curl пишем явно: **`curl.exe`**.
> - Файлы создаём через here-string `@'...'@` + `[System.IO.File]::WriteAllText` — это пишет **UTF-8 без BOM** (иначе JSON / `.conf` / `.properties` / `.sql` могут не прочитаться).
> - Продолжение длинной команды — обратный апостроф `` ` `` (вместо `\`).
> - `$(pwd)` → `"$PWD/..."`, `sleep` → `Start-Sleep -Seconds`, `grep` → `Select-String`, `rm -rf` → `Remove-Item -Recurse -Force`, `mkdir -p` → `New-Item -ItemType Directory -Force`.
> - `host.docker.internal` на Docker Desktop для Windows работает из коробки — все адреса те же, что и в macOS-версии.

---

## 0. Что мы строим и зачем

| | Гайд 4 (Spark) | **Гайд 4б (этот, Flink)** |
|---|---|---|
| Источник | PostgreSQL + Debezium (CDC) | **тот же** |
| Стриминг-движок | Spark Structured Streaming (микробатчи) | **Apache Flink** (непрерывный поток) |
| Модель времени | микробатчи по таймеру | событие за событием, event time из коробки |
| Гарантия | at-least-once (foreachBatch) | **exactly-once** (checkpoint → атомарный коммит Iceberg) |
| Результат | SCD2 `scd.dim.customers` | append-лог `scd.raw.customers_log` |
| Time Travel | есть | **есть** |

Схема:

```
┌────────────┐   WAL    ┌──────────┐  события  ┌───────┐  топик    ┌──────────────┐  append  ┌─────────┐  SQL  ┌───────┐
│ PostgreSQL │ ────────►│ Debezium │ ────────► │ Kafka │ ────────► │    Flink     │ ───────► │ Iceberg │ ────► │ Trino │
│ (источник) │ pgoutput │ (Connect)│           │       │           │ (SQL, exactly)│         │(Lakekeep)│       │       │
└────────────┘          └──────────┘           └───────┘           └──────────────┘         └─────────┘       └───────┘
```

**Идея лабораторной.** Меняем таблицу `customers` в PostgreSQL, Debezium ловит изменения из WAL и кладёт их в Kafka. **Flink** читает этот CDC-топик, «разворачивает» Debezium-конверт (`before`/`after`/`op`/`ts_ms`) в плоскую строку и **дописывает** её в Iceberg-таблицу `scd.raw.customers_log`. Каждая строка — одно событие изменений. В конце — **Time Travel** по снапшотам Iceberg, как в гайдах 4–5.

### Что получится в конце

Таблица `scd.raw.customers_log` с колонками:

| id | name | city | op | ts |
|---|---|---|---|---|
| 1 | Alice | Moscow | r | 2026-09-21 10:45:00.123 |
| 2 | Bob | Saint Petersburg | r | ... |
| ... | ... | ... | c/u/d | ... |

Каждая строка — одно событие CDC. События накапливаются: `op:"r"` (снапшот), `op:"c"` (insert), `op:"u"` (update), `op:"d"` (delete).

---

## 0.5. План на 2.5 часа

| Время | Блок | Что делаем | Чек-пойнт |
|---|---|---|---|
| 0:00–0:10 | Разминка | Flink vs Spark: непрерывный поток vs микробатч | — |
| 0:10–0:30 | Хранилище | warehouse `scd`, каталог, схема `raw` (или пропусти, если есть) | `SHOW CATALOGS` видит `scd` |
| 0:30–1:00 | CDC | PostgreSQL-источник + Kafka + Debezium | в топике видны события |
| 1:00–1:10 | Проверка топика | console-consumer, разбираем `before`/`after`/`op` | поняли структуру |
| 1:10–1:40 | Flink | jar'ы, `job.sql`, запуск | в Iceberg появились строки |
| 1:40–2:00 | Наблюдение | изменения → события в `customers_log` | счётчик растёт |
| 2:00–2:20 | Exactly-once | checkpoint → атомарный коммит, смотрим снапшоты | снапшотов меньше, чем строк |
| 2:20–2:35 | Time Travel | снапшоты, `FOR VERSION/TIMESTAMP AS OF` | «данные тогда» меньше |
| 2:35–2:45 | Итоги + уборка | вопросы §11, очистка §13 | лабораторная сдана |

> ⏱️ Самые долгие этапы — **скачивание образов** (`debezium/connect` ~1.1 ГБ, `apache/flink` ~0.6 ГБ) и **первый запуск Flink** (~60–90 с: JVM + старт кластера + копирование jar'ов). Скачай образы заранее (§1.3), а Flink запускай сразу после регистрации коннектора.

---

## 1. Пререквизиты

**1.1. Docker Desktop** — запущен (в режиме **Linux-контейнеров**, это значение по умолчанию). Проверь:

```powershell
docker --version
docker run --rm hello-world    # должно напечатать "Hello from Docker!"
```

**1.2. Свободные порты.** Гайд займёт порты `9000`, `9001` (MinIO/Silo), `5432` (Postgres Lakekeeper), `8181` (Lakekeeper), `8080` (Trino), `5433` (источник PG), `9092` (Kafka), `8083` (Debezium), `8081` (Flink). Убедись, что все свободны:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 9000,9001,5432,8181,8080,5433,9092,8083,8081 -ErrorAction SilentlyContinue
```

Пустой вывод = свободны. (Если какие-то заняты — освободи их; менять порты не рекомендуется, потому что адреса зашиты в конфиги ниже.)

**1.3. Скачай образы заранее** (чтобы не ждать во время лабораторной):

```powershell
# базовый стек (§1.5)
docker pull pgsty/silo:latest
docker pull pgsty/mc:latest
docker pull postgres:17
docker pull quay.io/lakekeeper/catalog:latest-main
docker pull trinodb/trino:476
# компоненты лабораторной
docker pull apache/kafka:3.8.0
docker pull debezium/connect:3.0.0.Final
docker pull postgres:16-alpine
docker pull apache/flink:1.20.0
```

**1.4. Рабочая папка.** Все файлы лабораторной кладём в одну папку — `C:\flink-cdc-lab` (путь **без пробелов и кириллицы**, иначе у Docker могут быть проблемы с монтированием). Создай её со всеми подпапками и перейди в неё:

```powershell
New-Item -ItemType Directory -Force -Path C:\flink-cdc-lab\connect, C:\flink-cdc-lab\flink\lib, C:\flink-cdc-lab\flink\checkpoints, C:\flink-cdc-lab\catalog | Out-Null
cd C:\flink-cdc-lab
```

Дальше все команды с относительными путями (`.\flink\...`, `.\connect\...`, `.\create-warehouse-scd.json`) выполняй из этой папки. Файлы создаются через here-string — просто копируй блок целиком, редактор не нужен.

**1.5. Базовый стек: MinIO + Lakekeeper + Trino.** Гайду нужен работающий lakehouse — поднимаем его здесь же, без отдельных гайдов.

**MinIO** (объектное хранилище; используем свободный форк **Silo** — официальные образы `minio/minio` больше не раздаются бесплатно):

```powershell
docker run -d --name lakehouse-minio `
  -p 9000:9000 -p 9001:9001 `
  -e MINIO_ROOT_USER=minioadmin `
  -e MINIO_ROOT_PASSWORD=minioadmin `
  -v minio-data:/data `
  pgsty/silo:latest server /data --console-address ":9001"
```

Проверка: открой http://localhost:9001 → логин `minioadmin` / пароль `minioadmin`.

**Lakekeeper** (Iceberg REST Catalog). Сначала Postgres под его метаданные, затем сам каталог:

```powershell
docker run -d --name lakehouse-pg `
  -e POSTGRES_PASSWORD=postgres `
  -p 5432:5432 `
  -v pg-data:/var/lib/postgresql/data `
  postgres:17
```

Подожди ~5 с (`docker exec lakehouse-pg pg_isready` → `accepting connections`), затем миграции и запуск:

```powershell
docker run --rm `
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' `
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@host.docker.internal:5432/postgres' `
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@host.docker.internal:5432/postgres' `
  quay.io/lakekeeper/catalog:latest-main migrate

docker run -d --name lakekeeper `
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' `
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@host.docker.internal:5432/postgres' `
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@host.docker.internal:5432/postgres' `
  -p 8181:8181 `
  quay.io/lakekeeper/catalog:latest-main serve
```

Прими Terms of Use (один раз, без этого регистрация warehouse упадёт) и проверь здоровье:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8181/management/v1/bootstrap -ContentType 'application/json' -Body '{"accept-terms-of-use": true}'
curl.exe -s http://localhost:8181/health      # 200
```

> 💡 **Почему здесь `Invoke-RestMethod`, а не `curl.exe`?** Тело запроса — JSON с кавычками, а Windows PowerShell не умеет корректно передавать такие аргументы в нативные команды (`curl.exe`). Для JSON-тел удобнее нативный `Invoke-RestMethod` — он сам сериализует тело и выставляет `Content-Type`. В остальных местах (GET-запросы, `-d @файл`) спокойно работаем с `curl.exe`.

> Если bootstrap вернул `Catalog is not open for bootstrap` / `CatalogAlreadyBootstrapped` — каталог уже инициализирован, это норм, пропусти.

**Trino** (SQL-движок). Монтируем папку `catalog/` целиком — потом положим в неё `scd.properties` и перезапустим Trino:

```powershell
docker run -d --name trino `
  -p 8080:8080 `
  -v "$PWD/catalog:/etc/trino/catalog" `
  trinodb/trino:476
```

Проверка: `curl.exe -s http://localhost:8080/v1/info` вернёт JSON (Trino готовится ~20–30 с).

---

## Шпаргалка: порты и пароли

| Сервис | Адрес | Логин | Пароль |
|---|---|---|---|
| **PostgreSQL-источник** | localhost:5433 | `postgres` | `postgres` |
| **Kafka** — брокер | host.docker.internal:9092 | — | без авторизации |
| **Debezium Connect** — REST | http://localhost:8083 | — | без авторизации |
| **Flink** — веб-интерфейс | http://localhost:8081 | — | без авторизации |
| MinIO — консоль / S3 | localhost:9001 / :9000 | `minioadmin` | `minioadmin` |
| Lakekeeper — веб-интерфейс / REST | http://localhost:8181/ui/ | — | без авторизации |
| Trino — веб-интерфейс | http://localhost:8080/ui/ | — | без авторизации |

> БД источника — `shop`, таблица — `public.customers`.

---

## 2. Этап 1 — хранилище `scd` (бакет, warehouse, каталог, схема)

> Если делал Гайд 4/5 и `scd` уже создан — переходи сразу к §2.4.

**2.1. Бакет `scd` в MinIO.** Консоль http://localhost:9001 → логин `minioadmin`/`minioadmin` → слева **Buckets → Create Bucket → имя `scd`**. Либо через `mc`:

```powershell
docker run --rm --entrypoint /bin/sh --add-host host.docker.internal:host-gateway `
  pgsty/mc:latest -c "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin && mc mb --ignore-existing m/scd"
```

**2.2. Warehouse `scd` в Lakekeeper.** Создай файл `create-warehouse-scd.json` (имя warehouse = `scd`, бакет = `scd`) — скопируй блок целиком:

```powershell
[System.IO.File]::WriteAllText("$PWD\create-warehouse-scd.json", @'
{
  "warehouse-name": "scd",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3", "bucket": "scd", "key-prefix": "",
    "endpoint": "http://host.docker.internal:9000", "region": "us-east-1",
    "path-style-access": true, "flavor": "s3-compat", "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3", "credential-type": "access-key",
    "access-key-id": "minioadmin", "secret-access-key": "minioadmin"
  }
}
'@)
```

Зарегистрируй warehouse:

```powershell
curl.exe -s -X POST http://localhost:8181/management/v1/warehouse `
  -H "Content-Type: application/json" `
  -d "@create-warehouse-scd.json"
```

> Ответ — `201 Created` со `"status":"active"`. Если `overlaps with existing warehouse scd` — уже есть, пропусти.

**2.3. Каталог `scd` в Trino.** Папка `catalog/` из рабочей папки уже смонтирована в `/etc/trino/catalog` (§1.5). Значит, достаточно положить `scd.properties` прямо в неё — файл сам окажется в нужном месте внутри контейнера:

```powershell
[System.IO.File]::WriteAllText("$PWD\catalog\scd.properties", @'
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://host.docker.internal:8181/catalog
iceberg.rest-catalog.warehouse=scd
iceberg.rest-catalog.nested-namespace-enabled=true
iceberg.unique-table-location=true
fs.native-s3.enabled=true
s3.endpoint=http://host.docker.internal:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
'@)
```

Перезапусти Trino и дождись готовности (~20–30 с):

```powershell
docker restart trino
# подожди, затем проверь:
docker exec trino trino --execute "SHOW CATALOGS"   # должен появиться scd
```

**2.4. Схема `raw`** (в ней будет лежать CDC-лог):

```powershell
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS scd.raw"
```

Проверка: `docker exec trino trino --execute "SHOW SCHEMAS FROM scd"` → увидишь `raw`.

---

## 3. Этап 2 — PostgreSQL-источник

Поднимаем PostgreSQL **на порту 5433** (5432 занят Lakekeeper) с включённой **логической репликацией** (`wal_level=logical`) — без неё Debezium не прочитает WAL:

```powershell
docker run -d --name pg-src -p 5433:5432 `
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=shop `
  postgres:16-alpine `
  -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4
```

Подожди, пока поднимется (`docker exec pg-src pg_isready -U postgres` → `accepting connections`), и создай таблицу с тремя строками:

```powershell
docker exec pg-src psql -U postgres -d shop -c "CREATE TABLE public.customers (id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL, city TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()); INSERT INTO public.customers (name, city) VALUES ('Alice','Moscow'), ('Bob','Saint Petersburg'), ('Carol','Kazan');"
```

Проверка:

```powershell
docker exec pg-src psql -U postgres -d shop -c "SELECT * FROM public.customers ORDER BY id;"
docker exec pg-src psql -U postgres -d shop -c "SHOW wal_level;"   # logical
```

> 💡 **Что такое `-c wal_level=logical ...`?** Это аргументы запуска PostgreSQL. `wal_level=logical` включает логическую репликацию (Debezium читает изменения через неё), `max_replication_slots`/`max_wal_senders` — слоты и соединения для репликации. Без этих флагов Debezium упадёт с ошибкой про `wal_level`.

---

## 4. Этап 3 — Kafka + Debezium Connect

**Kafka (KRaft):**

```powershell
docker run -d --name kafka -p 9092:9092 `
  -e KAFKA_NODE_ID=1 `
  -e KAFKA_PROCESS_ROLES=broker,controller `
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 `
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://host.docker.internal:9092 `
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT `
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 `
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 `
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 `
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 `
  apache/kafka:3.8.0
```

Подожди ~15 с и проверь, что брокер стартовал:

```powershell
docker logs kafka 2>&1 | Select-String "Kafka Server started"
```

**Debezium Connect:**

```powershell
docker run -d --name connect `
  -p 8083:8083 `
  --add-host host.docker.internal:host-gateway `
  -e BOOTSTRAP_SERVERS=host.docker.internal:9092 `
  -e CONFIG_STORAGE_TOPIC=connect_configs `
  -e OFFSET_STORAGE_TOPIC=connect_offsets `
  -e STATUS_STORAGE_TOPIC=connect_statuses `
  -e CONFIG_STORAGE_REPLICATION_FACTOR=1 `
  -e OFFSET_STORAGE_REPLICATION_FACTOR=1 `
  -e STATUS_STORAGE_REPLICATION_FACTOR=1 `
  -e GROUP_ID=1 `
  debezium/connect:3.0.0.Final
```

Подожди ~30–60 с (JVM) и проверь REST-эндпоинт:

```powershell
curl.exe -s http://localhost:8083/                            # вернёт {"version":"3.8.0",...}
curl.exe -s http://localhost:8083/connector-plugins | Select-String postgres   # плагин на месте
```

**Коннектор.** Создай файл `connect\customers-connector.json` (скопируй блок целиком):

```powershell
[System.IO.File]::WriteAllText("$PWD\connect\customers-connector.json", @'
{
  "name": "customers-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "host.docker.internal",
    "database.port": "5433",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "shop",
    "topic.prefix": "cdc",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "filtered",
    "table.include.list": "public.customers",
    "slot.name": "debezium_customers",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "snapshot.mode": "initial"
  }
}
'@)
```

Зарегистрируй:

```powershell
curl.exe -s -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@connect\customers-connector.json"
```

Проверь статус:

```powershell
curl.exe -s http://localhost:8083/connectors/customers-connector/status   # state: RUNNING
```

---

## 5. Этап 4 — смотрим CDC-топик

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic cdc.public.customers `
  --from-beginning --max-messages 3 --timeout-ms 15000
```

Увидишь Debezium-конверт (снапшот `op:"r"`):

```json
{"before":null,"after":{"id":1,"name":"Alice","city":"Moscow","updated_at":"..."},"op":"r","ts_ms":1789986848629,...}
```

- `before` — строка до изменения (`null` у снапшота/insert);
- `after` — строка после (`null` у delete);
- `op` — тип: `r` (снапшот), `c` (insert), `u` (update), `d` (delete);
- `ts_ms` — время события в миллисекундах.

Именно эти поля Flink будет «разворачивать» в плоскую строку.

---

## 6. Этап 5 — Flink: jar'ы и джоба

**6.1. Jar'ы.** В отличие от Spark (гайд 4/5, где нужно 6 jar'ов), для Flink хватает **четырёх**:

```powershell
New-Item -ItemType Directory -Force -Path flink\lib, flink\checkpoints | Out-Null
cd flink\lib
curl.exe -fsSL -o iceberg-flink-runtime-1.20-1.7.1.jar `
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.1/iceberg-flink-runtime-1.20-1.7.1.jar
curl.exe -fsSL -o iceberg-aws-bundle-1.7.1.jar `
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
curl.exe -fsSL -o flink-sql-connector-kafka-3.3.0-1.20.jar `
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
curl.exe -fsSL -o flink-shaded-hadoop-2-uber-2.8.3-10.0.jar `
  https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
cd ..\..
```

| Jar | Зачем |
|---|---|
| `iceberg-flink-runtime` | Flink-коннектор Iceberg (читает/пишет таблицы, REST-каталог) |
| `iceberg-aws-bundle` | S3FileIO + AWS SDK — чтобы писать в Silo/MinIO |
| `flink-sql-connector-kafka` | чтение Kafka-топика |
| `flink-shaded-hadoop-2-uber` | Hadoop-классы (`org.apache.hadoop.conf.Configuration`) — без них Iceberg не создаст каталог |

> 💡 `flink-sql-connector-kafka` — это **один fat-jar**, в отличие от Spark, где Kafka-коннектор тянул за собой `kafka-clients` + `commons-pool2` + ещё два jar'а. Версия `3.3.0-1.20` должна совпадать с версией Flink (1.20).
>
> 💡 **Зачем Hadoop?** `iceberg-flink-runtime` не тащит Hadoop (он рассчитывает, что Hadoop уже есть на кластере). В образе Spark Hadoop был, а в `apache/flink:1.20.0` — нет. Поэтому добавляем `flink-shaded-hadoop-2-uber` (один fat-jar со всеми Hadoop-классами, версия `2.8.3-10.0` под Flink 1.20).

**6.2. Джоба `flink/job.sql`** — читает CDC-топик, разворачивает Debezium, пишет в Iceberg.

> ⚠️ **Почему SQL, а не PyFlink (`job.py`)?** В официальном образе `apache/flink:1.20.0` **нет Python** (это Ubuntu 22.04 + Java 11), поэтому `flink run -py job.py` падает с `Cannot run program "python"`. Ставить Python в контейнер — это ~500 МБ зависимостей PyFlink. Проще и чище использовать **Flink SQL Client** — он на чистой Java и выполняет ровно тот же SQL. Поэтому джоба — это `.sql`-файл, а не `.py`.

```powershell
[System.IO.File]::WriteAllText("$PWD\flink\job.sql", @'
-- Debezium CDC -> Iceberg на Flink SQL Client (append-лог изменений, exactly-once)
-- Примечание: `raw` — зарезервированное слово Flink (тип RAW), поэтому в кавычках.

SET 'execution.runtime-mode' = 'streaming';
-- exactly-once: checkpoint каждые 10 с -> Iceberg коммитит снапшот атомарно
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 'file:///opt/flink/checkpoints';

-- 1) Kafka-источник (Debezium-конверт как обычный JSON)
CREATE TABLE cdc_source (
  `before` ROW<id BIGINT, name STRING, city STRING>,
  `after`  ROW<id BIGINT, name STRING, city STRING>,
  op   STRING,
  ts_ms BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.public.customers',
  'properties.bootstrap.servers' = 'host.docker.internal:9092',
  'properties.group.id' = 'flink-cdc',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- 2) Каталог Iceberg -> Lakekeeper (REST) + S3 (Silo)
CREATE CATALOG scd WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://host.docker.internal:8181/catalog',
  'warehouse' = 'scd',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://host.docker.internal:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin',
  's3.region' = 'us-east-1'
);

-- 3) Целевая таблица (append-only CDC-лог)
-- CREATE DATABASE не принимает имя вида "catalog.db" -> переключаем каталог на время
USE CATALOG scd;
CREATE DATABASE IF NOT EXISTS `raw`;
USE CATALOG default_catalog;

CREATE TABLE IF NOT EXISTS scd.`raw`.customers_log (
  id   BIGINT,
  name STRING,
  city STRING,
  op   STRING,
  ts   TIMESTAMP(3)
);

-- 4) Разворачиваем Debezium-конверт -> плоская строка, дописываем в Iceberg
INSERT INTO scd.`raw`.customers_log
SELECT
  COALESCE(`after`.id, `before`.id)     AS id,
  COALESCE(`after`.name, `before`.name) AS name,
  COALESCE(`after`.city, `before`.city) AS city,
  op,
  TO_TIMESTAMP_LTZ(ts_ms, 3)            AS ts
FROM cdc_source;
'@)
```

**Как читать джобу:**

- `SET 'execution.checkpointing.interval' = '10s'` — Flink каждые 10 с делает checkpoint, и **Iceberg коммитит снапшот атомарно** в момент checkpoint (exactly-once). Вместо Python-кода настройки задаются `SET`-операторами.
- `cdc_source` — Kafka-топик как таблица. `before`/`after` — это `ROW`-типы, `ts_ms` — время события.
- `CREATE CATALOG scd` — подключаем Iceberg к **Lakekeeper** (REST) и к **Silo** (S3 через `io-impl=S3FileIO`). Это ровно тот же warehouse `scd`, который видит Trino.
- `USE CATALOG scd` + `CREATE DATABASE` — схема `raw` создаётся так, потому что `CREATE DATABASE scd.raw` Flink-парсер не понимает (имя вида `каталог.бд` в `CREATE DATABASE` не поддерживается).
- `CREATE TABLE IF NOT EXISTS` — `IF NOT EXISTS` делает джобу идемпотентной: при повторном запуске таблица переиспользуется, а не падает с «уже существует».
- `INSERT ... SELECT` — для каждого Debezium-события берём `after` (а для delete, где `after=null`, — `before`) и дописываем строку. `TO_TIMESTAMP_LTZ(ts_ms, 3)` превращает миллисекунды в timestamp.

> ⚠️ **`raw` — зарезервированное слово.** В Flink SQL `RAW` — это тип (`RAW<class>`, бинарная строка), поэтому имя базы `raw` нужно брать в обратные кавычки: `` `raw` ``. В Trino это не мешает — там `scd.raw.customers_log` пишется без кавычек.

> ⚠️ **DELETE несёт только `id`.** По умолчанию `REPLICA IDENTITY` таблицы — `DEFAULT` (только первичный ключ в `before`). Поэтому у события `op:"d"` колонки `name`/`city` будут `NULL`. Это то же, что в практической №1 — там показано, как `REPLICA IDENTITY FULL` кладёт в `before` всю строку (и чем за это платим).

---

## 7. Этап 6 — запуск Flink

```powershell
docker run -d --name flink `
  --entrypoint bash `
  --add-host host.docker.internal:host-gateway `
  -p 8081:8081 `
  -v "$PWD/flink/lib:/opt/flink/lib-extra" `
  -v "$PWD/flink/job.sql:/opt/flink/job.sql" `
  -v "$PWD/flink/checkpoints:/opt/flink/checkpoints" `
  apache/flink:1.20.0 `
  -c "cp /opt/flink/lib-extra/*.jar /opt/flink/lib/ && /opt/flink/bin/start-cluster.sh && sleep 10 && /opt/flink/bin/sql-client.sh -f /opt/flink/job.sql && sleep infinity"
```

Что тут происходит (важно!):

- **`--entrypoint bash`** — обходим штатный `docker-entrypoint.sh` образа (он сбрасывает пользователя в не-root `flink`), чтобы можно было копировать jar'ы в `/opt/flink/lib/` и писать checkpoint'и в примонтированную папку.
- **`start-cluster.sh`** — поднимает JobManager + TaskManager **внутри контейнера**. SQL Client не умеет работать без кластера: `sql-client.sh embedded` во Flink 1.20 не поднимает встроенный кластер, а пытается слать джобу в несуществующий `localhost:8081`.
- **`sql-client.sh -f job.sql`** — выполняет наш SQL, последний `INSERT` отправляется в кластер как стрим-джоба.
- **`sleep infinity`** — держит контейнер (и кластер внутри него) живым, пока джоба стримит. Без этого контейнер завершится сразу после отправки джобы и убьёт JobManager/TaskManager до первого checkpoint.

Проверь, что джоба поднялась и строки появились:

```powershell
docker logs flink 2>&1 | Select-String "successfully submitted"   # Job ID: ...
docker exec trino trino --execute "SELECT * FROM scd.raw.customers_log ORDER BY ts"
```

Должны быть 3 строки снапшота (`op='r'`: Alice, Bob, Carol). Первый коммит — на первый checkpoint (через ~10 с после старта джобы).

> 💡 **Почему jar'ы монтируем в `/opt/flink/lib-extra`, а не `/opt/flink/lib`?** В `/opt/flink/lib` лежат библиотеки самого Flink — если смонтировать поверх, Flink не запустится. Поэтому кладём в отдельную папку, а при старте копируем в `lib/` (`cp ...` в `bash -c`).

> 💡 **Веб-интерфейсы (открой в соседних вкладках):**
> - **Flink** — **http://localhost:8081**: вкладка **Jobs** → запущенная джоба и её граф операторов; вкладка **Checkpoints** → новые checkpoint'ы каждые 10 с.
> - **Trino** — **http://localhost:8080**: выполняющиеся/завершённые запросы (твой `SELECT` через `trino --execute` появится в истории) и список воркеров.

> 💡 **Логи JobManager/TaskManager** теперь пишутся в файлы `/opt/flink/log/`, а не в `docker logs` (там — только вывод `start-cluster.sh` и `sql-client.sh`). Смотреть их: `docker exec flink sh -c 'tail -f /opt/flink/log/*taskexecutor*.log'`.

---

## 8. Этап 7 — изменения и наблюдение

Пока Flink крутится, меняй источник (каждое изменение — с паузой, чтобы видеть отдельные события):

```powershell
# 1) Bob переехал
docker exec pg-src psql -U postgres -d shop -c "UPDATE public.customers SET city='Novosibirsk' WHERE name='Bob';"
Start-Sleep -Seconds 5
# 2) новый клиент Dave
docker exec pg-src psql -U postgres -d shop -c "INSERT INTO public.customers (name, city) VALUES ('Dave','Yekaterinburg');"
Start-Sleep -Seconds 5
# 3) Carol удалена
docker exec pg-src psql -U postgres -d shop -c "DELETE FROM public.customers WHERE name='Carol';"
```

Смотри CDC-лог в Iceberg:

```powershell
docker exec trino trino --execute "SELECT id, name, city, op, ts FROM scd.raw.customers_log ORDER BY ts"
```

Увидишь:

```text
 id |  name   |  city   | op |           ts
----+---------+---------+----+------------------------
  1 | Alice   | Moscow  | r  | ... (снапшот)
  2 | Bob     | Saint Petersburg | r  | ...
  3 | Carol   | Kazan   | r  | ...
  2 | Bob     | Novosibirsk | u | ... (update)
  4 | Dave    | Yekaterinburg | c | ... (insert)
  3 | NULL    | NULL    | d  | ... (delete — только id)
```

Это и есть «поток правды»: **каждое изменение** в источнике — строка в Iceberg, почти мгновенно, а не ночным batch. Обрати внимание на delete: `name`/`city` — `NULL`, потому что `REPLICA IDENTITY DEFAULT` кладёт в `before` только `id`.

---

## 9. Этап 8 — exactly-once: checkpoint → атомарный коммит

Flink пишет в Iceberg **не построчно, а на каждый checkpoint** — атомарным коммитом нового снапшота. Между checkpoint'ами Flink складывает data-файлы в S3, но в таблицу их **не видно**: они «всплывают» одним коммитом в момент checkpoint. Это и есть exactly-once на выходе — таблица всегда в согласованном состоянии (либо снапшот N, либо N+1), «полузаписанных» строк не бывает.

**1) Проверь, что checkpoint'ы идут** (каждые 10 с). Логи JobManager/TaskManager — в файлах `/opt/flink/log/`, а не в `docker logs`:

```powershell
docker exec flink sh -c 'grep -h "Committed append\|Committed snapshot" /opt/flink/log/*taskexecutor*.log | tail -n 5'
```

Увидишь `Committed snapshot ... (MergeAppend)` — это и есть атомарный коммит Iceberg на checkpoint.

**2) Снапшоты Iceberg** — каждый checkpoint = один атомарный commit:

```powershell
docker exec trino trino --execute 'SELECT committed_at, operation, snapshot_id FROM scd.raw."customers_log$snapshots" ORDER BY committed_at'
```

> ⚠️ Кавычки вокруг `customers_log$snapshots` обязательны (`$` в имени).

Снапшотов **намного меньше, чем строк**: в таблице уже десятки событий, а снапшотов — по одному на каждые 10 с. Это и есть границы, на которых Iceberg фиксирует накопленные изменения. Упади Flink между checkpoint'ами — незакоммиченные файлы останутся осиротевшими, а таблица останется на последнем снапшоте: без потерь и без дублей.

> 💡 **Те же коммиты глазами в MinIO-консоли** — открой **http://localhost:9001** (`minioadmin`/`minioadmin`), войди в бакет `scd` и раскрой папку таблицы (`raw/customers_log*`): внутри `data/` (parquet-файлы со строками) и `metadata/` (файлы снапшотов). На каждом checkpoint в `metadata/` появляется новый файл — это и есть атомарный коммит, который ты только что видел в SQL.

**3) А как же «перезапуск без дублей»?** Полное «продолжить ровно с того же места» после остановки — это **savepoint** (ручная операция из лекции 14), а не обычный `docker restart`. Обычный `docker restart flink` стартует джобу заново и перечитает топик с начала (у нас `scan.startup.mode='earliest-offset'`) — поэтому в §13 и советуем чистить папку `flink/checkpoints` перед повторным прогоном. В этой лабораторной достаточно увидеть сам механизм: **checkpoint = атомарный коммит Iceberg**.

> 💡 `checkpoint` (автоматический, для восстановления после сбоя) против `savepoint` (ручной, для апгрейда/миграции) — разница из лекции 14.

---

## 10. Этап 9 — Time Travel

Возьми `snapshot_id` самого первого снапшота (сразу после загрузки, там 3 строки):

```sql
SELECT committed_at, snapshot_id, operation
FROM scd.raw."customers_log$snapshots"
ORDER BY committed_at;
```

```sql
-- сколько строк было на первом снапшоте
SELECT count(*) AS rows
FROM scd.raw.customers_log
FOR VERSION AS OF 2801610465087767715;   -- подставь СВОЙ snapshot_id

-- сколько строк было 2 минуты назад
SELECT count(*) AS rows
FROM scd.raw.customers_log
FOR TIMESTAMP AS OF TIMESTAMP '2026-09-21 12:26:30 UTC';   -- подставь СВОЁ время
```

«Тогда» строк меньше, чем сейчас — таблица росла, а снапшоты Iceberg зафиксировали её состояние в каждый момент.

> 💡 **Как выполнить SQL-блоки выше?** Так же, как в §9, — через `docker exec trino trino --execute "..."`, либо через веб-интерфейс Trino http://localhost:8080 (кнопка запуска запроса). Для запросов с `$` в имени системной таблицы используй одинарные кавычки в PowerShell, как в §9.

### Вывод

- **Flink vs Spark.** Flink читает CDC и пишет в Iceberg **событие за событием**, без микробатчей; результат появляется почти мгновенно.
- **Exactly-once.** Checkpoint каждые 10 с → Iceberg коммитит снапшот атомарно; между checkpoint'ами данные в таблицу не попадают.
- **Time Travel** — работает как в гайдах 4–5, потому что это свойство Iceberg, а не движка.

---

## 11. Итоги и вопросы

1. **Flink vs Spark.** Чем Flink отличается от Spark Structured Streaming из гайда 4? Что значит «событие за событием» против «микробатчей»?
2. **CDC.** Что лежит в `before`/`after` для каждого `op`? Почему `COALESCE(after.id, before.id)` нужно для delete?
3. **Каталог.** Что делает `CREATE CATALOG scd` с `catalog-type='rest'`? Почему Flink видит ту же таблицу `scd.raw.customers_log`, что и Trino?
4. **Exactly-once.** Почему Iceberg коммитит снапшот на каждый checkpoint, а не на каждую строку? Что случится, если checkpoint отключить?
5. **REPLICA IDENTITY.** Почему у `op:"d"` в логе `name`/`city` — `NULL`? Как это починить и чем заплатить?

---

## 12. Если что-то пошло не так

- **Flink: `Cannot run program "python"`** — пытаешься запустить PyFlink (`flink run -py`). В образе `apache/flink:1.20.0` нет Python. Используй SQL Client (`job.sql`), как в этом гайде — Python не нужен.
- **Flink: `ClassNotFoundException: org.apache.hadoop.conf.Configuration`** — не скачан/не скопирован `flink-shaded-hadoop-2-uber` (§6.1). Без Hadoop-классов Iceberg не создаст каталог.
- **Flink: `Connection refused: /0.0.0.0:8081`** — SQL Client не нашёл кластер. Проверь, что в команде запуска `start-cluster.sh` идёт **до** `sql-client.sh` (§7). Режим `sql-client.sh embedded` во Flink 1.20 не поднимает кластер сам.
- **Flink: `ClassNotFoundException: ...Iceberg`** — jar'ы не скопировались в `/opt/flink/lib/`. Проверь, что `flink/lib/*.jar` скачаны и `cp` в `bash -c` отработал.
- **Flink: `No FileSystem for scheme "s3"`** — в `CREATE CATALOG` нет `io-impl=S3FileIO` и строк `s3.*` (§6.2), или не скачан `iceberg-aws-bundle`.
- **Flink: `ParseException: Encountered "raw"`** — забыл обратные кавычки: имя базы должно быть `` `raw` `` (зарезервированное слово Flink).
- **Flink: `AlreadyExistsException ... already exists`** — таблица уже создана прошлым прогоном. `CREATE TABLE IF NOT EXISTS` (§6.2) решает; для совсем чистого прогона удали таблицу/warehouse (§13, шаг 2).
- **Flink: `Catalog scd does not exist` / ошибка REST** — Lakekeeper не отвечает (`curl.exe -s http://localhost:8181/health`), или warehouse `scd` не создан (§2.2).
- **Flink: `Could not find table scd.raw.customers_log`** — не выполнен `CREATE DATABASE` / `CREATE TABLE` (§6.2). Проверь порядок SQL-операторов в `job.sql`.
- **Flink: не читает топик** — коннектор Debezium не RUNNING (`curl.exe -s http://localhost:8083/connectors/customers-connector/status`), или топик пустой (§4).
- **В логе нет `customers_log`** — первый коммит происходит на первый checkpoint (до ~10 с после старта). Подожди дольше.
- **Debezium: `logical decoding requires wal_level >= logical`** — источник PG поднят без `-c wal_level=logical` (§3).
- **Spark-коннектор не при чём** — это Flink, не путай с гайдом 4/5: jar'ов четыре, а не шесть.
- **Порт `8081` занят** — освободи или смени `-p 8081:8081` (тогда веб-интерфейс переедет).

---

## 13. Остановить и удалить всё

```powershell
# 1. Контейнеры лабораторной
docker rm -f flink connect kafka pg-src

# 2. Удаляем warehouse `scd` из Lakekeeper (таблица + namespace + сам warehouse)
$SCD_ID = (curl.exe -s http://localhost:8181/management/v1/warehouse | ConvertFrom-Json).warehouses | Where-Object { $_.name -eq 'scd' } | Select-Object -ExpandProperty 'warehouse-id'
curl.exe -s -X DELETE "http://localhost:8181/catalog/v1/$SCD_ID/namespaces/raw/tables/customers_log"
curl.exe -s -X DELETE "http://localhost:8181/catalog/v1/$SCD_ID/namespaces/raw"
Start-Sleep -Seconds 5   # Lakekeeper дочищает файлы таблицы в фоне
curl.exe -s -X DELETE "http://localhost:8181/management/v1/warehouse/$SCD_ID"

# 3. Убираем каталог scd из Trino
Remove-Item -Force "$PWD\catalog\scd.properties"
docker restart trino

# 4. Удаляем бакет scd из MinIO (через отдельный контейнер mc, как в §2.1 — в образе сервера Silo клиента mc нет)
docker run --rm --entrypoint /bin/sh --add-host host.docker.internal:host-gateway `
  pgsty/mc:latest -c "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin >/dev/null 2>&1; mc rb --force m/scd"

# 5. Базовый стек (§1.5) — если больше не нужен
docker rm -f trino lakekeeper lakehouse-pg lakehouse-minio
docker volume rm minio-data pg-data

# 6. Образы (скачаются заново при следующем запуске)
docker rmi apache/flink:1.20.0 `
          apache/kafka:3.8.0 `
          debezium/connect:3.0.0.Final `
          postgres:16-alpine `
          quay.io/lakekeeper/catalog:latest-main `
          trinodb/trino:476 `
          postgres:17 `
          pgsty/silo:latest `
          pgsty/mc:latest
```

Рабочую папку удали целиком: `Remove-Item -Recurse -Force C:\flink-cdc-lab`. Чтобы **просто приостановить** — `docker stop flink connect kafka pg-src` без `rm`.

> ⚠️ **Шаг 2 можно пропустить** (warehouse/catalog/бакет останутся висеть — на повторный прогон это не влияет). Для **чистого** повторения шаг 2 обязателен. Если удаление вернуло `Warehouse has unfinished tasks` — подожди ещё несколько секунд и повтори.

> 💡 **Чистый повторный прогон.** Хост-папка `flink\checkpoints` не удаляется вместе с контейнером. Для прохода с нуля очисти её: `Remove-Item -Recurse -Force flink\checkpoints; New-Item -ItemType Directory -Force flink\checkpoints` (и, при желании, пересоздай источник `pg-src` — его replication slot хранит позицию в WAL).

---

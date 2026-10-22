# Гайд 4. SCD Type 2 на CDC-потоке: PostgreSQL → Kafka → Spark → Iceberg (+ Time Travel)

Стек: **PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Iceberg (Lakekeeper) → Trino**.

Это **самодостаточный** гайд: базовый Lakehouse (MinIO / Lakekeeper / Trino) поднимаем здесь же, в §1.5, поверх него строим **поток изменений** из PostgreSQL и превращаем его в **медленно меняющееся измерение (SCD Type 2)**. В конце — **Time Travel**-запросы, которые достают данные «какими они были в момент X».

> ⚠️ **Важно перед стартом (обновление 2026): образы MinIO.** Официальные образы больше не раздаются бесплатно: `minio/minio` удалён с Docker Hub, `quay.io/minio/minio` и `quay.io/minio/mc` отвечают `401`, а AIStor требует лицензию. Используй свободный форк **Silo** — команды, порты и переменные окружения те же, меняется только имя образа:
> - сервер: `pgsty/silo:latest` (вместо `minio/minio` / `quay.io/minio/minio`);
> - клиент `mc`: `pgsty/mc:latest` (вместо `minio/mc` / `quay.io/minio/mc`).

> 🍎 **Только macOS (Docker Desktop).** Windows-версия будет отдельно.

---

## 0. Что мы строим и зачем

Три понятия, которые связывает эта лабораторная:

- **CDC (Change Data Capture)** — захват изменений в источнике. Вместо того чтобы раз в час перечитывать всю таблицу, мы читаем **журнал** (WAL) PostgreSQL и получаем только строки, которые реально изменились. Для этого Debezium подключается к WAL через logical replication и пишет каждое изменение в Kafka.
- **SCD Type 2** — способ хранить **историю** изменения строки. Когда у клиента меняется город, мы не перетираем старую строку, а добавляем новую версию с колонками `valid_from` / `valid_to` / `is_current`. Старая версия «закрывается», новая становится актуальной.
- **Time Travel** — способ Iceberg читать таблицу **на момент прошлого снапшота** (`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`). Это «физическая» история файлов, в отличие от «логической» истории SCD2.

Схема:

```
┌────────────┐   WAL    ┌──────────┐  события  ┌───────┐  топик    ┌─────────────────────┐  MERGE  ┌─────────┐  SQL  ┌───────┐
│ PostgreSQL │ ────────►│ Debezium │ ────────► │ Kafka │ ────────► │ Spark Structured    │ ──────► │ Iceberg │ ────► │ Trino │
│ (источник) │ pgoutput │ (Connect)│           │       │           │ Streaming (SCD2)    │         │(Lakekeep)│       │       │
└────────────┘          └──────────┘           └───────┘           └─────────────────────┘         └─────────┘       └───────┘
```

| Компонент | Что делает | Где поднимается |
|---|---|---|
| **PostgreSQL** (порт 5433) | источник: таблица `customers`, которую мы меняем | §3 |
| **Debezium Connect** (8083) | читает WAL, пишет изменения в Kafka | §5 |
| **Kafka** (9092) | брокер сообщений, топик `cdc.public.customers` | §4 |
| **Spark Structured Streaming** | читает топик, применяет SCD2, пишет в Iceberg | §7–8 (образ из §1.3) |
| **Iceberg (Lakekeeper)** | целевая таблица `scd.dim.customers` с историей | базовый стек §1.5 |
| **Trino** | смотрим историю + Time Travel | базовый стек §1.5 |

### Что получится в конце

Таблица `scd.dim.customers` после серии изменений будет выглядеть так (каждая строка — **версия**):

| id | name | city | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| 1 | Alice | Moscow | t0 | t3 | false |
| 1 | Alicia | Moscow | t3 | — | true |
| 2 | Bob | Saint Petersburg | t0 | t2 | false |
| 2 | Bob | Novosibirsk | t2 | — | true |
| 3 | Carol | Kazan | t0 | t4 | false |
| 4 | Dave | Yekaterinburg | t1 | — | true |

Клиента `1` переименовали (Alice → Alicia), `2` переехал, `4` добавили, `3` удалили. История не потеряна — и при этом есть «снимок на сейчас» (`is_current = true`) и «снимок на любой прошлый момент» (Time Travel).

---

## 0.5. План на 3 часа

| Время | Блок | Что делаем | Чек-пойнт |
|---|---|---|---|
| 0:00–0:15 | Разминка + базовый стек | поднимаем MinIO / Lakekeeper / Trino (§1.5); понимаем CDC / SCD2 / Time Travel | базовый стек поднят |
| 0:15–0:40 | Хранилище + источник | бакет/warehouse `scd`; PostgreSQL-источник с таблицей и данными | `SELECT` в источнике работает |
| 0:40–1:10 | CDC | Kafka (KRaft) + Debezium Connect + коннектор | в топике видны события |
| 1:10–1:50 | Spark-поток | jar'ы, `spark-defaults.conf`, джоба `scd2.py`, запуск | в Iceberg появились строки |
| 1:50–2:20 | Наблюдение | скрипт изменений, смотрим SCD2-историю | 6 версий в таблице |
| 2:20–2:45 | Time Travel | снапшоты, `FOR VERSION/TIMESTAMP AS OF` | «данные тогда» совпали |
| 2:45–3:00 | Итоги + уборка | вопросы §11, очистка §13 | лабораторная сдана |

> ⏱️ Самые долгие «механические» этапы — это **скачивание образов** (`debezium/connect` ~1.1 ГБ, `apache/kafka` ~0.6 ГБ) и **первый запуск Spark** (~40–50 с: JVM + jar'ы). Чтобы не жечь время лабораторной, скачай образы заранее (§1.3) и запускай Spark сразу после регистрации коннектора — он будет прогреваться, пока ты смотришь топик.

---

## 1. Пререквизиты

**1.1. Docker Desktop** — запущен. Проверь:

```bash
docker --version
docker run --rm hello-world    # должно напечатать "Hello from Docker!"
```

**1.2. Свободные порты.** Гайд займёт порты `9000`, `9001` (MinIO/Silo), `5432` (Postgres Lakekeeper), `8181` (Lakekeeper), `8080` (Trino), `5433` (источник PG), `9092` (Kafka), `8083` (Debezium). Убедись, что все свободны:

```bash
lsof -nP -iTCP:9000 -iTCP:9001 -iTCP:5432 -iTCP:8181 -iTCP:8080 -iTCP:5433 -iTCP:9092 -iTCP:8083 -sTCP:LISTEN
```

Пустой вывод = свободны. (Если какие-то заняты — освободи их; менять порты не рекомендуется, потому что адреса зашиты в конфиги ниже.)

**1.3. Скачай образы заранее** (чтобы не ждать во время лабораторной):

```bash
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
docker pull apache/spark:3.5.3
```

**1.4. Рабочая папка.** Все файлы лабораторной кладём в одну папку. Создай её со всеми подпапками (в т.ч. `catalog/` — туда Trino будет читать файлы каталогов) и перейди в неё:

```bash
mkdir -p ~/scd2-lab/connect ~/scd2-lab/spark/jars ~/scd2-lab/spark/conf ~/scd2-lab/spark/checkpoint ~/scd2-lab/catalog
cd ~/scd2-lab
```

Дальше все команды с относительными путями (`./spark/...`, `./connect/...`, `./create-warehouse-scd.json`) выполняй из этой папки. Файлы создаются через `cat > файл << 'EOF'` — просто копируй блок целиком, редактор не нужен.

**1.5. Базовый стек: MinIO + Lakekeeper + Trino.** Гайду нужен работающий lakehouse — поднимаем его здесь же, без отдельных гайдов.

**MinIO** (объектное хранилище; используем свободный форк **Silo** — официальные образы `minio/minio` больше не раздаются бесплатно):

```bash
docker run -d --name lakehouse-minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data \
  pgsty/silo:latest server /data --console-address ":9001"
```

Проверка: открой http://localhost:9001 → логин `minioadmin` / пароль `minioadmin`.

**Lakekeeper** (Iceberg REST Catalog). Сначала Postgres под его метаданные, затем сам каталог:

```bash
docker run -d --name lakehouse-pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -v pg-data:/var/lib/postgresql/data \
  postgres:17
```

Подожди ~5 с (`docker exec lakehouse-pg pg_isready` → `accepting connections`), затем миграции и запуск:

```bash
docker run --rm \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@host.docker.internal:5432/postgres' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@host.docker.internal:5432/postgres' \
  quay.io/lakekeeper/catalog:latest-main migrate

docker run -d --name lakekeeper \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@host.docker.internal:5432/postgres' \
  -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@host.docker.internal:5432/postgres' \
  -p 8181:8181 \
  quay.io/lakekeeper/catalog:latest-main serve
```

Прими Terms of Use (один раз, без этого регистрация warehouse упадёт) и проверь здоровье:

```bash
curl -X POST http://localhost:8181/management/v1/bootstrap \
  -H "Content-Type: application/json" \
  -d '{"accept-terms-of-use": true}'
curl http://localhost:8181/health      # 200
```

> Если bootstrap вернул `Catalog is not open for bootstrap` / `CatalogAlreadyBootstrapped` — каталог уже инициализирован, это норм, пропусти.

**Trino** (SQL-движок). Монтируем папку `catalog/` целиком — потом положим в неё `scd.properties` и перезапустим Trino:

```bash
docker run -d --name trino \
  -p 8080:8080 \
  -v "$(pwd)/catalog:/etc/trino/catalog" \
  trinodb/trino:476
```

Проверка: `curl http://localhost:8080/v1/info` вернёт JSON (Trino готовится ~20–30 с).

---

## Шпаргалка: порты и пароли

| Сервис | Адрес | Логин | Пароль |
|---|---|---|---|
| **PostgreSQL-источник** | localhost:5433 | `postgres` | `postgres` |
| **Kafka** — брокер | host.docker.internal:9092 | — | без авторизации |
| **Debezium Connect** — REST | http://localhost:8083 | — | без авторизации |
| MinIO — консоль / S3 | localhost:9001 / :9000 | `minioadmin` | `minioadmin` |
| Lakekeeper — REST | localhost:8181 | — | без авторизации |
| Trino | localhost:8080 | `trino` | пусто |

> Все пароли — демо, только для локальной лабораторной. БД источника называется `shop`, таблица — `public.customers`.

---

## 2. Этап 1 — хранилище `scd` (бакет, warehouse, каталог)

Данные лабораторной держим отдельно от базового стека: свой бакет, свой warehouse, свой каталог.

**2.1. Бакет `scd` в MinIO.** Консоль http://localhost:9001 → логин `minioadmin`/`minioadmin` → слева **Buckets → Create Bucket → имя `scd`**. Либо через `mc`:

```bash
docker run --rm --entrypoint /bin/sh --add-host host.docker.internal:host-gateway \
  pgsty/mc:latest -c "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin && mc mb --ignore-existing m/scd"
```

**2.2. Warehouse `scd` в Lakekeeper.** Создай файл `create-warehouse-scd.json` (имя warehouse = `scd`, бакет = `scd`) — скопируй блок целиком:

```bash
cat > create-warehouse-scd.json << 'EOF'
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
EOF
```

Зарегистрируй warehouse:

```bash
curl -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -d @create-warehouse-scd.json
```

> Ожидаемый ответ — `201 Created` со `"status":"active"`. Если `Storage profile overlaps with existing warehouse scd` — warehouse уже есть, пропусти.

**2.3. Каталог `scd` в Trino.** Кладём `scd.properties` рядом с остальными файлами каталогов (`lakekeeper.properties`, `bronze.properties`). Куда именно — подскажет Trino (нас интересует `Source` для `Destination /etc/trino/catalog`):

```bash
docker inspect trino -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}'
```

Теперь создай файл прямо в каталоге Trino (команда сама находит папку монтирования — скопируй блок целиком):

```bash
TRINO_CATALOG="$(docker inspect trino -f '{{range .Mounts}}{{if eq .Destination "/etc/trino/catalog"}}{{.Source}}{{end}}{{end}}')"
cat > "$TRINO_CATALOG/scd.properties" << 'EOF'
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
EOF
```

Перезапусти Trino и дождись готовности (~20–30 с):

```bash
docker restart trino
# подожди, затем проверь:
docker exec trino trino --execute "SHOW CATALOGS"   # должен появиться scd
```

**2.4. Схема `dim`** (в ней будет жить наше измерение):

```bash
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS scd.dim"
```

Проверка: `docker exec trino trino --execute "SHOW SCHEMAS FROM scd"` → увидишь `dim`.

---

## 3. Этап 2 — PostgreSQL-источник

Поднимаем отдельный PostgreSQL **на порту 5433** (5432 занят Lakekeeper) с включённой **логической репликацией** (`wal_level=logical`) — без неё Debezium не прочитает WAL:

```bash
docker run -d --name pg-src -p 5433:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=shop \
  postgres:16-alpine \
  -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4
```

Подожди, пока поднимется (`docker exec pg-src pg_isready -U postgres` → `accepting connections`), и создай таблицу с тремя строками:

```bash
docker exec pg-src psql -U postgres -d shop -c "
CREATE TABLE public.customers (
  id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO public.customers (name, city) VALUES
  ('Alice','Moscow'), ('Bob','Saint Petersburg'), ('Carol','Kazan');
"
```

Проверка:

```bash
docker exec pg-src psql -U postgres -d shop -c "SELECT * FROM public.customers ORDER BY id;"
docker exec pg-src psql -U postgres -d shop -c "SHOW wal_level;"   # logical
```

> 💡 **Что такое `-c wal_level=logical ...`?** Это аргументы запуска PostgreSQL. `wal_level=logical` включает логическую репликацию (Debezium читает изменения через неё), `max_replication_slots`/`max_wal_senders` — слоты и соединения для репликации. Без этих флагов Debezium упадёт с ошибкой про `wal_level`.

> ⚠️ **Пароль.** В команде выше пароль `postgres` (см. шпаргалку). Его же укажем в конфиге Debezium на этапе 4.

---

## 4. Этап 3 — Kafka (один узел, режим KRaft)

Современный Kafka (3.3+) умеет работать **без ZooKeeper** — режим **KRaft**. Для лабораторной хватит одного контейнера, который одновременно broker и controller:

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://host.docker.internal:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  apache/kafka:3.8.0
```

Подожди ~15 с и проверь, что брокер стартовал:

```bash
docker logs kafka 2>&1 | grep -i "Kafka Server started"
```

> 💡 **Почему `ADVERTISED_LISTENERS=host.docker.internal:9092`?** Kafka сообщает клиентам адрес, по которому к нему подключаться. Наши клиенты (Debezium, Spark) живут **в других контейнерах**, поэтому адрес `localhost` им бы указывал «внутрь себя». `host.docker.internal` — это хост изнутри любого контейнера, т.е. правильный адрес Kafka для всех.

> ⚠️ **Флаги `*_REPLICATION_FACTOR=1`.** По умолчанию Kafka хочет репликацию 3 (несколько узлов). У нас узел один, поэтому опускаем фактор репликации до 1 — иначе внутренние топики не создадутся.

> 💡 **Веб-интерфейс Kafka (необязательно).** В образе `apache/kafka` веб-UI нет. Хочешь смотреть топики и сообщения глазами — подними **kafka-ui** отдельным контейнером:

```bash
docker run -d --name kafka-ui -p 8090:8080 \
  --add-host host.docker.internal:host-gateway \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 \
  provectuslabs/kafka-ui:latest
```

→ открой **http://localhost:8090**: слева топики, внутри — сами сообщения. Топиков пока нет — появятся, когда заработает продюсер/коннектор (следующие этапы).

---

## 5. Этап 4 — Debezium Connect

Debezium Connect — это Kafka Connect с готовыми коннекторами (в т.ч. PostgreSQL). Поднимаем его и указываем, где Kafka:

```bash
docker run -d --name connect \
  -p 8083:8083 \
  --add-host host.docker.internal:host-gateway \
  -e BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e CONFIG_STORAGE_TOPIC=connect_configs \
  -e OFFSET_STORAGE_TOPIC=connect_offsets \
  -e STATUS_STORAGE_TOPIC=connect_statuses \
  -e CONFIG_STORAGE_REPLICATION_FACTOR=1 \
  -e OFFSET_STORAGE_REPLICATION_FACTOR=1 \
  -e STATUS_STORAGE_REPLICATION_FACTOR=1 \
  -e GROUP_ID=1 \
  debezium/connect:3.0.0.Final
```

Подожди ~30–60 с (JVM) и проверь REST-эндпоинт:

```bash
curl http://localhost:8083/                          # вернёт {"version":"3.8.0",...}
curl http://localhost:8083/connector-plugins | grep -i postgres   # плагин на месте
```

Теперь регистрируем сам коннектор. Создай файл `connect/customers-connector.json` (скопируй блок целиком):

```bash
cat > connect/customers-connector.json << 'EOF'
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
EOF
```

Зарегистрируй:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connect/customers-connector.json
```

Проверь статус:

```bash
curl http://localhost:8083/connectors/customers-connector/status   # state: RUNNING
```

**Что здесь важно:**

- `topic.prefix=cdc` → события пойдут в топик **`cdc.public.customers`** (префикс + схема + таблица).
- `plugin.name=pgoutput` — встроенный плагин логической репликации PostgreSQL (не нужно ставить сторонние `decoderbufs`).
- `snapshot.mode=initial` — при первом старте Debezium снимет **снапшот** уже существующих строк (наши 3 клиента) и выложит их в топик как события с `op: "r"`.
- `value.converter.schemas.enable=false` — сообщения будут чистым JSON без обёртки `{"schema":...,"payload":...}`, так их проще читать в Spark.

> ⚠️ **`publication.autocreate.mode=filtered`.** Debezium сам создаст *publication* (список таблиц для репликации) и *replication slot*. Для этого нужен суперпользователь — мы и ходим под `postgres`.

---

## 6. Этап 5 — смотрим CDC-поток

Прочитай топик с начала (должны быть 3 события-снапшота, по одному на строку):

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.public.customers \
  --from-beginning --max-messages 3 --timeout-ms 15000
```

Ты увидишь JSON вида:

```json
{"before":null,"after":{"id":1,"name":"Alice","city":"Moscow","updated_at":"..."},"op":"r","ts_ms":1789986848629,...}
```

- `op` — тип операции: `r` (snapshot-чтение), `c` (insert), `u` (update), `d` (delete);
- `after` — строка после изменения (у `d` — `null`), `before` — строка до (у `c`/`r` — `null`);
- `ts_ms` — время события в миллисекундах.

Это и есть CDC: в топик попадают **только изменения**, а не вся таблица.

---

## 7. Этап 6 — Spark: jar'ы, конфиг, джоба

Образ `apache/spark:3.5.3` уже скачан (§1.3), Iceberg-каталог к Lakekeeper настроим в §7.2. Для этой лабораторной добавляем **Kafka-коннектор** и **джобу SCD2**.

**7.1. Jar'ы.** Создай папку `spark/jars` и скачай туда шесть файлов (Iceberg ×2 — для каталога Iceberg, Kafka ×4 — для чтения топика):

```bash
mkdir -p spark/jars spark/conf spark/checkpoint
cd spark/jars
curl -fsSL -o iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
curl -fsSL -o iceberg-aws-bundle-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
curl -fsSL -o spark-sql-kafka-0-10_2.12-3.5.3.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar
curl -fsSL -o spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar
curl -fsSL -o kafka-clients-3.4.1.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
curl -fsSL -o commons-pool2-2.11.1.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
cd ../..
```

> ⚠️ **`commons-pool2`.** `spark-sql-kafka` использует пул Kafka-консьюмеров из Apache Commons Pool. В Spark-образе этого jar'а нет — без него стрим падает с `ClassNotFoundException: org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig`. (lz4/snappy/zstd Spark уже бандлит, их добавлять не нужно.)

**7.2. Конфиг `spark/conf/spark-defaults.conf`** (каталог `scd` → тот же Lakekeeper, + список jar'ов) — скопируй блок целиком:

```bash
cat > spark/conf/spark-defaults.conf << 'EOF'
spark.sql.extensions                         org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.scd                        org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.scd.catalog-impl           org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.scd.uri                    http://host.docker.internal:8181/catalog
spark.sql.catalog.scd.warehouse              scd
spark.sql.catalog.scd.io-impl                org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.scd.s3.endpoint            http://host.docker.internal:9000
spark.sql.catalog.scd.s3.path-style-access   true
spark.sql.catalog.scd.s3.access-key-id       minioadmin
spark.sql.catalog.scd.s3.secret-access-key   minioadmin
spark.sql.catalog.scd.s3.region              us-east-1
spark.jars                                   /opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar,/opt/spark/extra-jars/iceberg-aws-bundle-1.7.1.jar,/opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/extra-jars/kafka-clients-3.4.1.jar,/opt/spark/extra-jars/commons-pool2-2.11.1.jar
EOF
```

**7.3. Джоба `spark/scd2.py`** (читает топик → парсит Debezium → SCD2 через `MERGE`) — скопируй блок целиком:

```bash
cat > spark/scd2.py << 'EOF'
"""
SCD Type 2 на CDC-потоке Debezium.

Читает Kafka-топик cdc.public.customers (Debezium-события из PostgreSQL),
применяет SCD Type 2 и пишет результат в Iceberg-таблицу scd.dim.customers.

Логика SCD2 в foreachBatch (на каждом микро-батче):
  1) для UPDATE и DELETE закрываем текущую версию (valid_to = время события, is_current = false);
  2) для INSERT / UPDATE / snapshot вставляем новую версию (valid_from = время события).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_TOPIC = "cdc.public.customers"
KAFKA_BOOTSTRAP = "host.docker.internal:9092"
TARGET_TABLE = "scd.dim.customers"

# Схема одной строки "after"/"before" (берём только id/name/city — остальное не нужно).
ROW_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
])

# Схема Debezium-конверта (value.converter.schemas.enable=false -> без обёртки "schema/payload").
PAYLOAD_SCHEMA = StructType([
    StructField("before", ROW_SCHEMA, True),
    StructField("after", ROW_SCHEMA, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
])

spark = (SparkSession.builder
         .appName("scd2-cdc-customers")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Целевая SCD2-таблица (format-version 2 обязателен для MERGE/row-level операций Iceberg).
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id          BIGINT,
        name        STRING,
        city        STRING,
        valid_from  TIMESTAMP,
        valid_to    TIMESTAMP,
        is_current  BOOLEAN
    ) USING iceberg
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
""")
print(f"[scd2] target table {TARGET_TABLE} ready")

# 2. CDC-поток из Kafka.
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", KAFKA_TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

# 3. Парсим Debezium JSON: строка берётся из after (или before, если это DELETE),
#    время изменения — из ts_ms конверта.
events = (raw
          .selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), PAYLOAD_SCHEMA).alias("e"))
          .select(
              coalesce(col("e.after.id"), col("e.before.id")).alias("id"),
              coalesce(col("e.after.name"), col("e.before.name")).alias("name"),
              coalesce(col("e.after.city"), col("e.before.city")).alias("city"),
              col("e.op").alias("op"),
              to_timestamp(col("e.ts_ms") / 1000).alias("ts"),
          ))


def apply_scd2(batch_df, epoch_id):
    """Один микро-батч: закрываем старые версии, вставляем новые."""
    rows = batch_df.collect()
    if not rows:
        return

    # Материализуем батч в обычный DataFrame: temp view над streaming-планом
    # не резолвится в batch-запросе MERGE (для учебной лабы объём крошечный).
    local = spark.createDataFrame(rows, schema=batch_df.schema)
    local.createOrReplaceTempView("updates")

    # (a) Закрыть текущие версии для UPDATE и DELETE.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW close_rows AS
        SELECT id, ts FROM updates WHERE op IN ('u', 'd')
    """)
    spark.sql("""
        MERGE INTO scd.dim.customers AS t
        USING close_rows AS s
        ON t.id = s.id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET t.valid_to = s.ts, t.is_current = false
    """)

    # (b) Вставить новую версию для INSERT, UPDATE и snapshot-чтения.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW new_rows AS
        SELECT id, name, city, ts FROM updates WHERE op IN ('c', 'r', 'u')
    """)
    spark.sql("""
        MERGE INTO scd.dim.customers AS t
        USING new_rows AS s
        ON t.id = s.id AND t.is_current = true
        WHEN NOT MATCHED THEN INSERT (id, name, city, valid_from, valid_to, is_current)
        VALUES (s.id, s.name, s.city, s.ts, NULL, true)
    """)

    print(f"[scd2] batch #{epoch_id} processed")


query = (events.writeStream
         .foreachBatch(apply_scd2)
         .outputMode("append")
         .option("checkpointLocation", "/opt/spark/checkpoint")
         .trigger(processingTime="3 seconds")
         .start())

print("[scd2] streaming started, waiting for CDC events...")
query.awaitTermination()
EOF
```

**Как читать джобу.** `foreachBatch` на каждом микро-батче (каждые 3 секунды) запускает функцию `apply_scd2`, которая делает два `MERGE`:

1. **Закрыть** — для `u`/`d` находит текущую версию (`is_current=true`) и проставляет `valid_to` + `is_current=false`.
2. **Вставить** — для `c`/`r`/`u` добавляет новую версию (`valid_from=ts`, `is_current=true`). Для `d` (delete) новой версии нет — строка просто закрывается.

> ⚠️ **Зачем `format-version=2`?** Row-level операции Iceberg (`MERGE` с `UPDATE`/`DELETE`) требуют таблиц формата v2. У v1 их нет — получишь ошибку при первом `MERGE`.

> ⚠️ **Зачем `collect()`?** В `foreachBatch` приходит DataFrame, привязанный к streaming-плану; если сделать над ним `createOrReplaceTempView` и тут же вызвать batch-`MERGE`, Spark не найдёт вьюху (`UnresolvedRelation`). `collect()` материализует крошечный батч в обычный DataFrame — для учебной лабораторной этого достаточно (в продакшене объёмы другие и пишут по-другому).

---

## 8. Этап 7 — запуск Spark-потока

Запускаем джобу фоном и следим за логом (Spark стартует ~40–50 с — это JVM + jar'ы):

```bash
docker run -d --name scd-spark \
  --add-host host.docker.internal:host-gateway \
  -v "$(pwd)/spark/conf:/opt/spark/conf" \
  -v "$(pwd)/spark/jars:/opt/spark/extra-jars" \
  -v "$(pwd)/spark/scd2.py:/opt/spark/scd2.py" \
  -v "$(pwd)/spark/checkpoint:/opt/spark/checkpoint" \
  apache/spark:3.5.3 \
  /opt/spark/bin/spark-submit /opt/spark/scd2.py

docker logs -f scd-spark
```

Дождись в логе строк `[scd2] streaming started` и затем `[scd2] batch #0 processed` — это Spark прочитал 3 снапшот-события из топика и записал их в Iceberg. Для выхода из `docker logs -f` — `Ctrl-C` (контейнер продолжит работать).

Проверь, что строки появились:

```bash
docker exec trino trino --execute "SELECT * FROM scd.dim.customers ORDER BY id;"
```

Должны быть 3 строки (Alice, Bob, Carol) с `is_current = true` и пустым `valid_to`.

> ⚠️ **Почему jar'ы монтируем в `/opt/spark/extra-jars`, а не `/opt/spark/jars`?** В `/opt/spark/jars` лежат библиотеки самого Spark — если смонтировать поверх, Spark не запустится (`ClassNotFoundException: org.apache.spark.launcher.Main`). Поэтому для наших jar'ов — отдельная папка, а путь указываем в `spark.jars`.

> 💡 **Не дождался `batch #0`?** Проверь, что коннектор RUNNING (§5) и топик непустой (§6). Spark читает с начала (`startingOffsets=earliest`), так что видит и уже записанные события.

---

## 9. Этап 8 — изменения и SCD2-история

Пока Spark крутится, начни менять источник. Делай изменения по одному, с паузой в несколько секунд — так каждое попадёт в отдельный микро-батч:

```bash
# 1) Bob переехал
docker exec pg-src psql -U postgres -d shop -c "UPDATE public.customers SET city='Novosibirsk' WHERE name='Bob';"
sleep 6
# 2) Alice сменила имя
docker exec pg-src psql -U postgres -d shop -c "UPDATE public.customers SET name='Alicia' WHERE id=1;"
sleep 6
# 3) новый клиент Dave
docker exec pg-src psql -U postgres -d shop -c "INSERT INTO public.customers (name, city) VALUES ('Dave','Yekaterinburg');"
sleep 6
# 4) Carol удалена
docker exec pg-src psql -U postgres -d shop -c "DELETE FROM public.customers WHERE name='Carol';"
```

В логе Spark (`docker logs scd-spark | grep '\[scd2\]'`) увидишь `batch #1..#4 processed`.

Теперь смотрим, что получилось:

**Полная история** (все версии):

```sql
SELECT id, name, city, valid_from, valid_to, is_current
FROM scd.dim.customers ORDER BY id, valid_from;
```

**Текущее состояние** (только актуальные версии):

```sql
SELECT id, name, city FROM scd.dim.customers WHERE is_current = true ORDER BY id;
```

Сверь с ожиданием: Alice стала Alicia (2 версии), Bob переехал (2 версии), Dave добавился (1 версия), Carol закрыта `valid_to` без новой версии. Итого **6 версий** и **3 актуальные строки**.

---

## 10. Этап 9 — Time Travel

Каждый микро-батч создаёт **снапшот** Iceberg. Посмотри их:

```sql
SELECT committed_at, snapshot_id, operation
FROM scd.dim."customers$snapshots"
ORDER BY committed_at;
```

> ⚠️ Кавычки обязательны: в имени системной таблицы есть `$`.

Ты увидишь, что каждое изменение даёт **два** снапшота: `overwrite` (закрытие старой версии — строка переписывается с новым `valid_to`) и `append` (вставка новой версии). `summary` каждого снапшота показывает, сколько строк добавлено/удалено.

Теперь — **Time Travel**. Возьми `snapshot_id` самого первого `append` (сразу после начальной загрузки, там 3 строки) и прочитай таблицу «на тот момент»:

```sql
SELECT id, name, city, is_current
FROM scd.dim.customers
FOR VERSION AS OF 7027931970046921607   -- подставь СВОЙ snapshot_id
ORDER BY id;
```

Должно вернуться **3 строки** (Alice, Bob, Carol) — то, что было до изменений.

То же самое — **по времени** (подставь момент после загрузки, но до изменений):

```sql
SELECT id, name, city
FROM scd.dim.customers
FOR TIMESTAMP AS OF TIMESTAMP '2026-09-21 10:46:20 UTC'   -- подставь СВОЁ время
ORDER BY id;
```

И, наконец, «середина» истории — момент после переезда Bob, но до переименования Alice (подставь время между соответствующими `committed_at`):

```sql
SELECT id, name, city, is_current
FROM scd.dim.customers
FOR TIMESTAMP AS OF TIMESTAMP '2026-09-21 10:47:55 UTC'
ORDER BY id, valid_from;
```

Ты увидишь 4 строки: Bob уже в двух версиях (старая «Saint Petersburg» закрыта, новая «Novosibirsk» актуальна), а Alice ещё «Alice».

### Два взгляда на «данные тогда»

Это ключевой вывод лабораторной:

- **SCD2** — «логическая» история: ты сам решаешь, какие колонки версионировать, и хранишь все версии в одной таблице. Запрос «как было» — это обычный `WHERE valid_from <= X AND (valid_to > X OR valid_to IS NULL)`.
- **Iceberg Time Travel** — «физическая» история: снапшоты — это реальные состояния файлов таблицы в момент каждого коммита. Запрос «как было» — `FOR VERSION/TIMESTAMP AS OF`, и неважно, версионировал ты колонки или нет.

---

## 10.5. Поиграться: 1000 INSERT + 1000 UPDATE

Когда конвейер работает, прогони нагрузку побольше и посмотри, как SCD2 ведёт себя на массовых изменениях. Всё по-прежнему: меняем источник, а Spark сам перекладывает изменения в историю.

**1. +1000 клиентов и обновление всех 1000.** Между командами пауза, чтобы изменения попали в разные снапшоты:

```bash
# 1) добавляем 1000 клиентов (получат id 5..1004)
docker exec pg-src psql -U postgres -d shop -c "INSERT INTO public.customers (name, city) SELECT 'Customer ' || g, 'City ' || (g % 50) FROM generate_series(1, 1000) AS g;"
sleep 10
# 2) все 1000 переезжают в «Metropolis»
docker exec pg-src psql -U postgres -d shop -c "UPDATE public.customers SET city = 'Metropolis-' || id WHERE id >= 5;"
```

**2. Смотрим, как поток переваривает нагрузку.** Батчей станет заметно больше, а предупреждение `batch is falling behind` на крупных батчах — норма (JVM и `collect()` + два `MERGE` на батч), это не ошибка:

```bash
docker logs scd-spark --tail 40 | grep '\[scd2\]'
```

**3. Считаем версии в Iceberg:**

```sql
SELECT count(*) AS total_versions FROM scd.dim.customers;
SELECT count(*) AS current_rows   FROM scd.dim.customers WHERE is_current = true;
```

Ожидание: `total_versions = 3006` (6 до нагрузки + 1000 insert + 1000 закрытий + 1000 новых версий от update), `current_rows = 1003` (Alicia, Bob, Dave + 1000 новых; Carol удалена и в «текущих» отсутствует).

**4. Бонус — Time Travel на нагрузке.** До обновления все 1000 ещё жили в `City %`, после — в `Metropolis-…`. Найди момент «между» по `committed_at` снапшотов:

```sql
SELECT committed_at, snapshot_id, operation
FROM scd.dim."customers$snapshots"
ORDER BY committed_at DESC LIMIT 5;
```

…и прочитай таблицу «до» обновления (подставь время после INSERT, но до UPDATE):

```sql
SELECT count(*) FROM scd.dim.customers
FOR TIMESTAMP AS OF TIMESTAMP '<подставь СВОЁ время> UTC';
```

Должно вернуться `1006` — это 6 исходных версий + 1000 добавленных, ещё без `Metropolis`.

---

## 11. Итоги и вопросы

1. **CDC.** Какие события (`op`) Debezium записал в топик и что лежит в `before`/`after` для каждого типа? Зачем нужен `wal_level=logical`?
2. **SCD2.** Объясни, как два `MERGE` в `apply_scd2` реализуют «закрыть старую версию + вставить новую». Почему для `DELETE` новой версии нет?
3. **Версии.** Сколько строк в таблице сейчас и сколько из них `is_current=true`? Чем `valid_to` актуальной строки отличается от остальных?
4. **Time Travel.** Чем `FOR VERSION AS OF` отличается от `FOR TIMESTAMP AS OF`? Почему снапшотов вдвое больше, чем изменений (подсказка: `overwrite` + `append`)?
5. **Два подхода.** В какой ситуации для «данных на вчера» ты бы выбрал SCD2, а в какой — Time Travel? Могут ли они дать разный ответ?

---

## 12. Если что-то пошло не так

- **Debezium: `FATAL: logical decoding requires wal_level >= logical`** — источник PG поднят без `-c wal_level=logical`. Пересоздай `pg-src` с флагами из §3.
- **Debezium: `password authentication failed for user "postgres"`** — в конфиге коннектора неверный `database.password`. Сверь с §5 (должен быть `postgres`).
- **Connect не стартует / `GET /connectors` не отвечает** — подожди дольше (JVM ~30–60 с): `docker logs connect`. Если ругается на Kafka — проверь, что Kafka жив (`docker logs kafka` → `Kafka Server started`).
- **Топик `cdc.public.customers` пустой** — коннектор не RUNNING: `curl http://localhost:8083/connectors/customers-connector/status`. Если `FAILED`, смотри `docker logs connect` и `curl http://localhost:8083/connectors/customers-connector/status` подробнее (там бывает сообщение об ошибке конфига).
- **Spark: `ClassNotFoundException: ...GenericKeyedObjectPoolConfig`** — не скачан `commons-pool2` (§7.1). Добавь jar и перезапусти `scd-spark`.
- **Spark: `The table or view 'updates' cannot be found`** — в `apply_scd2` не сделана материализация `collect()` (§7.3). Сверь джобу.
- **Spark: `MERGE` падает с ошибкой про format version** — таблица создана v1. Нужен `format-version=2` (джоба §7.3 создаёт её сама; если пересоздаёшь вручную — укажи v2).
- **`Catalog scd does not exist`** — не создан `scd.properties` или Trino/Spark не перезапущен. Проверь `SHOW CATALOGS` в Trino и `spark.sql.catalog.scd.*` в `spark-defaults.conf`.
- **Spark: `Multiple streaming queries ... using metadata` / `.metadata.crc already exists`** — чекпойнт-папка содержит старый стейт (в т.ч. скрытые файлы). Почисти её **целиком** и перезапусти: `rm -rf spark/checkpoint && mkdir spark/checkpoint`. (Команда `rm -rf spark/checkpoint/*` не удаляет скрытые файлы — это и есть причина.)
- **`No FileSystem for scheme "s3"`** — в `spark-defaults.conf` нет `io-impl=S3FileIO` и строк `s3.*` (§7.2).
- **Порт занят** — освободи или смени `-p` (тогда поменяй `database.port`/`BOOTSTRAP_SERVERS`/адрес Kafka везде).

---

## 13. Остановить и удалить всё

```bash
# 1. Контейнеры лабораторной
docker rm -f scd-spark connect kafka pg-src

# 2. Удаляем warehouse `scd` из Lakekeeper (таблица + namespace + сам warehouse)
SCD_ID=$(curl -s http://localhost:8181/management/v1/warehouse | python3 -c "import sys,json; print([w['warehouse-id'] for w in json.load(sys.stdin).get('warehouses',[]) if w['name']=='scd'][0])")
curl -s -X DELETE "http://localhost:8181/catalog/v1/$SCD_ID/namespaces/dim/tables/customers"
curl -s -X DELETE "http://localhost:8181/catalog/v1/$SCD_ID/namespaces/dim"
sleep 5   # Lakekeeper дочищает файлы таблицы в фоне
curl -s -X DELETE "http://localhost:8181/management/v1/warehouse/$SCD_ID"

# 3. Убираем каталог scd из Trino
TRINO_CATALOG="$(docker inspect trino -f '{{range .Mounts}}{{if eq .Destination "/etc/trino/catalog"}}{{.Source}}{{end}}{{end}}')"
rm -f "$TRINO_CATALOG/scd.properties"
docker restart trino

# 4. Удаляем бакет scd из MinIO
docker exec lakehouse-minio sh -c "mc alias set m http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; mc rb --force m/scd"

# 5. Базовый стек (§1.5) — если больше не нужен
docker rm -f trino lakekeeper lakehouse-pg lakehouse-minio
docker volume rm minio-data pg-data

# 6. Образы (скачаются заново при следующем запуске)
docker rmi apache/spark:3.5.3 \
          debezium/connect:3.0.0.Final \
          apache/kafka:3.8.0 \
          postgres:16-alpine \
          quay.io/lakekeeper/catalog:latest-main \
          trinodb/trino:476 \
          postgres:17 \
          pgsty/silo:latest \
          pgsty/mc:latest
```

Рабочую папку удали целиком: `rm -rf ~/scd2-lab`. Чтобы **просто приостановить** — `docker stop scd-spark connect kafka pg-src` без `rm`.

> ⚠️ **Шаг 2 можно пропустить** (warehouse/catalog/бакет останутся висеть — на повторный прогон это не влияет, §2.2 выдаст «warehouse уже есть» и ты просто пропустишь создание). Но для **чистого** повторения лабораторной шаг 2 обязателен: он убирает старые таблицы и снапшоты. Если удаление warehouse вернуло `Warehouse has unfinished tasks` — подожди ещё несколько секунд и повтори `curl -X DELETE .../warehouse/$SCD_ID`.

> 💡 **Важно про чекпойнт Spark.** Пока папка `spark/checkpoint` цела, Spark помнит, до какого места дочитал Kafka. Если перезапустишь джобу с той же папкой — она продолжит с последнего смещения, а не перечитает всё заново. Для «чистого» повторения лабораторной удали `spark/checkpoint` и пересоздай (или сбрось коннектор Debezium: `curl -X DELETE http://localhost:8083/connectors/customers-connector` и зарегистрируй заново).

---

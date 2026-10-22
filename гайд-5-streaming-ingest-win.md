# Гайд 5. Streaming-загрузка из публичного API: API → Kafka → Spark → Iceberg (+ Time Travel) — Windows

Стек: **Open-Meteo (погода) → Python-продюсер → Kafka → Spark Structured Streaming → Iceberg (Lakekeeper) → Trino**.

Это **облегчённая альтернатива Гайду 4**. Вместо PostgreSQL + Debezium + SCD Type 2 мы берём готовый поток данных из интернета (публичный API без ключа), кладём его в Kafka и дописываем в Iceberg простым **append**. Никакого CDC, никакого `MERGE` — при этом **Time Travel** работает как и раньше: каждый микро-батч — это новый снапшот Iceberg.

> ⚠️ **Важно перед стартом (обновление 2026): образы MinIO.** Официальные образы больше не раздаются бесплатно: `minio/minio` удалён с Docker Hub, `quay.io/minio/minio` и `quay.io/minio/mc` отвечают `401`, а AIStor требует лицензию. Используй свободный форк **Silo** — команды, порты и переменные окружения те же, меняется только имя образа:
> - сервер: `pgsty/silo:latest` (вместо `minio/minio` / `quay.io/minio/minio`);
> - клиент `mc`: `pgsty/mc:latest` (вместо `minio/mc` / `quay.io/minio/mc`).

> 🪟 **Windows (Docker Desktop, режим Linux-контейнеров).** Версия для macOS — в соседнем файле `гайд-5-streaming-ingest.md`.

> 📌 **Как читать Windows-версию.** Все команды — для **PowerShell** (Windows PowerShell 5.1 или PowerShell 7), Docker Desktop в режиме **Linux-контейнеров**. Отличия от macOS-версии:
> - `curl` в PowerShell — это алиас на `Invoke-WebRequest`, а **не** настоящий curl. Поэтому настоящий curl пишем явно: **`curl.exe`**.
> - Файлы создаём через here-string `@'...'@` + `[System.IO.File]::WriteAllText` — это пишет **UTF-8 без BOM** (иначе JSON / `.conf` / `.properties` / `.py` могут не прочитаться).
> - Продолжение длинной команды — обратный апостроф `` ` `` (вместо `\`).
> - `$(pwd)` → `"$PWD/..."`, `sleep` → `Start-Sleep -Seconds`, `grep` → `Select-String`, `rm -rf` → `Remove-Item -Recurse -Force`, `mkdir -p` → `New-Item -ItemType Directory -Force`.
> - `host.docker.internal` на Docker Desktop для Windows работает из коробки — все адреса те же, что и в macOS-версии.

---

## 0. Что мы строим и зачем

| | Гайд 4 (сложный) | **Гайд 5 (этот, лёгкий)** |
|---|---|---|
| Источник | PostgreSQL (меняем сами) | **Open-Meteo** — публичный API, данные летят сами |
| Захват | Debezium (CDC из WAL) | **Python-продюсер** (раз в 10 с опрашивает API) |
| Преобразование | SCD Type 2 (`MERGE`, 2 вьюхи) | **ничего** — просто append |
| Сложность | `format-version=2`, `collect()`, slot/replication | **нет** |
| Time Travel | есть | **есть** (снапшоты Iceberg) |

Схема:

```
┌────────────┐  HTTP   ┌────────────────┐ produce ┌───────┐ consume ┌────────────────┐ append ┌─────────┐ SQL ┌───────┐
│ Open-Meteo │ ──────► │ Python-продюсер │ ──────► │ Kafka │ ──────► │ Spark Structured │ ─────► │ Iceberg │ ───► │ Trino │
│  (погода)  │         │  (контейнер)    │         │       │         │ Streaming (append)│       │         │       │
└────────────┘         └────────────────┘         └───────┘         └──────────────────┘       └─────────┘       └───────┘
```

**Идея лабораторной.** Есть публичный сервис погоды (Open-Meteo) — бесплатный, без ключа. Наш продюсер каждые 10 секунд запрашивает текущую погоду в 5 городах и кладёт по одному JSON-сообщению на город в Kafka. Spark читает топик и дописывает строки в Iceberg-таблицу `scd.raw.weather`. Таблица **растёт** — и благодаря снапшотам Iceberg мы можем «отмотать время» и посмотреть, сколько данных было 5 минут назад.

### Что получится в конце

Таблица `scd.raw.weather` с колонками:

| city | temperature_2m | relative_humidity_2m | wind_speed_10m | observed_at | fetched_at |
|---|---|---|---|---|---|

Каждая строка — одно наблюдение о погоде. Наблюдения накапливаются: через минуту их ~30, через пять — ~150. Time Travel покажет, что на момент X строк было меньше, чем сейчас.

---

## 0.5. План на 2.5 часа

| Время | Блок | Что делаем | Чек-пойнт |
|---|---|---|---|
| 0:00–0:10 | Разминка | проверка стека; понимаем, что такое streaming-загрузка | стек поднят |
| 0:10–0:30 | Хранилище | бакет/warehouse `scd`, каталог, схема `raw` | `SHOW CATALOGS` видит `scd` |
| 0:30–0:50 | Kafka | один узел (KRaft) | `Kafka Server started` |
| 0:50–1:10 | Продюсер | `producer.py` + контейнер, Open-Meteo → Kafka | в топике видны сообщения |
| 1:10–1:20 | Проверка топика | console-consumer, разбираем JSON | поняли структуру сообщения |
| 1:20–1:50 | Spark | jar'ы, `spark-defaults.conf`, `ingest.py`, запуск | в Iceberg появились строки |
| 1:50–2:10 | Наблюдение | строки накапливаются, «свежая погода по городам» | счётчик растёт |
| 2:10–2:30 | Time Travel | снапшоты, `FOR VERSION/TIMESTAMP AS OF` | «данные тогда» меньше, чем сейчас |
| 2:30–2:45 | Итоги + уборка | вопросы §9, очистка §11 | лабораторная сдана |

> ⏱️ Самые долгие этапы — **скачивание образов** (`apache/kafka`, `python:3.12-slim`, `apache/spark`) и **первый запуск продюсера** (~30–60 с на `pip install`). Скачай образы заранее (§1.3), а Spark запускай сразу после продюсера — он прогревается, пока ты смотришь топик.

---

## 1. Пререквизиты

**1.1. Docker Desktop** — запущен (в режиме **Linux-контейнеров**, это значение по умолчанию). Проверь:

```powershell
docker --version
docker run --rm hello-world    # должно напечатать "Hello from Docker!"
```

**1.2. Свободные порты.** Гайд займёт порты `9000`, `9001` (MinIO/Silo), `5432` (Postgres Lakekeeper), `8181` (Lakekeeper), `8080` (Trino), `9092` (Kafka). Убедись, что все свободны:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 9000,9001,5432,8181,8080,9092 -ErrorAction SilentlyContinue
```

Пустой вывод = свободны. (В этой лабе порты 5433 и 8083 **не нужны** — PostgreSQL и Debezium мы не используем.)

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
docker pull python:3.12-slim
docker pull apache/spark:3.5.3
```

**1.4. Рабочая папка.** Все файлы лабораторной кладём в одну папку — `C:\stream-lab` (путь **без пробелов и кириллицы**, иначе у Docker могут быть проблемы с монтированием). Создай её со всеми подпапками и перейди в неё:

```powershell
New-Item -ItemType Directory -Force -Path C:\stream-lab\spark\jars, C:\stream-lab\spark\conf, C:\stream-lab\spark\checkpoint, C:\stream-lab\catalog | Out-Null
cd C:\stream-lab
```

Дальше все команды с относительными путями (`.\spark\...`, `.\producer.py`, `.\create-warehouse-scd.json`) выполняй из этой папки. Файлы создаются через here-string — просто копируй блок целиком, редактор не нужен.

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
| **Kafka** — брокер | host.docker.internal:9092 | — | без авторизации |
| **Open-Meteo** — API | https://api.open-meteo.com | — | без ключа |
| MinIO — консоль / S3 | localhost:9001 / :9000 | `minioadmin` | `minioadmin` |
| Lakekeeper — REST | localhost:8181 | — | без авторизации |
| Trino | localhost:8080 | `trino` | пусто |

---

## 2. Этап 1 — хранилище `scd` (бакет, warehouse, каталог, схема)

> Если делал Гайд 4 и `scd` уже создан — переходи сразу к §2.4.

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

**2.4. Схема `raw`** (в ней будет лежать сырой поток погоды):

```powershell
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS scd.raw"
```

Проверка: `docker exec trino trino --execute "SHOW SCHEMAS FROM scd"` → увидишь `raw`.

---

## 3. Этап 2 — Kafka (один узел, KRaft)

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

> 💡 **`ADVERTISED_LISTENERS=host.docker.internal:9092`** — Kafka сообщает клиентам адрес для подключения. Продюсер и Spark живут в других контейнерах, поэтому им нужен адрес хоста (`host.docker.internal`), а не `localhost` (который внутри контейнера указывал бы «на себя»).

> 💡 **Веб-интерфейс Kafka (необязательно).** В образе `apache/kafka` веб-UI нет. Хочешь смотреть топики и сообщения глазами — подними **kafka-ui** отдельным контейнером:

```powershell
docker run -d --name kafka-ui -p 8090:8080 `
  --add-host host.docker.internal:host-gateway `
  -e KAFKA_CLUSTERS_0_NAME=local `
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 `
  provectuslabs/kafka-ui:latest
```

→ открой **http://localhost:8090**: слева топики, внутри — сами сообщения. Топиков пока нет — появятся, когда заработает продюсер (следующий этап).

---

## 4. Этап 3 — продюсер (Open-Meteo → Kafka)

Публичный API **Open-Meteo** отдаёт текущую погоду без ключа и без регистрации. Убедись, что он отвечает:

```powershell
curl.exe -s "https://api.open-meteo.com/v1/forecast?latitude=55.7558&longitude=37.6173&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
```

Увидишь JSON с блоком `current` — температура, влажность, ветер.

Теперь скрипт продюсера. Он каждые 10 секунд опрашивает 5 городов и шлёт по одному сообщению на город в топик `weather`:

```powershell
[System.IO.File]::WriteAllText("$PWD\producer.py", @'
"""Продуцирует текущую погоду Open-Meteo в Kafka (топик weather)."""
import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

CITIES = [
    ("Moscow", 55.7558, 37.6173),
    ("Saint Petersburg", 59.9343, 30.3351),
    ("Kazan", 55.7887, 49.1221),
    ("Yekaterinburg", 56.8389, 60.6057),
    ("Novosibirsk", 55.0084, 82.9357),
]

API = "https://api.open-meteo.com/v1/forecast"
BOOTSTRAP = "host.docker.internal:9092"
TOPIC = "weather"
INTERVAL = 10  # секунд между опросами

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"[producer] пишу в {TOPIC} каждые {INTERVAL} с (Kafka: {BOOTSTRAP})", flush=True)

while True:
    fetched_at = datetime.now(timezone.utc).isoformat()
    for city, lat, lon in CITIES:
        try:
            r = requests.get(API, params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            }, timeout=10)
            r.raise_for_status()
            cur = r.json()["current"]
            msg = {
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "temperature_2m": cur["temperature_2m"],
                "relative_humidity_2m": cur["relative_humidity_2m"],
                "wind_speed_10m": cur["wind_speed_10m"],
                "observed_at": cur["time"],
                "fetched_at": fetched_at,
            }
            producer.send(TOPIC, value=msg)
            print(f"[producer] {city}: {cur['temperature_2m']}°C, "
                  f"влажность {cur['relative_humidity_2m']}%", flush=True)
        except Exception as e:
            print(f"[producer] ошибка по {city}: {e}", flush=True)
    producer.flush()
    time.sleep(INTERVAL)
'@)
```

Запускаем продюсера в контейнере (первый запуск ~30–60 с — качается образ и ставятся `requests` + `kafka-python`):

```powershell
docker run -d --name producer `
  --add-host host.docker.internal:host-gateway `
  -v "$PWD/producer.py:/app/producer.py" `
  python:3.12-slim `
  sh -c "pip install -q requests kafka-python && python /app/producer.py"

docker logs -f producer    # Ctrl-C для выхода; контейнер продолжит работать
```

В логе появятся строки вида `[producer] Moscow: 19.0°C, влажность 60%` — по 5 городов за цикл.

> 💡 **Что делает продюсер.** `requests.get` забирает `current` у Open-Meteo; `KafkaProducer.send` кладёт JSON в топик `weather`; `time.sleep(10)` задаёт частоту. `fetched_at` — это **наше** время опроса (уникально для каждого цикла), `observed_at` — время наблюдения от Open-Meteo.

---

## 5. Этап 4 — смотрим топик

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic weather `
  --from-beginning --max-messages 5 --timeout-ms 15000
```

Ты увидишь 5 JSON-сообщений (по одному на город) вида:

```json
{"city": "Moscow", "latitude": 55.7558, "temperature_2m": 19.0, "relative_humidity_2m": 60, "wind_speed_10m": 8.0, "observed_at": "2026-09-21T12:15", "fetched_at": "2026-09-21T12:24:54.591928+00:00"}
```

Запомни структуру — по ней мы зададим схему в Spark.

---

## 6. Этап 5 — Spark: jar'ы, конфиг, джоба

**6.1. Jar'ы.** Скачай те же шесть файлов, что и в гайде 4 (Iceberg ×2 + Kafka ×4):

```powershell
New-Item -ItemType Directory -Force -Path spark\jars, spark\conf, spark\checkpoint | Out-Null
cd spark\jars
curl.exe -fsSL -o iceberg-spark-runtime-3.5_2.12-1.7.1.jar `
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
curl.exe -fsSL -o iceberg-aws-bundle-1.7.1.jar `
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
curl.exe -fsSL -o spark-sql-kafka-0-10_2.12-3.5.3.jar `
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar
curl.exe -fsSL -o spark-token-provider-kafka-0-10_2.12-3.5.3.jar `
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar
curl.exe -fsSL -o kafka-clients-3.4.1.jar `
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
curl.exe -fsSL -o commons-pool2-2.11.1.jar `
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
cd ..\..
```

> ⚠️ **`commons-pool2` обязателен** — без него `spark-sql-kafka` падает с `ClassNotFoundException: ...GenericKeyedObjectPoolConfig`.

**6.2. Конфиг `spark/conf/spark-defaults.conf`** (каталог `scd` → Lakekeeper, + jar'ы) — скопируй блок целиком:

```powershell
[System.IO.File]::WriteAllText("$PWD\spark\conf\spark-defaults.conf", @'
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
'@)
```

**6.3. Джоба `spark/ingest.py`** — читает топик, парсит JSON, дописывает в Iceberg:

```powershell
[System.IO.File]::WriteAllText("$PWD\spark\ingest.py", @'
"""Streaming-загрузка: Kafka (топик weather) → Iceberg (append-only).

Никакого SCD/CDC — просто читаем поток и дописываем строки в таблицу.
Каждый микро-батч = новый снапшот Iceberg, поэтому Time Travel работает.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_TOPIC = "weather"
KAFKA_BOOTSTRAP = "host.docker.internal:9092"
TARGET_TABLE = "scd.raw.weather"

# Схема JSON-сообщения продюсера (все поля — из producer.py).
SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("observed_at", StringType(), True),
    StructField("fetched_at", StringType(), True),
])

spark = (SparkSession.builder
         .appName("stream-ingest-weather")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Целевая таблица (append-only — MERGE и format-version=2 не нужны).
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        city STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        temperature_2m DOUBLE,
        relative_humidity_2m DOUBLE,
        wind_speed_10m DOUBLE,
        observed_at STRING,
        fetched_at STRING
    ) USING iceberg
""")
print(f"[ingest] target table {TARGET_TABLE} ready")

# 2. Поток из Kafka.
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", KAFKA_TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

# 3. Парсим JSON: каждая строка = одно наблюдение о погоде.
events = (raw
          .selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), SCHEMA).alias("d"))
          .select("d.*"))


def append_batch(batch_df, epoch_id):
    """Просто дописываем микро-батч в Iceberg (append)."""
    batch_df.writeTo(TARGET_TABLE).append()
    print(f"[ingest] batch #{epoch_id} processed")


query = (events.writeStream
         .foreachBatch(append_batch)
         .outputMode("append")
         .option("checkpointLocation", "/opt/spark/checkpoint")
         .trigger(processingTime="10 seconds")
         .start())

print("[ingest] streaming started, waiting for weather events...")
query.awaitTermination()
'@)
```

**Как читать джобу.** `readStream` читает Kafka; `from_json` разбирает каждое сообщение по `SCHEMA`; `foreachBatch` каждые 10 секунд вызывает `append_batch`, который одной строкой `writeTo(...).append()` дописывает батч в Iceberg. Всё — **никакого `MERGE`, никакого SCD2**.

> 💡 **Почему проще, чем в гайде 4?** Там каждая строка могла меняться, поэтому нужен был `MERGE` и закрытие версий. Здесь данные — это **события** (наблюдения): они только добавляются, ничего не перезаписывается. Поэтому хватает обычного `append`.

---

## 7. Этап 6 — запуск Spark-потока

```powershell
docker run -d --name ingest-spark `
  --add-host host.docker.internal:host-gateway `
  -v "$PWD/spark/conf:/opt/spark/conf" `
  -v "$PWD/spark/jars:/opt/spark/extra-jars" `
  -v "$PWD/spark/ingest.py:/opt/spark/ingest.py" `
  -v "$PWD/spark/checkpoint:/opt/spark/checkpoint" `
  apache/spark:3.5.3 `
  /opt/spark/bin/spark-submit /opt/spark/ingest.py

docker logs -f ingest-spark
```

Дождись `[ingest] streaming started` и затем `[ingest] batch #0 processed` (~40–50 с — JVM + jar'ы). Для выхода из `docker logs -f` — `Ctrl-C` (контейнер продолжит работать). Проверь таблицу:

```powershell
docker exec trino trino --execute "SELECT * FROM scd.raw.weather LIMIT 5"
```

Должны быть первые строки с погодой по городам.

> 💡 **Про пути в `-v`.** `$PWD` разворачивается в `C:\stream-lab`, а Docker Desktop понимает смешанные разделители `C:\.../...` и автоматически монтирует Windows-папку в Linux-контейнер. Поэтому `"$PWD/spark/conf:/opt/spark/conf"` работает как есть.

> ⚠️ **Почему jar'ы монтируем в `/opt/spark/extra-jars`, а не `/opt/spark/jars`?** В `/opt/spark/jars` лежат библиотеки самого Spark — если смонтировать поверх, Spark не запустится. Поэтому для наших jar'ов — отдельная папка, а путь указываем в `spark.jars`.

---

## 8. Этап 7 — наблюдение и Time Travel

Продюсер продолжает слать данные, Spark — дописывать. Через пару минут накопится заметная история.

**Сколько строк сейчас и было на старте:**

```powershell
docker exec trino trino --execute "SELECT count(*) AS rows FROM scd.raw.weather"
```

**Свежая погода по каждому городу** (последнее наблюдение):

```sql
SELECT city, temperature_2m, relative_humidity_2m, wind_speed_10m, fetched_at
FROM (
  SELECT city, temperature_2m, relative_humidity_2m, wind_speed_10m, fetched_at,
         row_number() OVER (PARTITION BY city ORDER BY fetched_at DESC) AS rn
  FROM scd.raw.weather
) WHERE rn = 1 ORDER BY city;
```

**Снапшоты** (каждый микро-батч = один `append`-снапшот):

```sql
SELECT committed_at, snapshot_id, operation
FROM scd.raw."weather$snapshots"
ORDER BY committed_at;
```

> ⚠️ Кавычки вокруг `weather$snapshots` обязательны (`$` в имени).

**Time Travel.** Возьми `snapshot_id` самого первого снапшота (сразу после `batch #0`) и сравни:

```sql
-- сколько строк было на первом снапшоте
SELECT count(*) AS rows
FROM scd.raw.weather
FOR VERSION AS OF 2801610465087767715;   -- подставь СВОЙ snapshot_id

-- сколько строк было 2 минуты назад
SELECT count(*) AS rows
FROM scd.raw.weather
FOR TIMESTAMP AS OF TIMESTAMP '2026-09-21 12:26:30 UTC';   -- подставь СВОЁ время
```

Ты увидишь, что «тогда» строк **меньше**, чем сейчас — таблица росла, а снапшоты Iceberg зафиксировали её состояние в каждый момент. Это и есть Time Travel: без всякого SCD2 мы можем читать данные «как они были N минут назад», потому что каждый append-батч — это точка во времени.

> 💡 **Как выполнить SQL-блоки выше?** Так же, как в §7, — через `docker exec trino trino --execute "..."`, либо через веб-интерфейс Trino http://localhost:8080 (кнопка запуска запроса). Для запросов с `$` в имени системной таблицы используй одинарные кавычки в PowerShell: `docker exec trino trino --execute 'SELECT ... FROM scd.raw."weather$snapshots" ...'`.

### Вывод

- **Streaming ingest** — данные не «грузим раз в день», а они **постоянно прилетают** (API → Kafka → таблица).
- **Append-only** — события только добавляются, поэтому никакого `MERGE`/SCD не нужно.
- **Time Travel** — даже для append-only таблицы снапшоты Iceberg дают «снимки на момент X».

---

## 9. Итоги и вопросы

1. **Поток.** Чем streaming-загрузка отличается от batch (как в гайдах 1–3)? Где в схеме находится Kafka и зачем она нужна?
2. **Продюсер.** Что делает `KafkaProducer.send` и `value_serializer`? Зачем мы добавляем `fetched_at`?
3. **Схема.** Почему `temperature_2m` — `DoubleType`, а `fetched_at` — `StringType`? Что будет, если поле в JSON отсутствует?
4. **Append vs SCD2.** Почему здесь хватило `append`, а в гайде 4 понадобился `MERGE`? В чём принципиальная разница между «событием» и «изменяемой строкой»?
5. **Time Travel.** Почему на первом снапшоте строк меньше, чем сейчас? Как `FOR VERSION AS OF` узнаёт, какие файлы читать?

---

## 10. Если что-то пошло не так

- **Продюсер: `error ... Connection refused`** — Kafka не поднят или продюсер не видит `host.docker.internal:9092`. Проверь `docker logs kafka` → `Kafka Server started`, и что контейнер запущен с `--add-host host.docker.internal:host-gateway`.
- **Продюсер: ошибка по всем городам (`ConnectionError`/`Timeout`)** — нет доступа в интернет из контейнера или Open-Meteo временно недоступен. Проверь `curl.exe -s "https://api.open-meteo.com/..."` с хоста.
- **Топик `weather` пустой** — продюсер ещё качает образ/ставит пакеты. Подожди и посмотри `docker logs producer` (строка `[producer] пишу в weather`).
- **Spark: `ClassNotFoundException: ...GenericKeyedObjectPoolConfig`** — не скачан `commons-pool2` (§6.1).
- **Spark: `Catalog scd does not exist`** — не создан `scd.properties` или Trino/Spark не перезапущен (§2.3, §6.2).
- **Spark: `.metadata.crc already exists` / `Multiple streaming queries`** — чекпойнт содержит старый стейт. Почисти **целиком**: `Remove-Item -Recurse -Force spark\checkpoint; New-Item -ItemType Directory -Force spark\checkpoint`.
- **`No FileSystem for scheme "s3"`** — в `spark-defaults.conf` нет `io-impl=S3FileIO` и строк `s3.*` (§6.2).
- **Порт `9092` занят** — освободи или смени `-p` (тогда поправь `BOOTSTRAP` в `producer.py` и `KAFKA_BOOTSTRAP` в `ingest.py`).

---

## 11. Остановить и удалить всё

```powershell
# 1. Контейнеры лабораторной
docker rm -f ingest-spark producer kafka

# 2. Удаляем warehouse `scd` из Lakekeeper (таблица + namespace + warehouse)
$SCD_ID = (curl.exe -s http://localhost:8181/management/v1/warehouse | ConvertFrom-Json).warehouses | Where-Object { $_.name -eq 'scd' } | Select-Object -ExpandProperty 'warehouse-id'
curl.exe -s -X DELETE "http://localhost:8181/catalog/v1/$SCD_ID/namespaces/raw/tables/weather"
curl.exe -s -X DELETE "http://localhost:8181/catalog/v1/$SCD_ID/namespaces/raw"
Start-Sleep -Seconds 5   # Lakekeeper дочищает файлы в фоне
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
docker rmi apache/spark:3.5.3 `
          apache/kafka:3.8.0 `
          python:3.12-slim `
          quay.io/lakekeeper/catalog:latest-main `
          trinodb/trino:476 `
          postgres:17 `
          pgsty/silo:latest `
          pgsty/mc:latest
```

Рабочую папку удали целиком: `Remove-Item -Recurse -Force C:\stream-lab`. Чтобы **просто приостановить** — `docker stop ingest-spark producer kafka` без `rm`.

> ⚠️ **Шаг 2 можно пропустить** (warehouse/каталог/бакет останутся висеть — на повторный прогон это не влияет, §2.2 выдаст «warehouse уже есть»). Для **чистого** повторения шаг 2 обязателен. Если удаление вернуло `Warehouse has unfinished tasks` — подожди ещё несколько секунд и повтори удаление.

> 💡 **Важно про чекпойнт Spark.** Пока папка `spark\checkpoint` цела, Spark помнит, до какого места дочитал Kafka. Если перезапустишь джобу с той же папкой — она продолжит с последнего смещения, а не перечитает всё заново. Для «чистого» повторения лабораторной удали `spark\checkpoint` и пересоздай.

---

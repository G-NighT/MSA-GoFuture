# ADR-005: Единая платформа данных для ML (pricing/demand/fraud) и BI

## Контекст

GoFuture строит микросервисы (Booking/Driver/Pricing/Payments/Notification/Geography/Fraud/Analytics) и событийную платформу (Kafka + Flink/Streams).
Бизнес требует real-time динамического ценообразования, прогнозирования спроса и выявления мошенничества. Для этого нужна единая платформа:

- сбор данных из всех микросервисов (события + операционные данные),
- near-real-time и batch обработка,
- ML-пайплайны (feature engineering, обучение, деплой),
- BI-витрины для DataLens,
- (доп.) механизмы качества данных и governance.

## Требования

1. Поддержка real-time потоков (секунды/десятки секунд) для pricing и anti-fraud.
2. Поддержка batch (минуты/часы) для обучения моделей, отчётности и пересчётов.
3. Единый “источник событий” из доменов (event log) + возможность подтягивать операционные данные.
4. Трассируемость (lineage), воспроизводимость ML (версии данных/фич/моделей).
5. Контроль качества данных (валидность, полнота, свежесть, отсутствие дрейфа).
6. Интеграция с BI (DataLens) и ML-платформой (обучение + online serving).

## Решение

### 1) Общая схема

**Два контура данных:**

- **Streaming контур (real-time):** Kafka → Flink/KStreams → Feature Store (online) + “Realtime витрины” (ClickHouse) → ML serving (pricing/fraud near-real-time).
- **Batch контур (historical):** Kafka + CDC/outbox → Data Lake (Object Storage) → Spark/Flink batch → DWH/ClickHouse → обучение ML + BI.

**Почему так:**

- pricing и dispatch требуют low-latency признаков/агрегатов (стриминг),
- обучение и отчётность требуют больших исторических данных (batch),
- event log (Kafka) помогает унифицировать сбор и обеспечивает replay.

### 2) Источники данных

- **Доменные события из Kafka** (основной поток): booking/driver/pricing/payments/fraud/geo/notification.
- **CDC из OLTP БД сервисов (опционально):** Debezium/Kafka Connect для “табличных” данных, если события недостаточны или нужно полное состояние.
- **Внешние источники:** карты/погода/партнёрские данные (если есть) через отдельные ingestion jobs.

### 3) Хранилища и витрины

- **Object Storage (Data Lake):** сырой слой (raw) + очищенный (silver) + витрины (gold), партиционирование по дате/региону.
- **ClickHouse:** near-real-time аналитика, дешёвые агрегаты, витрины для BI.
- **Feature Store (например, Feast):**
  - online store (Redis/Cassandra/…): быстрый доступ сервисам/ML serving,
  - offline store (Data Lake/ClickHouse): воспроизводимость обучения.

### 4) ML платформа (пример)

- **Orchestrator:** Airflow (или аналог) для batch-пайплайнов и retraining расписаний.
- **Experiment tracking/registry:** MLflow (эксперименты, артефакты, реестр моделей).
- **Training:** Spark/Flink batch + GPU/CPU jobs в кластере (K8s).
- **Serving:**
  - online inference (модель как сервис) для Pricing/Fraud,
  - A/B и canary (по feature flags),
  - мониторинг дрейфа и качества предсказаний.

### 5) Интеграция с BI

- Основная точка: **ClickHouse витрины** (или DWH на его базе) → **DataLens**.
- Отдельные датасеты по регионам (и комплаенс/PII): обезличивание/агрегация перед BI.

## (Дополнительно) Механизмы обеспечения качества данных

### 1) Контрактность и совместимость

- **Schema Registry** для событий (Avro/Protobuf), политика совместимости (BACKWARD/FULL) для ключевых топиков.
- Consumer-driven contracts для критичных потоков (pricing/fraud).

### 2) Валидации на ingestion и в пайплайне

- Data Quality checks (например, Great Expectations / Deequ):
  - типы, обязательные поля, диапазоны,
  - референциальная целостность на ключах,
  - дедупликация по `event_id`,
  - проверка распределений (аномалии/выбросы).

### 3) Свежесть и полнота

- Метрики “freshness” и “completeness”:
  - lag по топикам, задержка event-time,
  - “сколько событий ожидаем/получили” по доменам,
  - возраст самых старых данных в витрине/feature store.

### 4) Lineage и каталог данных

- Lineage (OpenLineage/Marquez) + каталог (DataHub/Amundsen), чтобы понимать “откуда взялась фича/витрина”.
- Версионирование датасетов (датированные партиции + immutable raw).

### 5) Защита данных и комплаенс

- Классификация данных: PII/PCI/Telemetry.
- Маскирование/токенизация PII, разделение доступа (RBAC), аудит.
- Региональные политики хранения: данные “home region” + ограничение репликации.

## Альтернативы

- Только ClickHouse без Data Lake: проще, но хуже для воспроизводимости обучения и долгосрочного хранения.
- Только batch (без streaming): не выполнит требования real-time pricing/fraud.
- “События не нужны, достаточно CDC”: повышает связность с OLTP и усложняет эволюцию доменов.

## Компромиссы/риски

- Два контура (stream+batch) повышают сложность эксплуатации; нужен сильный monitoring/ownership.
- Качество данных требует дисциплины контрактов и регламентов (data governance).
- Feature store добавляет компонент, но сильно снижает риск “train/serve skew”.

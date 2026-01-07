# ADR-003: Событийная платформа (Event-Driven) для GoFuture

## Контекст

GoFuture переходит от монолита к доменным микросервисам. Цели: обработка 500k конкурентных поездок, real-time dynamic pricing и “умное” перераспределение водителей. Нужна событийная платформа с гарантированной доставкой, обработкой в реальном времени и наблюдаемостью.

Текущий стек observability: Prometheus/Grafana/Loki/Alertmanager. Асинхронщина: RabbitMQ (остается для task-queue), но для доменных событий нужен Kafka.

## Требования

- Доменные события (BookingCreated, DriverLocationUpdated и др.) с версионированием.
- Kafka топики + партиционирование по регионам (мульти-регион).
- Real-time обработчики (Kafka Streams и/или Flink).
- Saga для критических бизнес-процессов (создание поездки, назначение, оплата).
- Надёжная доставка: отсутствие потерь, идемпотентность, повторная обработка, DLQ.
- Мониторинг: метрики, логи, трейсы, алерты; инструменты отражены в To-Be C2.

## Решение

### 1) Модель доменных событий

**Формат события (рекомендуемый):**

- envelope: `event_id`, `event_type`, `event_version`, `occurred_at`, `producer`, `region`, `trace_id`, `correlation_id`, `key` (entity id)
- payload: бизнес-поля
- schema: Avro/Protobuf (предпочтительно) + schema registry; либо JSON + строгие контракты (хуже для эволюции)

**Примеры событий (минимальный набор):**
Booking:

- `BookingCreated.v1`
- `BookingCancelled.v1`
- `TripAssigned.v1`
- `TripStarted.v1`
- `TripCompleted.v1`

Driver:

- `DriverLocationUpdated.v1`
- `DriverStatusChanged.v1` (Online/Offline/Busy)

Pricing:

- `PriceQuoteRequested.v1` (если нужен запрос как событие)
- `PriceQuoted.v1`
- `SurgeFactorUpdated.v1` (регион/зона)

Payments:

- `PaymentAuthorizationRequested.v1`
- `PaymentAuthorized.v1`
- `PaymentFailed.v1`
- `PaymentCaptureRequested.v1`
- `PaymentCaptured.v1`
- `RefundIssued.v1`
- `PayoutRequested.v1` / `PayoutCompleted.v1` / `PayoutFailed.v1`

Fraud:

- `FraudCheckRequested.v1`
- `FraudCheckPassed.v1`
- `FraudCheckFailed.v1` / `RiskFlagRaised.v1`

Notification:

- `NotificationRequested.v1`
- `NotificationSent.v1`
- `NotificationFailed.v1`

### 2) Kafka топики и партиционирование “по регионам”

**Стратегия:** topic-per-domain + композитный ключ, который гарантирует упорядочивание _внутри сущности_ и при этом группирует нагрузку по региону.

**Нейминг (пример):**

- `gf.booking.events.v1`
- `gf.driver.events.v1`
- `gf.pricing.events.v1`
- `gf.payments.events.v1`
- `gf.fraud.events.v1`
- `gf.notifications.events.v1`
- `gf.dlq.v1` (или по доменам: `gf.booking.dlq.v1`)

**Ключ партиционирования (пример):**

- Для поездок: `key = region_id + ":" + trip_id`
- Для водителей: `key = region_id + ":" + driver_id`
- Для pricing зон: `key = region_id + ":" + zone_id`

**Партиции:**

- `partitions = regions * partitions_per_region`, например, 3 региона × 200 = **600 партиций** на критичных топиках (Booking/Driver/Pricing).
- Репликация: RF=3, `acks=all`, `min.insync.replicas=2`.

**Мульти-региональный подход (рекомендованный):**

- Kafka кластер **в каждом регионе** для низкой латентности и локальной устойчивости.
- Репликация “наружу” (в глобальный аналитический кластер) через **MirrorMaker 2** или Kafka Replicator:
  - для Analytics/BI и cross-region отчётности,
  - для антифрода (если нужен глобальный контекст).

### 3) Real-time обработка событий (KStreams/Flink)

**Kafka Streams (KStreams)** — для lightweight потоковой логики рядом с сервисом:

- агрегации, join по ключам, простые окна (например: count requests per zone)
- хорошо для “near-service” processing

**Flink** — для тяжёлой real-time обработки:

- сложные оконные агрегаты спрос/предложение,
- stateful вычисления для surge и “умного” распределения,
- exactly-once с checkpointing.

**Ключевые пайплайны:**

1. **Pricing Stream (Flink)**

   - inputs: `gf.booking.events.v1` (Created/Cancelled), `gf.driver.events.v1` (Status/Location), внешние сигналы
   - output: `PriceQuoted.v1`, `SurgeFactorUpdated.v1`

2. **Dispatch/Matching Stream (Flink или KStreams)**

   - inputs: `BookingCreated.v1`, `DriverLocationUpdated.v1`, `DriverStatusChanged.v1`, `SurgeFactorUpdated.v1`
   - output: `TripAssigned.v1` (или `AssignmentProposed.v1` → Booking подтверждает)

3. **Analytics Ingestion**
   - inputs: все доменные топики
   - output: ClickHouse (near-real-time)

### 4) Saga: выбор вида + события

**Выбор:** **Orchestration Saga** для core-flow (Booking), потому что:

- критично управлять последовательностью и компенсациями,
- проще контролировать SLA и деградацию,
- меньше риск “расползания” бизнес-правил по множеству сервисов.

**Где живёт оркестратор:** в Booking Service (в будущем можно вынести в отдельный Trip Orchestrator, но на старте проще внутри Booking).

**Основной сценарий “создание поездки”:**

1. Client → Booking API: создать поездку
2. Booking пишет в свою БД (и outbox) и публикует `BookingCreated.v1`
3. Pricing (Flink/KStreams) формирует `PriceQuoted.v1`
4. Booking получает `PriceQuoted.v1`, публикует `PaymentAuthorizationRequested.v1`
5. Payments публикует `PaymentAuthorized.v1` или `PaymentFailed.v1`
6. Booking при Authorized публикует `DriverAssignmentRequested.v1` (или просто использует `BookingCreated`, как триггер dispatch’а)
7. Dispatch публикует `TripAssigned.v1`
8. По завершению: `TripCompleted.v1` → `PaymentCaptureRequested.v1` → `PaymentCaptured.v1`

**Компенсации:**

- если PaymentFailed → Booking публикует `BookingCancelled.v1`
- если не назначили водителя за SLA → `BookingCancelled.v1` + `PaymentReversed.v1` (если успели авторизовать)
- если отмена пассажира после авторизации → refund flow

Опционально: часть доменов может жить в choreography, но core booking — оркестрация.

### 5) Надёжная доставка и обработка (Reliability)

**Проблема:** нельзя терять события и нельзя “ломать” бизнес от дублей.

Решение (комбинация):

- **Outbox pattern** в сервисных БД:
  - сервис пишет изменение состояния + запись outbox в одной транзакции
  - публикация в Kafka выполняется relay’ем/CDC
- **CDC (Debezium + Kafka Connect)** для публикации outbox → Kafka
  - снижает риск ошибок dual-write в Django/Python
- Продюсеры Kafka:
  - `acks=all`, идемпотентный продюсер, retries, backoff
- Консьюмеры:
  - идемпотентные хэндлеры (dedup по `event_id`)
  - retry topics (например: `gf.retry.5s`, `gf.retry.1m`, `gf.retry.10m`)
  - **DLQ** для “ядовитых” сообщений (с алертом)
- Exactly-once:
  - Flink: checkpoints + transactional sink
  - Kafka Streams: EOS v2 (если используем KStreams)
- Эволюция схем:
  - schema registry, совместимость (BACKWARD/ FULL) для ключевых топиков
- Трассировка:
  - `trace_id`/`correlation_id` в headers сообщений, чтобы склеить E2E путь

## Мониторинг: подход и метрики

### Инструменты

- **Prometheus** — сбор метрик
- **Grafana** — визуализация, SLO dashboards
- **Alertmanager** — алерты
- **Loki** — логи
- **OpenTelemetry Collector** — единая точка сбора трейсов/метрик/логов (корреляция)
- Рекомендация: **Grafana Tempo** или Jaeger — хранение и поиск трейсов
- **Kafka Exporter / JMX Exporter** — метрики брокеров
- **Burrow / Kafka Lag Exporter** — consumer lag и здоровье групп
- **Flink metrics reporter → Prometheus** — метрики Flink job’ов
- **Schema Registry metrics** — совместимость/ошибки

### Метрики (минимальный обязательный список)

**Kafka (инфраструктура):**

- Under-replicated partitions, ISR shrink/expand
- Broker CPU/mem/disk, network in/out
- Request latency (produce/fetch), error rate
- Topic bytes in/out, messages in/out
- Partition skew / hot partitions
- Controller events, offline partitions

**Kafka consumers/streams:**

- Consumer lag (per group/topic/partition)
- Rebalance count/time
- Processed records/sec, commit latency
- DLQ size / rate
- Retry topic backlog

**Flink/KStreams:**

- Checkpoint duration/failure rate
- Backpressure time
- Task busy time, restart count
- End-to-end event latency (event-time vs processing-time)
- State size / RocksDB compaction (если применимо)

**Outbox/CDC:**

- Outbox backlog size
- Oldest outbox record age
- Debezium connector status, lag, errors

**Бизнес-метрики (SLO-ориентированные):**

- Booking created/sec per region
- % успешных назначений водителя (Assignment success rate)
- Latency: booking→price, booking→assigned, assigned→start, start→complete
- Payment auth success rate, capture success rate, refund rate
- Surge factor distribution (антианомалии)
- Fraud reject rate + false positives (если есть ground truth)
- Notification delivery success rate + latency

### Обоснование

- Для 500k конкурентных поездок критично видеть не только инфраструктурные метрики Kafka, но и **lag/latency** по пайплайнам (иначе “реалтайм” становится batch).
- Для динамического ценообразования важны **event-time latency**, backpressure и checkpoint stability.
- Для минимизации MTTR — корреляция logs+metrics+traces через OTel и единые `trace_id`.

## Альтернативы

- Choreography Saga для всего — проще технически, но риск неконтролируемого бизнес-флоу и сложнее SLA/компенсации.
- Один глобальный Kafka кластер — проще, но хуже latency и отказоустойчивость в мульти-регионе.
- Только RabbitMQ — не подходит для высокоскоростного event streaming и stateful stream processing.

## Компромиссы / риски

- Мульти-регион Kafka + репликация увеличивают сложность эксплуатации и стоимость.
- Outbox + CDC требует дисциплины схем/контрактов и мониторинга коннекторов.
- Exactly-once снижает throughput; для некоторых топиков можно выбрать at-least-once + идемпотентность.

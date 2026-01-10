# ADR-006: Дизайн мультитенантной платформы GoFuture (изоляция, IAM, onboarding, мониторинг)

## Контекст

GoFuture хочет быстро запускать решения для партнёров в новых регионах:

- полная изоляция данных между партнёрами (tenants),
- кастомизация функциональности,
- единая IAM система (SSO + роли),
- автоматический процесс подключения новых партнёров,
- мониторинг “по тенантам” (SLO, квоты, аномалии, биллинг/лимиты).

Платформа уже проектируется как микросервисы + Kafka/Flink. Нужен единый подход к тенантности и безопасности.

## Требования

1. Изоляция данных tenant-to-tenant (логическая и/или физическая), защита от утечек.
2. IAM: SSO (OIDC/SAML), RBAC/ABAC, сервисные аккаунты.
3. Onboarding: автоматическое создание tenant, ресурсов (DB/schema, топики, квоты), конфигураций, ключей, ролей.
4. Наблюдаемость: метрики/логи/трейсы с измерением по `tenant_id` и `region`, алерты по SLO и квотам.
5. Поддержка кастомизации: фичи, тарифы, интеграции, лимиты, брендирование.

## Решение

### 1) Модель изоляции данных (выбор)

Выбираем **гибридную модель**:

- Для “высокой изоляции” (крупные партнёры, регуляторика): **DB-per-tenant** (или schema-per-tenant) для критичных доменов (Payments, PII).
- Для “массовых” партнёров: **shared DB, shared schema + row-level isolation** через `tenant_id`, + обязательная защита на уровне приложения и DB-политик (RLS).

Почему гибрид:

- DB-per-tenant даёт сильную изоляцию и проще комплаенс, но дороже.
- Shared даёт экономию и простоту операций для мелких партнёров.
- Возможность апгрейда тенанта: shared → dedicated.

**Единый ключ изоляции:** `tenant_id` (UUID) + `region_id`.

- В событиях Kafka: `tenant_id` в headers + в payload (минимум).
- В трассировке OTel: baggage/attributes `tenant_id`, `region`.

### 2) IAM и SSO

Компоненты:

- **IdP** (Keycloak / Auth0 / Azure AD и т.п.) с поддержкой OIDC + SAML.
- **API Gateway**: валидирует JWT, извлекает `tenant_id`, `roles`, `scopes`, прокидывает в сервисы.
- **Policy Engine** (опционально): OPA (Open Policy Agent) для централизованных policy (RBAC/ABAC).
- **Service-to-service**: mTLS + service accounts (client credentials flow).

Модель доступа:

- RBAC базово (роли), ABAC дополнительно (атрибуты: tenant, region, data-classification, environment).
- “Tenant admin” управляет пользователями своего тенанта, но не влияет на других.

### 3) Кастомизация

- **Feature Flags** (например, Unleash/LaunchDarkly/self-hosted): фичи по `tenant_id`.
- Конфиг тенанта (pricing policies, интеграции, лимиты) в Tenant Config Service.
- Версионирование конфигов + аудит изменений.

### 4) Автоматизированный onboarding

Вводим **Tenant Onboarding Service** + pipeline (GitOps/Jenkins).

При создании партнёра:

1. Создаём tenant в “Tenant Registry”.
2. Создаём IAM realm/tenant group, базовые роли, SSO настройки (если нужно).
3. Создаём ресурсы данных:
   - DB/schema или отдельную БД,
   - Kafka namespaces/ACL + квоты,
   - Секреты (KMS/secret manager).
4. Создаём baseline конфиг (лимиты, фичи, региональность).
5. Включаем мониторинг: дашборды/алерты по `tenant_id`.
6. Выдаём партнёру credentials и документацию/endpoint.

Onboarding должен быть идемпотентным (повторный запуск не ломает состояние).

### 5) Мониторинг мультитенантности

Цель: видеть здоровье платформы не только “в среднем”, но и **по каждому tenant**.

**Метрики/лэйблы:**

- Бизнес-метрики: bookings/sec, assignment success, payment success, pricing latency по tenant.
- Лейблы: `tenant_id`, `region`, `service`, `endpoint`, `status_code`, `error_type`.

**Контроль квот и noisy neighbor:**

- Rate limiting на API gateway per tenant (RPS/CPU quotas).
- Kafka quotas per tenant (bytes in/out, produce rate).
- Alerting на “необычное потребление” и “аномальный error rate” у одного tenant.

**SLO per tenant:**

- p95/p99 latency и error budget на критичных API (booking, pricing, payments).
- Отдельные алерты для “важных” партнёров (tiering).

## Альтернативы

- Только DB-per-tenant: лучшая изоляция, но дорого и сложно при тысячах партнёров.
- Только shared DB: дёшево, но выше риск утечек и сложнее комплаенс.
- IAM “в каждом сервисе”: плохо управляется, лучше централизовать через gateway + policy engine.

## Компромиссы/риски

- Shared model требует строгой дисциплины (RLS, тесты на изоляцию, security review).
- Рост метрик с label `tenant_id` может взорвать cardinality → нужен sampling/aggregation и tiered monitoring.
- Onboarding автоматизация требует хорошего управления секретами и правами (least privilege).

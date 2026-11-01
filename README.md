# Wiki Stream Lakehouse

Стриминговый пайплайн, который в реальном времени обрабатывает правки Википедии со всего мира и строит по ним аналитику: какие статьи сейчас в тренде, где идут войны правок, какую долю правок делают боты.

Учебный pet-проект для практики стриминговой обработки данных, lakehouse-архитектуры и командной работы с Git.

## Архитектура

```
Wikimedia EventStreams (SSE)
        │
        ▼
Python producer ──► Kafka ──► Spark Structured Streaming
                      ▲              │
                      └── DLQ ◄──────┤
                                     ▼
                     Apache Iceberg на RustFS (S3)
                     bronze → silver → gold
                                     │
                                     ▼
                     PostgreSQL (витрины) ──► Grafana
```

## Стек

| Задача | Технология |
|---|---|
| Источник данных | Wikimedia EventStreams, поток `recentchange` |
| Брокер сообщений | Apache Kafka 4.x (KRaft) |
| Обработка | Apache Spark 4.x, Structured Streaming |
| Табличный формат | Apache Iceberg + REST-каталог |
| Объектное хранилище | RustFS (S3-совместимое) |
| Витрины | PostgreSQL |
| Дашборды | Grafana |
| Оркестрация | Docker Compose, далее Kubernetes (k3d, Helm) |
| Качество кода | pytest, chispa, ruff, pre-commit, GitHub Actions |

## Слои данных

- **Bronze** — сырые события из Kafka без изменений.
- **Silver** — типизированные, дедуплицированные события; битые записи уходят в DLQ.
- **Gold** — оконные агрегаты: правки в минуту по языкам, трендовые статьи, доля ботов, войны правок.

## Статус

- [x] Этап 0 — окружение и репозиторий
- [ ] Этап 1 — producer
- [ ] Этап 2 — bronze-слой
- [ ] Этап 3 — silver-слой
- [ ] Этап 4 — gold-слой
- [ ] Этап 5 — витрины и Grafana
- [ ] Этап 6 — переезд в Kubernetes

## Запуск

Инструкция появится по завершении этапа 0.

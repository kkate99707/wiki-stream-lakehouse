"""Producer: читает поток правок Википедии (SSE) и пишет события в Kafka."""

import json
import logging
import os
import signal
import time

import httpx
from confluent_kafka import KafkaError, Message, Producer

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.recentchange")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
# Wikimedia требует осмысленный User-Agent с контактом, иначе может отвечать 403
USER_AGENT = os.getenv(
    "WIKI_USER_AGENT",
    "wiki-stream-lakehouse/0.1 (https://github.com/kkate99707/wiki-stream-lakehouse)",
)

log = logging.getLogger("producer")


# ---------------------------------------------------------------------------
# Чистые функции: без сети и Kafka, поэтому легко тестируются
# ---------------------------------------------------------------------------


def parse_event(payload: str) -> dict | None:
    """Разбирает JSON из строки `data:`. Возвращает None, если это не JSON-объект."""
    try:
        res = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return res if isinstance(res, dict) else None


def is_canary(event: dict) -> bool:
    """Canary-события Wikimedia шлёт для мониторинга, это не настоящие правки."""
    meta = event.get("meta", {})
    domain = meta.get("domain", {})
    return domain == "canary"


def message_key(event: dict) -> bytes | None:
    """Ключ сообщения - код вики (ruwiki, enwiki...): правки одной вики идут в одну партицию."""
    key = event.get("wiki")
    return key.encode("utf-8") if key else None


def build_producer_config(bootstrap_servers: str) -> dict:
    """Настройки producer'а: надёжная доставка без дублей + батчи со сжатием."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "wiki-producer",
        "acks": "all",  # все реплики должны подтвердить запись
        "enable.idempotence": True,  # отбрасывать дубли
        "linger.ms": 100,  # сколько мс ждать
        "compression.type": "zstd",  # сжатие
    }


# ---------------------------------------------------------------------------
# Работа с сетью и Kafka
# ---------------------------------------------------------------------------


class Stats:
    def __init__(self) -> None:
        self.sent = 0
        self.failed = 0
        self.skipped = 0


def on_delivery(stats: Stats):
    """Колбэк вызывается, когда Kafka подтвердила запись (или вернула ошибку)."""

    def callback(err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            stats.failed += 1
            log.error("delivery failed: %s", err)
        else:
            stats.sent += 1

    return callback


def stream_lines(last_event_id: str | None):
    """Открывает SSE-соединение и отдаёт строки ответа по одной."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        # Продолжить с места обрыва, а не с "сейчас"
        headers["Last-Event-ID"] = last_event_id
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
    with httpx.stream("GET", STREAM_URL, headers=headers, timeout=timeout) as response:
        response.raise_for_status()
        yield from response.iter_lines()


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = Producer(build_producer_config(BOOTSTRAP_SERVERS))
    stats = Stats()
    callback = on_delivery(stats)

    stopping = False

    def stop(*_args) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    last_event_id: str | None = None
    last_report = time.monotonic()

    log.info("streaming %s -> %s/%s", STREAM_URL, BOOTSTRAP_SERVERS, TOPIC)
    while not stopping:
        try:
            for line in stream_lines(last_event_id):
                if stopping:
                    break
                if line.startswith("id:"):
                    last_event_id = line[3:].strip()
                    continue
                if not line.startswith("data:"):
                    continue  # пустые строки, `event:`, комментарии-heartbeat

                event = parse_event(line[5:].strip())
                if event is None or is_canary(event):
                    stats.skipped += 1
                    continue

                # TODO: отправить событие в Kafka через producer.produce(...):
                #   - топик: TOPIC
                #   - key: ключ из message_key(event)
                #   - value: событие обратно в JSON и в bytes:
                #       json.dumps(event, ensure_ascii=False).encode("utf-8")
                #   - on_delivery: callback (чтобы считать доставленные и ошибки)
                producer.produce(
                    TOPIC,
                    key=message_key(event),
                    value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                    on_delivery=callback,
                )
                producer.poll(0)  # обработать колбэки уже доставленных сообщений

                if time.monotonic() - last_report > 10:
                    log.info(
                        "sent=%d failed=%d skipped=%d in_flight=%d",
                        stats.sent,
                        stats.failed,
                        stats.skipped,
                        len(producer),
                    )
                    last_report = time.monotonic()
        except (httpx.HTTPError, BufferError) as exc:
            if stopping:
                break
            log.warning("stream interrupted (%s), reconnecting in 5s", exc)
            producer.poll(1)
            time.sleep(5)

    log.info("stopping, flushing %d messages...", len(producer))
    remaining = producer.flush(10)
    log.info("done: sent=%d failed=%d not_flushed=%d", stats.sent, stats.failed, remaining)


if __name__ == "__main__":
    run()

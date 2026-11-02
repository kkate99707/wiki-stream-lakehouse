import json

from producer.main import build_producer_config, is_canary, message_key, parse_event

EDIT = {
    "id": 123,
    "type": "edit",
    "wiki": "ruwiki",
    "title": "Минск",
    "bot": False,
    "meta": {"domain": "ru.wikipedia.org"},
}


# --- parse_event -----------------------------------------------------------


def test_parse_event_returns_dict_for_valid_json():
    assert parse_event(json.dumps(EDIT)) == EDIT


def test_parse_event_returns_none_for_broken_json():
    assert parse_event('{"wiki": "ruwiki",') is None


def test_parse_event_returns_none_for_json_that_is_not_an_object():
    assert parse_event("[1, 2, 3]") is None


# --- is_canary -------------------------------------------------------------


def test_is_canary_detects_canary_event():
    assert is_canary({"meta": {"domain": "canary"}})


def test_is_canary_false_for_real_edit():
    assert not is_canary(EDIT)


def test_is_canary_false_when_meta_is_missing():
    assert not is_canary({"wiki": "ruwiki"})


# --- message_key -----------------------------------------------------------


def test_message_key_is_wiki_code_in_bytes():
    assert message_key(EDIT) == b"ruwiki"


def test_message_key_none_when_wiki_is_missing():
    assert message_key({"title": "no wiki"}) is None


# --- build_producer_config -------------------------------------------------


def test_config_uses_given_bootstrap_servers():
    assert build_producer_config("broker:9092")["bootstrap.servers"] == "broker:9092"


def test_config_waits_for_all_replicas():
    assert build_producer_config("x")["acks"] == "all"


def test_config_is_idempotent():
    assert build_producer_config("x")["enable.idempotence"] is True


def test_config_batches_and_compresses():
    config = build_producer_config("x")
    assert config["linger.ms"] > 0
    assert config["compression.type"] in {"lz4", "zstd", "snappy", "gzip"}

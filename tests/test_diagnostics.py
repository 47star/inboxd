import json
import logging

from inboxd.runtime.diagnostics import JSONFormatter


def test_jsonl_is_single_line_and_redacts_secrets():
    record = logging.LogRecord(
        "inboxd.test",
        logging.ERROR,
        "test",
        1,
        "Failure\nAuthorization: Bearer xoxp-fixture-secret",
        (),
        None,
    )
    record.event = "collector_state"
    record.details = {"source": "slack", "token": "private-value", "body": "private-content"}
    line = JSONFormatter("fixture-run").format(record)
    value = json.loads(line)
    assert "\n" not in line
    assert "fixture-secret" not in line
    assert "private-value" not in line
    assert "private-content" not in line
    assert value["source"] == "slack"
    assert value["run_id"] == "fixture-run"
    assert value["event"] == "collector_state"

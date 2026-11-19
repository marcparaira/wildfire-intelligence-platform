from unittest.mock import Mock

import pytest
from kafka.structs import OffsetAndMetadata, TopicPartition

from wildfire_firms import processor


def test_commits_only_the_persisted_record(monkeypatch: pytest.MonkeyPatch) -> None:
    persist = Mock(return_value=True)
    monkeypatch.setattr(processor, "persist", persist)
    connection, consumer = Mock(), Mock()
    record = Mock(topic="detections", partition=2, offset=10, value=b"event")

    assert processor.process_record(connection, consumer, record) is True
    persist.assert_called_once_with(connection, b"event")
    consumer.commit.assert_called_once_with(
        {TopicPartition("detections", 2): OffsetAndMetadata(11, "", -1)}
    )


def test_failed_persistence_never_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processor, "persist", Mock(side_effect=ValueError("invalid event")))
    consumer = Mock()
    with pytest.raises(ValueError, match="invalid event"):
        processor.process_record(Mock(), consumer, Mock())
    consumer.commit.assert_not_called()

"""Create and delete the topics for a pattern (contract section 1)."""

from __future__ import annotations

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

from rtmpack_harness.contract import (
    TOPIC_REPLICATION_FACTOR,
    TOPIC_RETENTION_MS,
    TOPIC_TIMESTAMP_TYPE,
    topic_names,
)


def topic_config() -> dict[str, str]:
    return {
        "message.timestamp.type": TOPIC_TIMESTAMP_TYPE,
        "retention.ms": str(TOPIC_RETENTION_MS),
    }


def create_topics(
    admin: AdminClient,
    pattern: str,
    partitions: int,
    timeout_s: float = 60.0,
) -> dict[str, str]:
    """Create the input and output topics. Returns {topic: "created" | "exists"}."""
    names = topic_names(pattern)
    new_topics = [
        NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=TOPIC_REPLICATION_FACTOR,
            config=topic_config(),
        )
        for name in names.values()
    ]
    futures = admin.create_topics(new_topics, operation_timeout=timeout_s, request_timeout=timeout_s)
    result: dict[str, str] = {}
    for name, future in futures.items():
        try:
            future.result(timeout=timeout_s)
            result[name] = "created"
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                result[name] = "exists"
            else:
                raise
    return result


def delete_topics(admin: AdminClient, pattern: str, timeout_s: float = 60.0) -> dict[str, str]:
    """Delete the input and output topics. Returns {topic: "deleted" | "absent"}."""
    names = list(topic_names(pattern).values())
    futures = admin.delete_topics(names, operation_timeout=timeout_s, request_timeout=timeout_s)
    result: dict[str, str] = {}
    for name, future in futures.items():
        try:
            future.result(timeout=timeout_s)
            result[name] = "deleted"
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                result[name] = "absent"
            else:
                raise
    return result


def set_retention(
    admin: AdminClient, pattern: str, retention_ms: int, timeout_s: float = 60.0
) -> dict[str, str]:
    """Set retention.ms on every topic of a pattern (incremental, other configs unchanged).

    Kafka then deletes closed segments older than the new retention, which frees broker
    storage within minutes. Returns {topic: "set" | "absent"}.
    """
    resources = [
        ConfigResource(
            ResourceType.TOPIC,
            name,
            incremental_configs=[
                ConfigEntry("retention.ms", str(retention_ms), incremental_operation=AlterConfigOpType.SET)
            ],
        )
        for name in topic_names(pattern).values()
    ]
    futures = admin.incremental_alter_configs(resources, request_timeout=timeout_s)
    result: dict[str, str] = {}
    for resource, future in futures.items():
        try:
            future.result(timeout=timeout_s)
            result[resource.name] = "set"
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                result[resource.name] = "absent"
            else:
                raise
    return result

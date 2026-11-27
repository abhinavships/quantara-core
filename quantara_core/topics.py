"""Single source of truth for every Kafka topic in the platform (the data contract between repos).
name -> (partitions, retention_ms). Keyed by vehicle_id where per-vehicle ordering matters."""
from . import config

H, D = 3_600_000, 86_400_000
TOPICS = {
    # ingest (input connectors)
    "raw.telemetry": (6, 6 * H), "raw.edge": (3, D), "raw.feeds": (1, D),
    # stream (validate -> enrich -> route)
    "valid.telemetry": (6, 6 * H), "valid.edge": (3, D), "valid.feeds": (1, D), "dlq": (1, D), "quarantine": (1, D),
    "vss.enriched": (6, 6 * H),
    "cloud.escalations": (3, D), "cloud.security": (3, D), "cloud.fleet": (1, D),
    # services (cloud brain)
    "cloud.rca": (3, D), "cloud.twin": (3, 6 * H), "cloud.route": (3, D),
    "cloud.ota": (3, D), "commands.out": (3, D), "notify.out": (3, D),
    # ops
    "control.inject": (1, H), "audit": (1, 7 * D),
}


def ensure_topics(admin=None, names=None):
    """Idempotently create topics (prefix-checked). Returns {name: 'created'|'exists'|error}.
    `admin` is injectable for tests; default is a Kafka AdminClient from env."""
    if admin is None:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient(config.kafka_conf())
    from confluent_kafka.admin import NewTopic
    want = {config.topic(n): TOPICS[n] for n in (names or TOPICS)}
    futs = admin.create_topics([NewTopic(t, num_partitions=p, replication_factor=2,
                                         config={"retention.ms": str(r)}) for t, (p, r) in want.items()])
    out = {}
    for t, f in futs.items():
        try:
            f.result(timeout=20); out[t] = "created"
        except Exception as e:                      # TOPIC_ALREADY_EXISTS is the normal re-run case
            out[t] = "exists" if "ALREADY_EXISTS" in str(e).upper() else f"error: {e}"
    return out

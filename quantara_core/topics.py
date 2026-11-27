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


# Cluster limit: at most 10 topics in total. Apps keep using the logical names above; the bus maps them onto this
# small set of physical topics, tags each message with its logical name and filters on read (see bus.py).
PHYSICAL = {
    "raw.telemetry": (6, 6 * H), "raw.edge": (3, D), "valid.telemetry": (6, 6 * H), "valid.edge": (3, D),
    "vss.enriched": (6, 6 * H), "cloud.events": (3, D), "edge.in": (3, D),
}
LOGICAL = {
    "raw.telemetry": "raw.telemetry", "raw.edge": "raw.edge", "raw.feeds": "raw.edge",
    "valid.telemetry": "valid.telemetry", "valid.edge": "valid.edge", "valid.feeds": "valid.edge",
    "vss.enriched": "vss.enriched",
    "dlq": "cloud.events", "quarantine": "cloud.events", "cloud.escalations": "cloud.events", "cloud.security": "cloud.events",
    "cloud.fleet": "cloud.events", "cloud.rca": "cloud.events", "cloud.twin": "cloud.events", "cloud.route": "cloud.events",
    "cloud.ota": "cloud.events", "notify.out": "cloud.events", "audit": "cloud.events",
    "commands.out": "edge.in", "control.inject": "edge.in",
}
assert set(LOGICAL) == set(TOPICS) and set(LOGICAL.values()) == set(PHYSICAL) and len(PHYSICAL) <= 10


def physical(short):
    return LOGICAL.get(short, short)


def ensure_topics(admin=None, names=None):
    """Idempotently create topics (prefix-checked). Returns {name: 'created'|'exists'|error}.
    `admin` is injectable for tests; default is a Kafka AdminClient from env."""
    if admin is None:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient(config.kafka_conf())
    from confluent_kafka.admin import NewTopic
    want = {config.topic(p): PHYSICAL[p] for p in sorted({physical(n) for n in (names or TOPICS)})}
    futs = admin.create_topics([NewTopic(t, num_partitions=p, replication_factor=2,
                                         config={"retention.ms": str(r)}) for t, (p, r) in want.items()])
    out = {}
    for t, f in futs.items():
        try:
            f.result(timeout=20); out[t] = "created"
        except Exception as e:                      # TOPIC_ALREADY_EXISTS is the normal re-run case
            out[t] = "exists" if "ALREADY_EXISTS" in str(e).upper() else f"error: {e}"
    return out

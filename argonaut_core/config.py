import os


def env(name, default=None, required=False):
    v = os.environ.get(name, default)
    if required and v is None:
        raise RuntimeError(f"missing env {name}")
    return v


def prefix() -> str:
    p = env("TOPIC_PREFIX", "argonaut")
    if "/" in p or not p:
        raise ValueError("TOPIC_PREFIX must be non-empty and contain no '/'")
    return p


def topic(name: str) -> str:
    """<prefix>.<name>; names may not contain '/'."""
    if "/" in name:
        raise ValueError("no '/' in topic names")
    return f"{prefix()}.{name}"


def group(name: str) -> str:
    return topic(f"cg-{name}")


def kafka_conf() -> dict:
    """librdkafka config from env. Never logs credentials."""
    c = {"bootstrap.servers": env("KAFKA_BOOTSTRAP", required=True),
         "security.protocol": env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
         "reconnect.backoff.ms": 500, "reconnect.backoff.max.ms": 10000}
    if c["security.protocol"].startswith("SASL"):
        c.update({"sasl.mechanism": env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
                  "sasl.username": env("KAFKA_SASL_USERNAME", required=True),
                  "sasl.password": env("KAFKA_SASL_PASSWORD", required=True)})
    return c

import time
import pytest
from quantara_core import config, crypto, bus, schemas


def test_topic_prefix(monkeypatch):
    monkeypatch.setenv("TOPIC_PREFIX", "team-x")
    assert config.topic("raw.telemetry") == "team-x.raw.telemetry"
    assert config.group("stream").startswith("team-x.")
    with pytest.raises(ValueError):
        config.topic("a/b")
    monkeypatch.setenv("TOPIC_PREFIX", "a/b")
    with pytest.raises(ValueError):
        config.prefix()


def test_sign_verify_tamper():
    priv, pub = crypto.gen_keypair()
    c = crypto.make_command(priv, "V1", "derate", {"limit": 0.6}, 1)
    assert crypto.verify(c, pub)
    c2 = {**c, "params": {"limit": 1.0}}
    assert not crypto.verify(c2, pub)
    assert not crypto.verify(c, crypto.gen_keypair()[1])
    assert not crypto.verify({**c, "sig": "!!"}, pub)


def test_membus_groups_independent():
    b = bus.MemBus()
    b.publish("t", {"a": 1}, key="k")
    assert len(b.poll("g1", ["t"])) == 1 and b.poll("g1", ["t"]) == []
    assert len(b.poll("g2", ["t"])) == 1


def test_schema():
    ok = {"vehicle_id": "V1", "ts": 1.0, "sim": True, "signals": {"Battery.Temp": 30.0}}
    assert schemas.validate_telemetry(ok) == []
    assert schemas.validate_telemetry({**ok, "signals": {"x": float("nan")}})
    assert schemas.validate_telemetry({"vehicle_id": "V1"})


def test_prefix_with_trailing_separator(monkeypatch):
    monkeypatch.setenv("TOPIC_PREFIX", "quantara_")
    assert config.topic("raw.telemetry") == "quantara_raw.telemetry"
    assert config.group("ingest") == "quantara_cg-ingest"


def test_topics_registry_and_ensure(monkeypatch):
    from quantara_core import topics
    monkeypatch.setenv("TOPIC_PREFIX", "quantara_")
    assert all("/" not in n for n in topics.TOPICS)

    class F:
        def __init__(self, err=None): self.err = err
        def result(self, timeout=None):
            if self.err: raise Exception(self.err)

    class Admin:
        def create_topics(self, nts):
            return {nt.topic: F("TOPIC_ALREADY_EXISTS" if nt.topic.endswith("telemetry") else None) for nt in nts}
    out = topics.ensure_topics(Admin(), ["raw.telemetry", "raw.edge"])
    assert out == {"quantara_raw.telemetry": "exists", "quantara_raw.edge": "created"}


def test_topic_budget_and_mapping():
    from quantara_core import topics as T
    assert len(T.PHYSICAL) <= 10 and len(T.PHYSICAL) == 7
    assert set(T.LOGICAL) == set(T.TOPICS)                       # every logical topic is routed
    assert all(T.physical(n) in T.PHYSICAL for n in T.TOPICS)


def test_route_out_in_roundtrip_filters_by_logical_topic(monkeypatch):
    from quantara_core import config
    from quantara_core.bus import route_in, route_out
    monkeypatch.setenv("TOPIC_PREFIX", "quantara_")
    ptopic, wire = route_out("quantara_cloud.rca", {"a": 1})
    assert ptopic == "quantara_cloud.events" and wire == {"a": 1, "_lt": "cloud.rca"}
    want = {"quantara_cloud.rca", "quantara_cloud.twin"}
    assert route_in(ptopic, wire, want) == ("quantara_cloud.rca", {"a": 1})        # tag removed, logical name restored
    assert route_in(ptopic, {"a": 1, "_lt": "audit"}, want) is None                 # same physical topic, not wanted
    assert route_in(ptopic, {"x": 1}, want)[0] in want                              # untagged -> first wanted sharing the topic
    assert route_in("quantara_raw.edge", {"x": 1}, want) is None
    assert route_in(ptopic, "junk", want) is None


def test_ensure_topics_creates_only_physical():
    from quantara_core import topics as T
    made = []

    class F:
        def result(self, timeout=0): return None

    class A:
        def create_topics(self, nts):
            made.extend(n.topic for n in nts); return {n.topic: F() for n in nts}
    out = T.ensure_topics(A())
    assert sorted(made) == sorted("quantara_" + p for p in T.PHYSICAL) and len(out) == 7

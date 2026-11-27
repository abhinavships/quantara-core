import time
import pytest
from argonaut_core import config, crypto, bus, schemas


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

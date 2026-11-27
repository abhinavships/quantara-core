"""Bus abstraction: KafkaBus (real, retrying) and MemBus (tests / no-broker dev).
Messages are JSON dicts; key is the vehicle id so a vehicle stays on one partition."""
import json
import logging
import threading
import time
from collections import defaultdict

from . import config, topics as _topics


def route_out(topic, msg):
    """logical full topic name + message -> (physical full topic, message tagged with its logical name)."""
    short = topic[len(config.topic("")):]
    return config.topic(_topics.physical(short)), ({**msg, "_lt": short} if isinstance(msg, dict) else msg)


def route_in(physical_topic, msg, wanted):
    """Returns (logical full topic, message) if the consumer asked for this logical topic, else None. `wanted` = logical full names."""
    if not isinstance(msg, dict): return None
    lt = msg.get("_lt")
    if lt is None:                                   # untagged: legacy or undecodable; deliver to the first wanted logical sharing the topic
        for w in sorted(wanted):
            if config.topic(_topics.physical(w[len(config.topic("")):])) == physical_topic: return w, msg
        return None
    name = config.topic(lt)
    return (name, {k: v for k, v in msg.items() if k != "_lt"}) if name in wanted else None


class MemBus:
    def __init__(self):
        self.log = defaultdict(list)          # topic -> [(key, dict)]
        self.offsets = defaultdict(int)       # (group, topic) -> next index
        self.lock = threading.Lock()

    def publish(self, topic, msg, key=None):
        with self.lock:
            self.log[topic].append((key, json.loads(json.dumps(msg))))

    def poll(self, group, topics, max_n=100, timeout=0.0):
        out = []
        with self.lock:
            for t in topics:
                i = self.offsets[(group, t)]
                chunk = self.log[t][i:i + max_n - len(out)]
                self.offsets[(group, t)] = i + len(chunk)
                out += [(t, k, m) for k, m in chunk]
        return out

    def flush(self): pass
    def close(self): pass


class KafkaBus:
    def __init__(self, retries=5):
        from confluent_kafka import Consumer, Producer
        self._Consumer, self._retries = Consumer, retries
        self.p = Producer({**config.kafka_conf(), "enable.idempotence": True,
                           "retries": 10, "linger.ms": 20})
        self.consumers, self.ok, self.failed = {}, 0, 0

    def _dr(self, err, msg):
        if err:
            self.failed += 1
            if self.failed <= 5 or self.failed % 100 == 0:
                logging.getLogger('bus').error('delivery failed topic=%s: %s (total failed=%d)', msg.topic(), err, self.failed)
        else:
            self.ok += 1

    def publish(self, topic, msg, key=None):
        topic, msg = route_out(topic, msg)
        data = json.dumps(msg, separators=(",", ":")).encode()
        for i in range(self._retries):
            try:
                self.p.produce(topic, data, key=key and key.encode(), on_delivery=self._dr)
                self.p.poll(0)
                return
            except BufferError:            # local queue full: drain and retry
                self.p.poll(0.5)
            except Exception:
                time.sleep(min(2 ** i * 0.2, 5))
        raise RuntimeError(f"publish to {topic} failed")

    def poll(self, group, topics, max_n=100, timeout=0.5):
        wanted = set(topics)
        phys = sorted({config.topic(_topics.physical(t[len(config.topic("")):])) for t in topics})
        key = (group, tuple(phys))
        if key not in self.consumers:
            c = self._Consumer({**config.kafka_conf(), "group.id": group,
                                "auto.offset.reset": "latest", "enable.auto.commit": True})
            c.subscribe(phys)
            self.consumers[key] = c
        out = []
        for m in self.consumers[key].consume(max_n, timeout):
            if m.error():
                continue                     # transient errors are retried by librdkafka
            try:
                v = json.loads(m.value())
            except ValueError:
                v = {"_undecodable": True}
            r = route_in(m.topic(), v, wanted)
            if r: out.append((r[0], m.key() and m.key().decode(), r[1]))
        return out

    def flush(self): self.p.flush(10)

    def close(self):
        self.flush()
        for c in self.consumers.values():
            c.close()


def get_bus():
    return KafkaBus() if config.env("KAFKA_BOOTSTRAP") else MemBus()

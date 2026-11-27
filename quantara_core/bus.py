"""Bus abstraction: KafkaBus (real, retrying) and MemBus (tests / no-broker dev).
Messages are JSON dicts; key is the vehicle id so a vehicle stays on one partition."""
import json
import queue
import threading
import time
from collections import defaultdict

from . import config


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
        self.consumers = {}

    def publish(self, topic, msg, key=None):
        data = json.dumps(msg, separators=(",", ":")).encode()
        for i in range(self._retries):
            try:
                self.p.produce(topic, data, key=key and key.encode())
                self.p.poll(0)
                return
            except BufferError:            # local queue full: drain and retry
                self.p.poll(0.5)
            except Exception:
                time.sleep(min(2 ** i * 0.2, 5))
        raise RuntimeError(f"publish to {topic} failed")

    def poll(self, group, topics, max_n=100, timeout=0.5):
        key = (group, tuple(sorted(topics)))
        if key not in self.consumers:
            c = self._Consumer({**config.kafka_conf(), "group.id": group,
                                "auto.offset.reset": "latest", "enable.auto.commit": True})
            c.subscribe(list(topics))
            self.consumers[key] = c
        out = []
        for m in self.consumers[key].consume(max_n, timeout):
            if m.error():
                continue                     # transient errors are retried by librdkafka
            try:
                out.append((m.topic(), m.key() and m.key().decode(), json.loads(m.value())))
            except ValueError:
                out.append((m.topic(), None, {"_undecodable": True}))
        return out

    def flush(self): self.p.flush(10)

    def close(self):
        self.flush()
        for c in self.consumers.values():
            c.close()


def get_bus():
    return KafkaBus() if config.env("KAFKA_BOOTSTRAP") else MemBus()

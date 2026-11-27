"""Ed25519 signed commands. Canonical JSON over every field except `sig`."""
import base64
import json
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey


def canon(d: dict) -> bytes:
    return json.dumps({k: v for k, v in d.items() if k != "sig"},
                      sort_keys=True, separators=(",", ":")).encode()


def gen_keypair():
    sk = Ed25519PrivateKey.generate()
    priv = sk.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
                            serialization.NoEncryption())
    pub = sk.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return base64.b64encode(priv).decode(), base64.b64encode(pub).decode()


def sign(cmd: dict, priv_b64: str) -> dict:
    sk = Ed25519PrivateKey.from_private_bytes(base64.b64decode(priv_b64))
    return {**cmd, "sig": base64.b64encode(sk.sign(canon(cmd))).decode()}


def verify(cmd: dict, pub_b64: str) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(base64.b64decode(pub_b64)).verify(
            base64.b64decode(cmd.get("sig", "")), canon(cmd))
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False


def make_command(priv_b64, vehicle_id, kind, params, counter, ttl_s=300,
                 consent=None, oem_approval=None, now=None):
    now = time.time() if now is None else now
    return sign({"vehicle_id": vehicle_id, "kind": kind, "params": params,
                 "counter": counter, "expires": now + ttl_s,
                 "consent": consent, "oem_approval": oem_approval}, priv_b64)

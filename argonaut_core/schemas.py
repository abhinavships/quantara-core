"""Minimal message validators (no jsonschema dependency). Return list of problems."""

TELEMETRY = {"vehicle_id": str, "ts": (int, float), "sim": bool, "signals": dict}
SIGNALS = ("Vehicle.Speed", "Battery.Temp", "Coolant.Flow", "Motor.Current",
           "Battery.SoC", "Battery.kmPerKwh", "Pump.Duty", "Power.kW")
KINDS = ("telemetry", "summary", "escalation", "security", "link")


def validate(msg: dict, spec=TELEMETRY) -> list:
    errs = []
    for k, t in spec.items():
        if k not in msg:
            errs.append(f"missing {k}")
        elif not isinstance(msg[k], t) or (t is not bool and isinstance(msg[k], bool) and t != bool):
            errs.append(f"bad type {k}")
    return errs


def validate_telemetry(msg: dict) -> list:
    errs = validate(msg)
    if not errs:
        for s, v in msg["signals"].items():
            if not isinstance(v, (int, float)) or v != v:
                errs.append(f"bad signal {s}")
    return errs

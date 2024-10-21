from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple


def config_to_kwargs(config: Dict[str, str], mode: str) -> Dict[str, Any]:
    consumer_kwargs = {}
    config_settings: List[Tuple[str, str, Callable[[str], Any]]] = [
        ('bootstrap.servers', 'bootstrap_servers', lambda s: s.split(',')),
        ('request.timeout.ms', 'request_timeout_ms', int),
        ('max.in.flight.requests.per.connection', 'max_in_flight_requests_per_connection', int),
    ]

    if mode == 'producer':
        config_settings.extend([
            ('request.required.acks', 'acks', _parse_acks),
        ])
    elif mode != 'consumer':
        raise ValueError(f"Invalid mode: '{mode}'")

    for (config_key, arg, converter) in config_settings:
        try:
            consumer_kwargs[arg] = converter(config[config_key])
        except KeyError:
            pass

    return consumer_kwargs


def _parse_acks(value: str) -> Any:
    if value == "-1":
        return "all"
    return int(value)

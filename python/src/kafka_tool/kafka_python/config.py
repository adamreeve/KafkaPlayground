from typing import Any, Dict


def config_to_kwargs(config: Dict[str, str]) -> Dict[str, Any]:
    consumer_kwargs = {}
    for (config_key, arg, converter) in [
        ('bootstrap.servers', 'bootstrap_servers', lambda s: s.split(',')),
        ('max.in.flight.requests.per.connection', 'max_in_flight_requests_per_connection', int),
        ('request.timeout.ms', 'request_timeout_ms', int),
    ]:
        try:
            consumer_kwargs[arg] = converter(config[config_key])
        except KeyError:
            pass

    return consumer_kwargs

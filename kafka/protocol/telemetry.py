from .api_message import ApiMessage


class GetTelemetrySubscriptionsRequest(ApiMessage): pass
class GetTelemetrySubscriptionsResponse(ApiMessage): pass

class PushTelemetryRequest(ApiMessage): pass
class PushTelemetryResponse(ApiMessage): pass


__all__ = [
    'GetTelemetrySubscriptionsRequest', 'GetTelemetrySubscriptionsResponse',
    'PushTelemetryRequest', 'PushTelemetryResponse',
]

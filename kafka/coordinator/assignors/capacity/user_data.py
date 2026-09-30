from kafka.protocol.api_data import ApiData


class CapacityAssignorUserData(ApiData, load_json=__package__):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('version', 1)
        super().__init__(*args, **kwargs)

"""Minimal OpenTelemetry Protocol Buffers serialization for KIP-714.

Implements just enough of opentelemetry.proto.metrics.v1.MetricsData (and the
common/resource types it references) to encode client telemetry metrics and
to decode them again for verification in tests. This avoids any dependency on
the protobuf or opentelemetry packages.

Message layout (field numbers) used here:

    MetricsData       { repeated ResourceMetrics resource_metrics = 1; }
    ResourceMetrics   { Resource resource = 1; repeated ScopeMetrics scope_metrics = 2; }
    Resource          { repeated KeyValue attributes = 1; }
    ScopeMetrics      { InstrumentationScope scope = 1; repeated Metric metrics = 2; }
    InstrumentationScope { string name = 1; string version = 2; }
    Metric            { string name = 1; string description = 2; string unit = 3;
                        Gauge gauge = 5; Sum sum = 7; }
    Gauge             { repeated NumberDataPoint data_points = 1; }
    Sum               { repeated NumberDataPoint data_points = 1;
                        AggregationTemporality aggregation_temporality = 2;
                        bool is_monotonic = 3; }
    NumberDataPoint   { repeated KeyValue attributes = 2;
                        fixed64 start_time_unix_nano = 3;
                        fixed64 time_unix_nano = 4;
                        double as_double = 6; sfixed64 as_int = 7; }
    KeyValue          { string key = 1; AnyValue value = 2; }
    AnyValue          { string string_value = 1; }
"""

import struct


# AggregationTemporality enum values (opentelemetry.proto.metrics.v1)
AGGREGATION_TEMPORALITY_UNSPECIFIED = 0
AGGREGATION_TEMPORALITY_DELTA = 1
AGGREGATION_TEMPORALITY_CUMULATIVE = 2

_WIRE_VARINT = 0
_WIRE_FIXED64 = 1
_WIRE_LENGTH_DELIMITED = 2
_WIRE_FIXED32 = 5


def _encode_varint(value):
    out = bytearray()
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            return bytes(out)


def _tag(field_number, wire_type):
    return _encode_varint((field_number << 3) | wire_type)


def _varint_field(field_number, value):
    return _tag(field_number, _WIRE_VARINT) + _encode_varint(value)


def _fixed64_field(field_number, value):
    return _tag(field_number, _WIRE_FIXED64) + struct.pack('<Q', value)


def _sfixed64_field(field_number, value):
    return _tag(field_number, _WIRE_FIXED64) + struct.pack('<q', value)


def _double_field(field_number, value):
    return _tag(field_number, _WIRE_FIXED64) + struct.pack('<d', value)


def _bytes_field(field_number, payload):
    return _tag(field_number, _WIRE_LENGTH_DELIMITED) + _encode_varint(len(payload)) + payload


def _string_field(field_number, value):
    return _bytes_field(field_number, value.encode('utf-8'))


def _key_value(key, value):
    any_value = _string_field(1, str(value))
    return _string_field(1, key) + _bytes_field(2, any_value)


def _number_data_point(point):
    out = bytearray()
    for key, value in (point.get('attributes') or {}).items():
        out += _bytes_field(2, _key_value(key, value))
    if point.get('start_time_unix_nano') is not None:
        out += _fixed64_field(3, point['start_time_unix_nano'])
    out += _fixed64_field(4, point['time_unix_nano'])
    if point.get('as_int') is not None:
        out += _sfixed64_field(7, point['as_int'])
    elif point.get('as_double') is not None:
        out += _double_field(6, point['as_double'])
    return bytes(out)


def _metric(metric):
    out = bytearray()
    out += _string_field(1, metric['name'])
    if metric.get('description'):
        out += _string_field(2, metric['description'])
    if metric.get('unit'):
        out += _string_field(3, metric['unit'])
    if 'gauge' in metric:
        gauge_payload = bytearray()
        for point in metric['gauge']['data_points']:
            gauge_payload += _bytes_field(1, _number_data_point(point))
        out += _bytes_field(5, bytes(gauge_payload))
    elif 'sum' in metric:
        sum_data = metric['sum']
        sum_payload = bytearray()
        for point in sum_data['data_points']:
            sum_payload += _bytes_field(1, _number_data_point(point))
        sum_payload += _varint_field(2, sum_data['aggregation_temporality'])
        sum_payload += _varint_field(3, 1 if sum_data['is_monotonic'] else 0)
        out += _bytes_field(7, bytes(sum_payload))
    return bytes(out)


def encode_metrics_data(metrics, resource_attributes=None, scope_name='', scope_version=''):
    """Encode a list of metric dicts as an OTLP MetricsData protobuf payload.

    Arguments:
        metrics (list[dict]): each dict has 'name', optional 'description' and
            'unit', and either a 'gauge' ({'data_points': [...]}) or a 'sum'
            ({'data_points': [...], 'aggregation_temporality': int,
            'is_monotonic': bool}) entry. Data points are dicts with optional
            'attributes' (dict[str, str]), optional 'start_time_unix_nano',
            required 'time_unix_nano', and either 'as_int' or 'as_double'.
        resource_attributes (dict[str, str], optional): Resource attributes.
        scope_name (str): InstrumentationScope name.
        scope_version (str): InstrumentationScope version.

    Returns:
        bytes: the serialized MetricsData protobuf message.
    """
    resource = bytearray()
    for key, value in (resource_attributes or {}).items():
        resource += _bytes_field(1, _key_value(key, value))
    scope = _string_field(1, scope_name)
    if scope_version:
        scope += _string_field(2, scope_version)
    scope_metrics = _bytes_field(1, scope)
    for metric in metrics:
        scope_metrics += _bytes_field(2, _metric(metric))
    resource_metrics = _bytes_field(1, bytes(resource)) + _bytes_field(2, scope_metrics)
    return _bytes_field(1, resource_metrics)


def _read_varint(data, pos):
    shift = 0
    result = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7


def _read_fields(data):
    pos = 0
    fields = []
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_number = tag >> 3
        wire_type = tag & 0x7
        if wire_type == _WIRE_VARINT:
            value, pos = _read_varint(data, pos)
        elif wire_type == _WIRE_FIXED64:
            value = data[pos:pos + 8]
            pos += 8
        elif wire_type == _WIRE_LENGTH_DELIMITED:
            length, pos = _read_varint(data, pos)
            value = data[pos:pos + length]
            pos += length
        elif wire_type == _WIRE_FIXED32:
            value = data[pos:pos + 4]
            pos += 4
        else:
            raise ValueError('Unsupported protobuf wire type: %d' % (wire_type,))
        fields.append((field_number, wire_type, value))
    return fields


def _decode_key_value(data):
    key = value = None
    for field_number, _wire_type, val in _read_fields(data):
        if field_number == 1:
            key = val.decode('utf-8')
        elif field_number == 2:
            for inner_field, _wt, inner_val in _read_fields(val):
                if inner_field == 1:
                    value = inner_val.decode('utf-8')
    return key, value


def _decode_attributes(data):
    attributes = {}
    for field_number, _wire_type, val in _read_fields(data):
        if field_number == 1:
            key, value = _decode_key_value(val)
            attributes[key] = value
    return attributes


def _decode_number_data_point(data):
    point = {
        'attributes': {},
        'start_time_unix_nano': None,
        'time_unix_nano': None,
        'as_int': None,
        'as_double': None,
    }
    for field_number, _wire_type, val in _read_fields(data):
        if field_number == 2:
            key, value = _decode_key_value(val)
            point['attributes'][key] = value
        elif field_number == 3:
            point['start_time_unix_nano'] = struct.unpack('<Q', val)[0]
        elif field_number == 4:
            point['time_unix_nano'] = struct.unpack('<Q', val)[0]
        elif field_number == 6:
            point['as_double'] = struct.unpack('<d', val)[0]
        elif field_number == 7:
            point['as_int'] = struct.unpack('<q', val)[0]
    return point


def _decode_metric(data):
    metric = {'name': None, 'description': '', 'unit': ''}
    for field_number, _wire_type, val in _read_fields(data):
        if field_number == 1:
            metric['name'] = val.decode('utf-8')
        elif field_number == 2:
            metric['description'] = val.decode('utf-8')
        elif field_number == 3:
            metric['unit'] = val.decode('utf-8')
        elif field_number == 5:
            metric['gauge'] = {
                'data_points': [
                    _decode_number_data_point(v)
                    for f, _w, v in _read_fields(val) if f == 1
                ],
            }
        elif field_number == 7:
            sum_data = {
                'data_points': [],
                'aggregation_temporality': AGGREGATION_TEMPORALITY_UNSPECIFIED,
                'is_monotonic': False,
            }
            for f, _w, v in _read_fields(val):
                if f == 1:
                    sum_data['data_points'].append(_decode_number_data_point(v))
                elif f == 2:
                    sum_data['aggregation_temporality'] = v
                elif f == 3:
                    sum_data['is_monotonic'] = bool(v)
            metric['sum'] = sum_data
    return metric


def decode_metrics_data(data):
    """Decode an OTLP MetricsData protobuf payload into plain dicts.

    Returns:
        dict: {'resource_metrics': [{'resource_attributes': {...},
        'scope_metrics': [{'scope': {'name': ..., 'version': ...},
        'metrics': [...]}]}]}
    """
    resource_metrics_list = []
    for field_number, _wire_type, val in _read_fields(data):
        if field_number != 1:
            continue
        resource_attributes = {}
        scope_metrics = []
        for f, _w, v in _read_fields(val):
            if f == 1:
                resource_attributes = _decode_attributes(v)
            elif f == 2:
                scope = {'name': '', 'version': ''}
                metrics = []
                for f2, _w2, v2 in _read_fields(v):
                    if f2 == 1:
                        for f3, _w3, v3 in _read_fields(v2):
                            if f3 == 1:
                                scope['name'] = v3.decode('utf-8')
                            elif f3 == 2:
                                scope['version'] = v3.decode('utf-8')
                    elif f2 == 2:
                        metrics.append(_decode_metric(v2))
                scope_metrics.append({'scope': scope, 'metrics': metrics})
        resource_metrics_list.append({
            'resource_attributes': resource_attributes,
            'scope_metrics': scope_metrics,
        })
    return {'resource_metrics': resource_metrics_list}

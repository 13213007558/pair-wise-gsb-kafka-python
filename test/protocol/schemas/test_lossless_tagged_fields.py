"""Lossless retention of unknown tagged fields (flexible versions / KIP-482).

A broker running a newer schema may emit tagged fields this client does not
know about. The decoder must keep them as opaque bytes so a controlled
intermediary can forward them, while never presenting them as interpreted,
authorized configuration. These tests cover:

  * raw unknown-tag retention and byte-for-byte re-encoding,
  * identical output from StructField.encode / encode_into / the codegen path,
  * retention inside nested structs and struct arrays,
  * rejection when a retained tag collides with a schema-known tag,
  * rejection of malformed sections (duplicate / out-of-order tags, bad
    lengths, truncated payloads) so "raw retention" cannot bypass validation.
"""
import io

import pytest

from kafka.protocol.data_container import DataContainer
from kafka.protocol.schemas.fields import StructField
from kafka.protocol.schemas.fields.codecs import (
    EncodeBuffer,
    RawTaggedField,
    TaggedFieldError,
    TaggedFields,
    UnsignedVarInt32,
)


def _flexible_struct():
    # Scalar fields plus one schema-known tagged field. No non-nullable nested
    # struct, so containers can be constructed without supplying every child.
    return StructField({
        'name': 'Outer', 'type': 'Outer',
        'versions': '0+', 'validVersions': '0',
        'fields': [
            {'name': 'x', 'type': 'int32', 'versions': '0+'},
            {'name': 'flavor', 'tag': 0, 'taggedVersions': '0+',
             'type': 'string', 'versions': '0+'},
        ],
    })


def _nested_struct():
    return StructField({
        'name': 'Outer', 'type': 'Outer',
        'versions': '0+', 'validVersions': '0',
        'fields': [
            {'name': 'x', 'type': 'int32', 'versions': '0+'},
            {'name': 'child', 'type': 'Child', 'versions': '0+',
             'fields': [
                 {'name': 'y', 'type': 'int32', 'versions': '0+'},
                 {'name': 'note', 'tag': 0, 'taggedVersions': '0+',
                  'type': 'string', 'versions': '0+'},
             ]},
            {'name': 'kids', 'type': '[]Child', 'versions': '0+',
             'fields': [
                 {'name': 'y', 'type': 'int32', 'versions': '0+'},
                 {'name': 'note', 'tag': 0, 'taggedVersions': '0+',
                  'type': 'string', 'versions': '0+'},
             ]},
        ],
    })


def _container(struct):
    return type('Outer', (DataContainer,), {'_struct': struct})


def _child_wire(y, unknowns):
    wire = y.to_bytes(4, 'big', signed=True)
    wire += UnsignedVarInt32.encode(1 + len(unknowns))
    # known tag 0: compact string 'hi' (length prefix 3)
    wire += UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(3) + b'\x03hi'
    for tag, payload in unknowns:
        wire += UnsignedVarInt32.encode(tag)
        wire += UnsignedVarInt32.encode(len(payload)) + payload
    return wire


# --------------------------------------------------------------------------
# RawTaggedField basics
# --------------------------------------------------------------------------

def test_raw_tagged_field_validates_inputs():
    raw = RawTaggedField(9, b'\x01\x02')
    assert raw.tag == 9 and raw.data == b'\x01\x02'
    # bytearray/memoryview are normalized to immutable bytes
    assert RawTaggedField(9, bytearray(b'ab')).data == b'ab'
    assert RawTaggedField(9, memoryview(b'ab')).data == b'ab'

    with pytest.raises(TaggedFieldError):
        RawTaggedField(-1, b'x')
    with pytest.raises(TaggedFieldError):
        RawTaggedField('9', b'x')
    with pytest.raises(TaggedFieldError):
        RawTaggedField(9, 'not-bytes')


def test_raw_tagged_field_read_from_rejects_truncation():
    data = io.BytesIO(b'ab')
    with pytest.raises(TaggedFieldError):
        RawTaggedField.read_from(7, data, 5)
    # a well-bounded read consumes exactly the declared length
    data = io.BytesIO(b'abcdef')
    raw = RawTaggedField.read_from(7, data, 3)
    assert raw == RawTaggedField(7, b'abc')
    assert data.read() == b'def'


# --------------------------------------------------------------------------
# TaggedFields codec: retention + ordering + collision
# --------------------------------------------------------------------------

def test_decode_keeps_unknown_tags_as_raw_bytes():
    struct = _flexible_struct()
    Outer = _container(struct)
    wire = (
        b'\x00\x00\x00\x05'
        + UnsignedVarInt32.encode(3)
        + UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(4) + b'\x04van'
        + UnsignedVarInt32.encode(7) + UnsignedVarInt32.encode(3) + b'\x01\x02\x03'
        + UnsignedVarInt32.encode(99) + UnsignedVarInt32.encode(2) + b'\xaa\xbb'
    )
    obj = struct.decode(io.BytesIO(wire), version=0,
                        compact=True, tagged=True, data_class=Outer)

    # known tag is interpreted ...
    assert obj.tags == {'flavor'}
    assert obj.flavor == 'van'
    # ... unknown tags are retained but never interpreted
    assert obj.unknown_tag_ids() == (7, 99)
    assert obj.unknown_tags['_7'] == b'\x01\x02\x03'
    assert obj.get_unknown_tag(99) == RawTaggedField(99, b'\xaa\xbb')
    assert obj.get_unknown_tag(1000) is None
    # no attribute access exposes the unknown value as business config
    assert not hasattr(obj, '_7')


def test_reencode_is_lossless_and_paths_agree():
    struct = _flexible_struct()
    Outer = _container(struct)
    wire = (
        b'\x00\x00\x00\x05'
        + UnsignedVarInt32.encode(2)
        + UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(4) + b'\x04van'
        + UnsignedVarInt32.encode(99) + UnsignedVarInt32.encode(2) + b'\xaa\xbb'
    )
    obj = struct.decode(io.BytesIO(wire), version=0,
                        compact=True, tagged=True, data_class=Outer)

    reference = struct.encode(obj, version=0, compact=True, tagged=True)

    out = EncodeBuffer()
    struct.encode_into(obj, out, version=0, compact=True, tagged=True)
    into_bytes = bytes(out.result())

    fn = struct.compiled_encode_into(0, compact=True, tagged=True)
    out2 = EncodeBuffer()
    fn(obj, out2)
    codegen_bytes = bytes(out2.result())

    assert reference == wire          # byte-for-byte preservation
    assert into_bytes == reference    # encode_into parity
    assert codegen_bytes == reference  # production codegen parity


def test_codegen_decode_retains_unknown_tags():
    struct = _flexible_struct()
    Outer = _container(struct)
    wire = (
        b'\x00\x00\x00\x05'
        + UnsignedVarInt32.encode(1)
        + UnsignedVarInt32.encode(7) + UnsignedVarInt32.encode(3) + b'\x01\x02\x03'
    )
    decode_fn = struct.compiled_decode_from(
        0, compact=True, tagged=True, data_class=Outer)
    obj, pos = decode_fn(memoryview(wire), 0)
    assert pos == len(wire)
    assert obj.unknown_tags == {'_7': b'\x01\x02\x03'}

    encode_fn = struct.compiled_encode_into(0, compact=True, tagged=True)
    out = EncodeBuffer()
    encode_fn(obj, out)
    assert bytes(out.result()) == wire


def test_nested_struct_and_struct_array_retention():
    struct = _nested_struct()
    Outer = _container(struct)
    wire = (
        b'\x00\x00\x00\x01'
        + _child_wire(10, [(5, b'\xde\xad'), (40, b'\x00')])
        + UnsignedVarInt32.encode(3)  # compact array, 2 elements
        + _child_wire(20, [(8, b'abcd')])
        + _child_wire(30, [])
        + UnsignedVarInt32.encode(1)
        + UnsignedVarInt32.encode(77) + UnsignedVarInt32.encode(2) + b'zz'
    )

    # reference decode
    obj = struct.decode(io.BytesIO(wire), version=0,
                        compact=True, tagged=True, data_class=Outer)
    assert obj.child.note == 'hi'                      # known nested tag interpreted
    assert obj.child.unknown_tag_ids() == (5, 40)
    assert obj.child.get_unknown_tag(5).data == b'\xde\xad'
    assert obj.kids[0].unknown_tags == {'_8': b'abcd'}
    assert obj.kids[1].unknown_tags is None
    assert obj.unknown_tags == {'_77': b'zz'}

    assert struct.encode(obj, version=0, compact=True, tagged=True) == wire

    # codegen decode -> encode round trip
    decode_fn = struct.compiled_decode_from(
        0, compact=True, tagged=True, data_class=Outer)
    obj2, pos = decode_fn(memoryview(wire), 0)
    assert pos == len(wire)
    assert obj2.child.unknown_tags == {'_5': b'\xde\xad', '_40': b'\x00'}
    assert obj2.kids[0].unknown_tags == {'_8': b'abcd'}
    encode_fn = struct.compiled_encode_into(0, compact=True, tagged=True)
    out = EncodeBuffer()
    encode_fn(obj2, out)
    assert bytes(out.result()) == wire


def test_explicit_retention_api_roundtrip():
    struct = _flexible_struct()
    Outer = _container(struct)
    obj = Outer(version=0, x=5, flavor='van')
    obj.add_unknown_tag(9, RawTaggedField(9, b'\x09\x08'))
    obj.add_unknown_tag(3, b'raw')  # bare-bytes convenience

    encoded = struct.encode(obj, version=0, compact=True, tagged=True)
    decoded = struct.decode(io.BytesIO(encoded), version=0,
                            compact=True, tagged=True, data_class=Outer)
    assert decoded.get_unknown_tag(9).data == b'\x09\x08'
    assert decoded.get_unknown_tag(3).data == b'raw'
    # retained tags never join the interpreted tag set
    assert decoded.tags == {'flavor'}


def test_constructor_unknown_tags_mapping():
    struct = _flexible_struct()
    Outer = _container(struct)
    obj = Outer(version=0, x=1, unknown_tags={'_7': b'ab', 50: b'cd'})
    assert obj.unknown_tag_ids() == (7, 50)
    encoded = struct.encode(obj, version=0, compact=True, tagged=True)
    # wire order must be ascending regardless of insertion order
    assert encoded.index(UnsignedVarInt32.encode(7)) < encoded.index(
        UnsignedVarInt32.encode(50))
    decoded = struct.decode(io.BytesIO(encoded), version=0,
                            compact=True, tagged=True, data_class=Outer)
    assert decoded.unknown_tags == {'_7': b'ab', '_50': b'cd'}


# --------------------------------------------------------------------------
# Security: collisions and malformed sections must never be "retained as-is"
# --------------------------------------------------------------------------

def test_retained_tag_colliding_with_known_tag_is_rejected():
    struct = _flexible_struct()
    Outer = _container(struct)
    obj = Outer(version=0, x=1)
    # tag 0 belongs to 'flavor'
    with pytest.raises(TaggedFieldError):
        obj.add_unknown_tag(0, b'xx')

    # same guard applies at the codec layer for hand-built dicts
    codec = struct.tagged_fields(0)
    with pytest.raises(TaggedFieldError):
        codec.encode({'_0': b'xx'}, version=0)
    out = EncodeBuffer()
    with pytest.raises(TaggedFieldError):
        codec.encode_into({'_0': b'xx'}, out, version=0)


def test_decode_rejects_duplicate_and_out_of_order_tags():
    struct = _flexible_struct()
    Outer = _container(struct)

    def section(tags):
        wire = b'\x00\x00\x00\x01' + UnsignedVarInt32.encode(len(tags))
        for tag, payload in tags:
            wire += UnsignedVarInt32.encode(tag)
            wire += UnsignedVarInt32.encode(len(payload)) + payload
        return wire

    duplicate = section([(7, b'x'), (7, b'y')])
    out_of_order = section([(9, b'x'), (7, b'y')])
    for blob in (duplicate, out_of_order):
        with pytest.raises(TaggedFieldError):
            struct.decode(io.BytesIO(blob), version=0,
                          compact=True, tagged=True, data_class=Outer)
        decode_fn = struct.compiled_decode_from(
            0, compact=True, tagged=True, data_class=Outer)
        with pytest.raises(TaggedFieldError):
            decode_fn(memoryview(blob), 0)


def test_decode_rejects_truncated_payload_and_bad_length():
    struct = _flexible_struct()
    Outer = _container(struct)
    # declares 5 payload bytes but only provides 2
    truncated = (
        b'\x00\x00\x00\x01'
        + UnsignedVarInt32.encode(1)
        + UnsignedVarInt32.encode(7) + UnsignedVarInt32.encode(5) + b'ab'
    )
    with pytest.raises(TaggedFieldError):
        struct.decode(io.BytesIO(truncated), version=0,
                      compact=True, tagged=True, data_class=Outer)

    # a malformed inner value for a *known* tag must also raise instead of
    # being silently swallowed by bounded retention
    bad_known = (
        b'\x00\x00\x00\x01'
        + UnsignedVarInt32.encode(1)
        + UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(1) + b'\xff'
    )
    with pytest.raises(Exception):
        struct.decode(io.BytesIO(bad_known), version=0,
                      compact=True, tagged=True, data_class=Outer)


def test_truncated_varint_still_raises():
    struct = _flexible_struct()
    Outer = _container(struct)
    # tag-count varint with continuation bit set but no following byte
    blob = b'\x00\x00\x00\x01\x01\xff'
    with pytest.raises(Exception):
        struct.decode(io.BytesIO(blob), version=0,
                      compact=True, tagged=True, data_class=Outer)


def test_struct_array_known_tagged_field_not_dropped_by_codegen():
    # Regression: an element with one untagged field plus a tagged field was
    # mistaken for a bare-scalar ("single-field") struct, so the codegen path
    # emitted an empty tagged section and silently dropped the known tag.
    struct = StructField({
        'name': 'Outer', 'type': 'Outer',
        'versions': '0+', 'validVersions': '0',
        'fields': [
            {'name': 'x', 'type': 'int32', 'versions': '0+'},
            {'name': 'kids', 'type': '[]Child', 'versions': '0+', 'fields': [
                {'name': 'y', 'type': 'int32', 'versions': '0+'},
                {'name': 'note', 'tag': 0, 'taggedVersions': '0+',
                 'type': 'string', 'versions': '0+'},
            ]},
        ],
    })
    Outer = _container(struct)
    Child = struct.fields['kids'].data_class
    obj = Outer(version=0, x=1, kids=[Child(version=0, y=2, note='hi')])

    reference = struct.encode(obj, version=0, compact=True, tagged=True)
    encode_fn = struct.compiled_encode_into(0, compact=True, tagged=True)
    out = EncodeBuffer()
    encode_fn(obj, out)
    assert bytes(out.result()) == reference

    # and the tagged value survives the full codegen round trip
    decode_fn = struct.compiled_decode_from(
        0, compact=True, tagged=True, data_class=Outer)
    decoded, _ = decode_fn(memoryview(reference), 0)
    assert decoded.kids[0].note == 'hi'


def test_retained_value_must_be_bytes_like():
    struct = _flexible_struct()
    Outer = _container(struct)
    with pytest.raises(TaggedFieldError):
        Outer(version=0, x=1, unknown_tags={'_7': 123})
    codec = struct.tagged_fields(0)
    with pytest.raises(TaggedFieldError):
        codec.encode({'_7': object()}, version=0)

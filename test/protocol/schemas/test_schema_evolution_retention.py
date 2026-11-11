"""End-to-end schema-evolution demonstration for unknown tagged fields.

A controlled intermediary is built against an *older* schema than the broker
it talks to. The newer broker emits tagged fields the intermediary was never
taught. The test proves that:

  1. the old client still interprets the fields it knows and treats the rest
     strictly as opaque retained bytes (never as understood/authorized
     configuration),
  2. re-encoding the message forwards every unknown field byte-for-byte, on
     both the reference and the compiled codegen paths,
  3. a current-schema client on the other side recovers the original values
     untouched.

ApiVersionsResponse v3 is used because its real schema already carries four
flexible tagged fields (tags 0-3). The "old" schema below only knows tag 0.
"""
import copy
import io

from kafka.protocol.data_container import DataContainer
from kafka.protocol.metadata import ApiVersionsResponse as CurrentResponse
from kafka.protocol.schemas import load_json
from kafka.protocol.schemas.fields import StructField
from kafka.protocol.schemas.fields.codecs import EncodeBuffer


VERSION = 3


def _build_old_schema():
    """Schema that only understands SupportedFeatures (tag 0).

    Tags 1 (FinalizedFeaturesEpoch), 2 (FinalizedFeatures) and 3
    (ZkMigrationReady) are removed, so a v3 broker emitting them looks like a
    forward-compatible extension to this client.
    """
    schema_json = copy.deepcopy(load_json('ApiVersionsResponse'))
    schema_json['fields'] = [
        field for field in schema_json['fields']
        if field.get('tag') in (None, 0)
    ]
    struct = StructField(schema_json)
    return type('OldApiVersionsResponse', (DataContainer,), {'_struct': struct}), struct


def _current_message():
    Feature = CurrentResponse.SupportedFeatureKey
    return CurrentResponse[VERSION](
        error_code=0,
        api_keys=[(18, 0, 3)],
        throttle_time_ms=0,
        # Non-default so tag 0 is actually present on the wire
        supported_features=[Feature(name='metadata_quorum', min_version=0, max_version=1)],
        finalized_features_epoch=42,
        finalized_features=[],
        zk_migration_ready=True,
    )


def test_old_schema_forwards_new_schema_message_losslessly():
    OldResponse, old_struct = _build_old_schema()
    message = _current_message()
    wire = message.encode()

    # --- Old client decodes the newer broker's message ---------------------
    old_obj = old_struct.decode(
        io.BytesIO(wire), version=VERSION, compact=True, tagged=True,
        data_class=OldResponse)

    # The tag it was taught (tag 0) is fully interpreted as a normal field.
    assert old_obj.tags == {'supported_features'}
    assert old_obj.supported_features[0].name == 'metadata_quorum'

    # The tags it was never taught are retained as raw bytes, not understood.
    assert old_obj.unknown_tag_ids() == (1, 3)  # tag 2 omitted: empty default
    assert old_obj.get_unknown_tag(1).data == (42).to_bytes(8, 'big')
    assert old_obj.get_unknown_tag(3).data == b'\x01'  # boolean true

    # Business logic cannot reach the extensions as authorized attributes.
    assert not hasattr(old_obj, 'zk_migration_ready')
    assert not hasattr(old_obj, 'finalized_features_epoch')
    assert not hasattr(old_obj, 'finalized_features')

    # --- Re-encoding forwards everything byte-for-byte --------------------
    reference = old_struct.encode(
        old_obj, version=VERSION, compact=True, tagged=True)
    assert reference == wire

    encode_fn = old_struct.compiled_encode_into(
        VERSION, compact=True, tagged=True)
    out = EncodeBuffer()
    encode_fn(old_obj, out)
    assert bytes(out.result()) == wire

    # --- A current-schema client recovers the original semantics ----------
    recovered = CurrentResponse[VERSION].decode(io.BytesIO(reference))
    assert recovered.zk_migration_ready is True
    assert recovered.finalized_features_epoch == 42
    assert recovered.supported_features[0].name == 'metadata_quorum'
    assert recovered.encode() == wire
    assert recovered == message


def test_retained_bytes_survive_multiple_hops():
    # The retained section must remain intact even if the message is decoded
    # and re-encoded repeatedly through the old intermediary.
    OldResponse, old_struct = _build_old_schema()
    wire = _current_message().encode()

    blob = wire
    for _ in range(3):
        hop = old_struct.decode(
            io.BytesIO(blob), version=VERSION, compact=True, tagged=True,
            data_class=OldResponse)
        blob = old_struct.encode(
            hop, version=VERSION, compact=True, tagged=True)
        assert blob == wire

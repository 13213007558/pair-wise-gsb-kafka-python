import io

from .types import UnsignedVarInt32


class TaggedFieldError(ValueError):
    """Raised when a tagged-fields section is malformed or ambiguous.

    This covers out-of-order/duplicate tags, declared payload lengths that
    overrun the buffer, and attempts to attach a retained (unknown) field to a
    tag that the current schema already explains. Retention never suppresses
    these checks: unknown bytes are only preserved when the surrounding wire
    structure is itself well formed.
    """


def _uvarint_size(value):
    """Return the number of bytes needed to encode value as an unsigned varint."""
    if value < 0x80:
        return 1
    elif value < 0x4000:
        return 2
    elif value < 0x200000:
        return 3
    elif value < 0x10000000:
        return 4
    else:
        return 5


def unknown_tag_key(tag):
    """Return the dict key under which an unknown tag is stored by convention."""
    return '_%d' % tag


def parse_unknown_tag_key(name):
    """Parse an ``'_<tag>'`` key into its numeric tag id.

    Returns ``None`` for keys that do not name a retained unknown tag.
    """
    if not isinstance(name, str) or not name.startswith('_'):
        return None
    digits = name[1:]
    if not digits or not digits.isdigit():
        return None
    return int(digits)


class RawTaggedField:
    """A tagged field the local schema does not explain, kept as raw bytes.

    The Kafka flexible-version wire format lets brokers add new tagged fields
    without breaking older clients. An older decoder must still be able to
    forward those fields byte-for-byte, so this object retains exactly the
    payload bytes that appeared inside the field's length prefix (the prefix
    itself is regenerated on encode).

    Only the raw payload is retained: the object is never parsed, so business
    code cannot mistake an extension field for an interpreted/authorized
    configuration value.
    """

    __slots__ = ('tag', 'data')

    def __init__(self, tag, data):
        if not isinstance(tag, int) or tag < 0:
            raise TaggedFieldError(
                'Retained tagged field tag must be a non-negative integer, got %r' % (tag,))
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TaggedFieldError(
                'Retained tagged field data must be bytes-like, got %s' % type(data).__name__)
        self.tag = tag
        self.data = bytes(data)

    @classmethod
    def read_from(cls, tag, data, size):
        """Read one retained field payload and verify the declared length.

        A truncated payload (fewer than ``size`` bytes available) raises
        :class:`TaggedFieldError` instead of silently retaining a short value.
        """
        if size < 0:
            raise TaggedFieldError('Invalid tagged field length %d for tag %d' % (size, tag))
        payload = data.read(size)
        if len(payload) != size:
            raise TaggedFieldError(
                'Truncated tagged field %d: expected %d bytes, got %d'
                % (tag, size, len(payload)))
        return cls(tag, payload)

    def encoded(self):
        """Return the full on-wire encoding (tag, length, payload)."""
        return (UnsignedVarInt32.encode(self.tag)
                + UnsignedVarInt32.encode(len(self.data))
                + self.data)

    def encode_into(self, out):
        UnsignedVarInt32.encode_into(out, self.tag)
        UnsignedVarInt32.encode_into(out, len(self.data))
        out.ensure(len(self.data))
        out.buf[out.pos:out.pos + len(self.data)] = self.data
        out.pos += len(self.data)

    def __eq__(self, other):
        if not isinstance(other, RawTaggedField):
            return NotImplemented
        return self.tag == other.tag and self.data == other.data

    def __hash__(self):
        return hash((self.tag, self.data))

    def __repr__(self):
        return 'RawTaggedField(tag=%d, data=%r)' % (self.tag, self.data)


def _coerce_retained_field(tag, value):
    """Normalize a retained unknown-tag value into a RawTaggedField."""
    if isinstance(value, RawTaggedField):
        if value.tag != tag:
            raise TaggedFieldError(
                'Retained field key tag %d does not match RawTaggedField tag %d'
                % (tag, value.tag))
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return RawTaggedField(tag, value)
    raise TaggedFieldError(
        'Unsupported retained tagged field value for tag %d: %s'
        % (tag, type(value).__name__))


class TaggedFields:
    def __init__(self, fields):
        self._fields = list(fields) # listify to clean filter() inputs
        self._tags = {field.tag: field for field in self._fields}
        self._names = {field.name: field for field in self._fields}

    # ------------------------------------------------------------------
    # Encode planning
    # ------------------------------------------------------------------
    def _known_tag_encodings(self, item, version):
        """Yield (tag, name, value) for schema-known tags present on item."""
        if isinstance(item, dict):
            for name, val in item.items():
                field = self._names.get(name)
                if field is not None and val != field.default:
                    yield field.tag, name, val
        else:
            # DataContainer/object and tuple alike expose known fields by name.
            # Tuples never carry retained fields; _retained_fields handles that.
            for name, field in self._names.items():
                if hasattr(item, name):
                    val = getattr(item, name)
                    if val != field.default:
                        yield field.tag, name, val

    def _retained_fields(self, item):
        """Collect retained unknown fields as RawTaggedField objects.

        Raises TaggedFieldError if a retained field reuses a tag that this
        schema already explains, or if the stored value cannot be carried as
        opaque bytes.
        """
        retained = []
        if isinstance(item, dict):
            candidates = item.items()
        else:
            unknown = getattr(item, 'unknown_tags', None)
            candidates = unknown.items() if unknown else ()
        for name, val in candidates:
            tag = parse_unknown_tag_key(name)
            if tag is None:
                continue
            if tag in self._tags:
                raise TaggedFieldError(
                    'Retained unknown tag %d collides with schema field %r'
                    % (tag, self._tags[tag].name))
            retained.append(_coerce_retained_field(tag, val))
        return retained

    def _plan(self, item, version):
        """Resolve the ordered list of entries to put on the wire.

        Known entries are (tag, 'known', field, value); retained entries are
        (tag, 'raw', None, RawTaggedField). Both encode paths consume this same
        plan, guaranteeing identical output. Entries are emitted in strictly
        ascending tag order, as required by the Kafka wire format, and a
        duplicate tag is rejected.
        """
        entries = [(tag, 'known', self._tags[tag], val)
                   for tag, _name, val in self._known_tag_encodings(item, version)]
        entries += [(raw.tag, 'raw', None, raw)
                    for raw in self._retained_fields(item)]
        entries.sort(key=lambda entry: entry[0])
        prev_tag = -1
        for tag, _kind, _field, _val in entries:
            if tag <= prev_tag:
                raise TaggedFieldError('Duplicate or out-of-order tagged field %d' % tag)
            prev_tag = tag
        return entries

    def encode(self, item, version=None):
        entries = self._plan(item, version)
        ret = [UnsignedVarInt32.encode(len(entries))]
        for tag, kind, field, val in entries:
            if kind == 'raw':
                ret.append(val.encoded())
                continue
            ret.append(UnsignedVarInt32.encode(tag))
            # struct tags have an empty nested tagged fields (tagged=None)
            encoded_val = field.encode(val, version=version, compact=True, tagged=None)
            ret.append(UnsignedVarInt32.encode(len(encoded_val)))
            ret.append(encoded_val)
        return b''.join(ret)

    def encode_into(self, item, out, version=None):
        entries = self._plan(item, version)
        UnsignedVarInt32.encode_into(out, len(entries))
        for tag, kind, field, val in entries:
            if kind == 'raw':
                val.encode_into(out)
                continue
            UnsignedVarInt32.encode_into(out, tag)
            # Encode value into buffer after reserving space for size prefix.
            # Strategy: assume size fits in 1 byte (< 128), encode value,
            # then fix up if the size varint is larger.
            out.ensure(1)  # reserve the assumed 1-byte size prefix
            size_pos = out.pos
            out.pos += 1  # reserve 1 byte for size
            val_start = out.pos
            # struct tags have an empty nested tagged fields (tagged=None)
            field.encode_into(val, out, version=version, compact=True, tagged=None)
            val_size = out.pos - val_start
            actual_size_len = _uvarint_size(val_size)
            if actual_size_len == 1:
                # Common case: size fits in 1 byte, just write it
                out.buf[size_pos] = val_size
            else:
                # Rare case: size needs more bytes. Shift the value data forward.
                extra = actual_size_len - 1
                out.ensure(extra)
                # Move value bytes forward to make room
                out.buf[val_start + extra:out.pos + extra] = out.buf[val_start:out.pos]
                out.pos += extra
                # Write the multi-byte size at size_pos
                saved_pos = out.pos
                out.pos = size_pos
                UnsignedVarInt32.encode_into(out, val_size)
                out.pos = saved_pos

    def decode(self, data, version=None):
        num_fields = UnsignedVarInt32.decode(data)
        ret = {}
        prev_tag = -1
        for i in range(num_fields):
            tag = UnsignedVarInt32.decode(data)
            if tag <= prev_tag:
                raise TaggedFieldError('Duplicate or out-of-order tagged field %d' % tag)
            prev_tag = tag
            size = UnsignedVarInt32.decode(data)
            if tag in self._tags:
                # Read the full bounded payload first so a malformed/overlong
                # inner value cannot spill into the next tag or be masked by
                # "raw retention".
                field = self._tags[tag]
                raw = RawTaggedField.read_from(tag, data, size)
                # struct tags have an empty nested tagged fields (tagged=None)
                ret[field.name] = field.decode(
                    io.BytesIO(raw.data), version=version, compact=True, tagged=None)
            else:
                ret[unknown_tag_key(tag)] = RawTaggedField.read_from(tag, data, size).data
        return ret

    @classmethod
    def decode_empty(cls, data):
        assert UnsignedVarInt32.decode(data) == 0

    @classmethod
    def encode_empty(cls):
        return UnsignedVarInt32.encode(0)

    def __repr__(self):
        return 'TaggedFields(%s)' % list(self._names.keys())

# -*- coding: utf-8 -*-
"""Audit: does kafka-python (check_crcs=True) reject corrupt records BEFORE
invoking the business deserializer?

Method: drive kafka.consumer.fetcher.Fetcher.PartitionRecords (the exact
code path a real consumer uses) with independently built wire bytes, while
spying on every validate_crc() and every deserializer call. The event log
gives the call-order evidence; the case matrix gives the coverage.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import wire_fixtures as wf

from kafka.consumer.fetcher import Fetcher
from kafka.errors import CorruptRecordError
from kafka.record.memory_records import MemoryRecords
from kafka.record.legacy_records import LegacyRecordBatch, LegacyRecord
from kafka.record.default_records import DefaultRecordBatch, DefaultRecord
from kafka.structs import TopicPartition

wf.self_check()

EVENTS = []         # call-order evidence, reset per case
SIDE_EFFECTS = []   # what the "audit deserializer" would have written


class AuditDeserializer(object):
    """Business deserializer with a side effect (audit write)."""

    def __init__(self, name):
        self.name = name

    def deserialize(self, topic, headers, data):
        EVENTS.append(("deserialize", self.name, data))
        SIDE_EFFECTS.append({"kind": "AUDIT_WRITE", "field": self.name,
                             "payload": None if data is None else bytes(data)})
        return data


def _spy(cls, label):
    original = cls.validate_crc

    def wrapper(self):
        result = original(self)
        EVENTS.append(("validate_crc", label, bool(result)))
        return result

    cls.validate_crc = wrapper


for _cls, _label in ((LegacyRecordBatch, "LegacyRecordBatch"),
                     (LegacyRecord, "LegacyRecord"),
                     (DefaultRecordBatch, "DefaultRecordBatch"),
                     (DefaultRecord, "DefaultRecord")):
    _spy(_cls, _label)


def run_case(wire_bytes, check_crcs=True, fetch_offset=0):
    """Drive Fetcher.PartitionRecords exactly like Fetcher._append does."""
    del EVENTS[:]
    del SIDE_EFFECTS[:]
    records = MemoryRecords(wire_bytes)
    tp = TopicPartition("audit-topic", 0)
    part = Fetcher.PartitionRecords(
        fetch_offset, tp, records,
        key_deserializer=AuditDeserializer("key"),
        value_deserializer=AuditDeserializer("value"),
        check_crcs=check_crcs)
    consumed, error = [], None
    try:
        while part:                       # mirrors Fetcher._append's `if not part`
            chunk = part.take(100)        # same entry point the fetcher uses
            if not chunk:
                break
            consumed.extend(chunk)
    except CorruptRecordError as exc:
        error = "CorruptRecordError: %s" % exc
    except Exception as exc:              # noqa: BLE001 - record any other failure mode
        error = "%s: %s" % (type(exc).__name__, exc)
    return {
        "consumed": [(r.offset, r.key, r.value) for r in consumed],
        "error": error,
        "events": list(EVENTS),
        "side_effects": list(SIDE_EFFECTS),
    }


def summarize(result):
    deser = [e for e in result["events"] if e[0] == "deserialize"]
    crcs = [e for e in result["events"] if e[0] == "validate_crc"]
    return {
        "crc_checks": len(crcs),
        "crc_failed": any(e[2] is False for e in crcs),
        "deserialize_calls": len(deser),
        "records_consumed": len(result["consumed"]),
        "error": (result["error"] or "").split(":")[0] or "none",
    }


# ---------------------------------------------------------------------------
# Case matrix
# ---------------------------------------------------------------------------

K, V = b"key-1", b"value-payload-1"
K2, V2 = b"key-2", b"value-payload-2"
TS = 1700000000000

valid_v0 = wf.legacy_message(0, 0, K, V)
valid_v1 = wf.legacy_message(1, 0, K, V, timestamp=TS)
valid_v1_gzip = wf.legacy_gzip_batch(1, 0, [(K, V), (K2, V2)], timestamp=TS)
valid_v2 = wf.v2_batch([wf.v2_record(0, 0, K, V, headers=[("trace", b"abc")]),
                        wf.v2_record(1, 5, K2, V2)])
valid_v2_gzip = wf.v2_batch([wf.v2_record(0, 0, K, V),
                             wf.v2_record(1, 5, K2, V2)], compression="gzip")

CASES = []
def case(name, category, wire, expect, note, check_crcs=True):
    CASES.append(dict(name=name, category=category, wire=wire,
                      expect=expect, note=note, check_crcs=check_crcs))

# --- Positive controls: independently built valid bytes must parse cleanly ---
case("P1 legacy-v0 valid", "positive", valid_v0,
     "accepted", "oracle sanity: hand-built v0 message parses")
case("P2 legacy-v1 valid", "positive", valid_v1,
     "accepted", "oracle sanity: hand-built v1 message parses")
case("P3 legacy-v1 gzip valid", "positive", valid_v1_gzip,
     "accepted", "2 inner records, relative offsets")
case("P4 v2 valid", "positive", valid_v2,
     "accepted", "2 records, one with headers")
case("P5 v2 gzip valid", "positive", valid_v2_gzip,
     "accepted", "gzip-compressed record batch")

# --- Mutations INSIDE CRC coverage: must be rejected before deserializer ---
case("I1 legacy-v0 value byte flipped", "inside-crc", wf.flip(valid_v0, len(valid_v0) - 1),
     "crc-reject", "value is CRC-covered")
case("I2 legacy-v1 key byte flipped", "inside-crc", wf.flip(valid_v1, 31),
     "crc-reject", "key is CRC-covered")
case("I3 legacy-v1 attributes flipped", "inside-crc", wf.flip(valid_v1, 17),
     "crc-reject", "attributes byte is CRC-covered")
case("I4 legacy-v1 timestamp flipped", "inside-crc", wf.flip(valid_v1, 20),
     "crc-reject", "v1 timestamp is CRC-covered")
case("I5 legacy-gzip compressed payload flipped", "inside-crc",
     wf.flip(valid_v1_gzip, len(valid_v1_gzip) - 3),
     "crc-reject", "outer CRC covers compressed bytes; rejected before gunzip")
case("I6 legacy-gzip inner CRC corrupt, outer CRC valid", "inside-crc",
     wf.legacy_gzip_batch(1, 0, [(K, V), (K2, V2)], timestamp=TS, inner_corrupt_index=1),
     "partial-then-reject", "corruption predates compression; outer CRC passes, good inner record 1 is deserialized, corrupt inner record 2 is rejected before ITS deserializer call")
case("I7 v2 record value byte flipped", "inside-crc",
     wf.flip(valid_v2, len(valid_v2) - len(V2) - 1 - len(V2) + 2),
     "crc-reject", "record payload is CRC32C-covered")
case("I8 v2 attributes flipped", "inside-crc", wf.flip(valid_v2, wf.V2_ATTRS),
     "crc-reject", "attributes are the first CRC32C-covered bytes")
case("I9 v2 last_offset_delta flipped", "inside-crc", wf.flip(valid_v2, wf.V2_LAST_OFFSET_DELTA),
     "crc-reject", "header field, but inside CRC32C coverage")
case("I10 v2-gzip compressed payload flipped", "inside-crc",
     wf.flip(valid_v2_gzip, len(valid_v2_gzip) - 2),
     "crc-reject", "batch CRC32C covers compressed payload; rejected before gunzip")
case("I11 v2 corrupt 2nd record of 2-record batch", "inside-crc",
     wf.flip(valid_v2, len(valid_v2) - 1),
     "crc-reject", "batch CRC is atomic: record 1 must NOT be deserialized either")

# --- Mutations OUTSIDE CRC coverage: CRC inherently cannot see these ---
case("O1 legacy-v1 offset field rewritten 0->5", "outside-crc",
     wf.poke_int(valid_v1, 0, ">q", 5),
     "accepted-shifted", "offset is outside CRC; consumer accepts shifted offset")
case("O2 legacy-v1 message_size shrunk by 1", "outside-crc",
     wf.poke_int(valid_v1, 8, ">i", len(valid_v1) - 12 - 1),
     "crc-reject", "size field not covered, but shorter slice changes CRC input -> caught indirectly")
case("O3 legacy-v1 message_size grown by 4", "outside-crc",
     wf.poke_int(valid_v1, 8, ">i", len(valid_v1) - 12 + 4),
     "silent-drop", "declared size exceeds buffer -> framing drops batch, no error at all")
case("O4 v2 base_offset rewritten 0->5", "outside-crc",
     wf.poke_int(valid_v2, wf.V2_BASE_OFFSET, ">q", 5),
     "accepted-shifted", "base_offset is outside CRC32C; offsets shift silently")
case("O5 v2 leader_epoch rewritten", "outside-crc",
     wf.poke_int(valid_v2, wf.V2_LEADER_EPOCH, ">i", 7),
     "accepted-shifted", "leader epoch is outside CRC32C")
case("O6 v2 batch_length shrunk by 4", "outside-crc",
     wf.poke_int(valid_v2, wf.V2_LENGTH, ">i", len(valid_v2) - 12 - 4),
     "crc-reject", "length not covered, but shorter slice changes CRC32C input -> caught indirectly")
case("O7 v2 batch_length grown by 4", "outside-crc",
     wf.poke_int(valid_v2, wf.V2_LENGTH, ">i", len(valid_v2) - 12 + 4),
     "silent-drop", "declared length exceeds buffer -> framing drops batch silently")
case("O8 v2 magic byte 2->1", "outside-crc",
     wf.poke_int(valid_v2, wf.V2_MAGIC, ">b", 1),
     "crc-reject", "magic is outside CRC; batch is REINTERPRETED as legacy and fails legacy CRC by accident")

# --- Truncation ---
case("T1 legacy truncated mid-message", "truncation", valid_v1[:-5],
     "silent-drop", "declared size exceeds remaining buffer: dropped, no error, no deserializer call")
case("T2 legacy truncated below LOG_OVERHEAD", "truncation", valid_v1[:10],
     "silent-drop", "fewer than 12 bytes left: dropped silently")
case("T3 legacy declared size below MIN_SLICE", "truncation",
     wf.poke_int(valid_v1, 8, ">i", 10)[:22],
     "framing-reject", "complete slice smaller than minimum record overhead -> CorruptRecordError")
case("T4 v2 truncated mid-batch", "truncation", valid_v2[:-6],
     "silent-drop", "batch_length exceeds buffer: dropped silently")
case("T5 v2 truncated record, length field adjusted", "truncation",
     wf.poke_int(valid_v2[:-3], wf.V2_LENGTH, ">i", len(valid_v2) - 12 - 3),
     "crc-reject", "framing made consistent, so CRC32C runs and fails")
case("T6 v2-gzip truncated payload, length adjusted", "truncation",
     wf.poke_int(valid_v2_gzip[:-4], wf.V2_LENGTH, ">i", len(valid_v2_gzip) - 12 - 4),
     "crc-reject", "CRC32C over shortened gzip payload fails before gunzip")

# --- Tamper with CRC recomputed: proves CRC is not authentication ---
case("A1 v2 value swapped, CRC32C recomputed", "tamper",
     wf.v2_batch([wf.v2_record(0, 0, K, b"FORGED-payload!!", headers=[("trace", b"abc")]),
                  wf.v2_record(1, 5, K2, V2)]),
     "accepted", "attacker recomputes unkeyed CRC -> consumer accepts forged value")
case("A2 legacy-v1 value swapped, CRC32 recomputed", "tamper",
     wf.legacy_message(1, 0, K, b"FORGED-payload!!", timestamp=TS),
     "accepted", "same for legacy CRC32")

# --- Mixed fetch: good record precedes corrupt one in the same buffer ---
case("M1 legacy messageset [good, corrupt]", "mixed",
     valid_v1 + wf.flip(wf.legacy_message(1, 1, K2, V2, timestamp=TS), -1),
     "partial-then-reject", "good record IS deserialized (side effect fires) before the corrupt one is rejected")

# --- Contrast: protection disappears with check_crcs=False ---
case("C1 legacy-v1 corrupt value, check_crcs=False", "control",
     wf.flip(valid_v1, len(valid_v1) - 1),
     "accepted", "with check_crcs=False the corrupt value reaches the deserializer",
     check_crcs=False)


def expectation_met(expect, s):
    if expect == "accepted":
        return s["error"] == "none" and s["records_consumed"] > 0
    if expect == "accepted-shifted":
        return s["error"] == "none" and s["records_consumed"] > 0 and not s["crc_failed"]
    if expect == "crc-reject":
        return s["error"] == "CorruptRecordError" and s["deserialize_calls"] == 0
    if expect == "framing-reject":
        return s["error"] == "CorruptRecordError" and s["deserialize_calls"] == 0
    if expect == "silent-drop":
        return s["error"] == "none" and s["records_consumed"] == 0 and s["deserialize_calls"] == 0
    if expect == "partial-then-reject":
        return (s["error"] == "CorruptRecordError" and s["records_consumed"] == 1
                and s["deserialize_calls"] == 2)  # key+value of the good record only
    return False


def main():
    results = []
    for c in CASES:
        r = run_case(c["wire"], check_crcs=c["check_crcs"])
        s = summarize(r)
        ok = expectation_met(c["expect"], s)
        results.append(dict(case=c, result=r, summary=s, ok=ok))
        print("%-46s %-12s crc=%d(fail=%s) deser=%d consumed=%d err=%-18s %s" % (
            c["name"], c["expect"], s["crc_checks"], s["crc_failed"],
            s["deserialize_calls"], s["records_consumed"], s["error"],
            "PASS" if ok else "**FAIL**"))

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audit_results.json")
    with open(out, "w") as fh:
        json.dump([{
            "name": r["case"]["name"], "category": r["case"]["category"],
            "expect": r["case"]["expect"], "note": r["case"]["note"],
            "check_crcs": r["case"]["check_crcs"], "ok": r["ok"],
            "summary": r["summary"],
            "events": [[e[0], e[1], (e[2] if not isinstance(e[2], bytes) else e[2].decode("utf-8", "replace"))]
                       for e in r["result"]["events"]],
            "consumed": [[o, None if k is None else k.decode("utf-8", "replace"),
                          None if v is None else v.decode("utf-8", "replace")]
                         for o, k, v in r["result"]["consumed"]],
            "error": r["result"]["error"],
        } for r in results], fh, indent=2, ensure_ascii=False)
    print("\nwrote %s" % out)
    failed = [r for r in results if not r["ok"]]
    print("total=%d failed=%d" % (len(results), len(failed)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

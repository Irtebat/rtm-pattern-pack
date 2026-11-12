"""NN-name: <one line on what the pattern does>.

Template for a new pattern. Copy this file to pNN_<name>.py in this folder
(for example p09_filter_rekey.py), replace every <...>, and the harness picks it
up: any module here that defines PATTERN is registered. Files starting with an
underscore, like this one, are skipped.

Most patterns use the shared input event (contract section 2: event_id, key,
seq, event_ts, is_dup) plus a few fields of their own, as below. A pattern with
a different event shape subclasses KeyedGenerator instead; see p02_unnest.py.
"""

from __future__ import annotations

import json
from typing import Optional

from rtmpack_harness.events import EventGenerator, event_encoder
from rtmpack_harness.patterns.base import EXTENDED_MIN_RECORD_BYTES, Pattern, event_id_key


class TemplateGenerator(EventGenerator):
    pattern_id = "NN-name"

    def _extra(self) -> dict:
        # Fields added to the shared event. Draw random numbers from self._rng only,
        # always in the same order, so a seed gives the same input every run.
        return {"amount": self._rng.randint(100, 50_000)}


def _check(suffix: Optional[str], doc: dict) -> bool:
    # True when an output record's value is correct. Remove (and check_value below)
    # if the output has no value to check.
    return "amount" in doc


PATTERN = Pattern(
    id="NN-name",
    description="<short description>",
    make_generator=TemplateGenerator,
    # Writes the extra fields into the JSON, before the padding.
    encode=event_encoder(lambda e: f',"amount":{e.amount}'),
    # Kafka keys of the output records one input must produce.
    output_keys=event_id_key,
    # Manifest count the collector compares against: "unique_sent" for one output
    # per input, "expected_outputs" when output_keys returns another number.
    expected_count_field="unique_sent",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    check_value=_check,
)

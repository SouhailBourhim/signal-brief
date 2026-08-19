"""Pure helpers from `spark/jobs/normalize.py` that don't need a JVM.

Deliberately its own file, unmarked: `test_normalize_window.py` is `pytestmark =
pytest.mark.spark` at module level, and `_to_signed_i64` is exactly the kind of small
pure function that should be checkable without one.
"""

from __future__ import annotations

from signal_core.hashing import simhash64
from signal_core.spark.jobs.normalize import _to_signed_i64


def test_to_signed_i64_is_a_no_op_below_the_signed_max():
    assert _to_signed_i64(0) == 0
    assert _to_signed_i64(2**63 - 1) == 2**63 - 1


def test_to_signed_i64_wraps_values_above_the_signed_max():
    # The exact bug: pyarrow's safe cast rejects this value going into a `long` column.
    assert _to_signed_i64(2**64 - 1) == -1
    assert _to_signed_i64(2**63) == -(2**63)


def test_to_signed_i64_round_trips_through_hamming():
    """The reason the reinterpretation is safe: `dedup.hamming` XORs and masks to 64
    bits rather than comparing magnitudes, so the signed and unsigned forms of the same
    bit pattern must agree."""
    from signal_core.hashing import hamming

    unsigned_a, unsigned_b = (
        simhash64("Northwind acquires Lumen Robotics"),
        simhash64("Lumen Robotics to be bought by Northwind"),
    )
    signed_a, signed_b = _to_signed_i64(unsigned_a), _to_signed_i64(unsigned_b)
    assert hamming(unsigned_a, unsigned_b) == hamming(signed_a, signed_b)

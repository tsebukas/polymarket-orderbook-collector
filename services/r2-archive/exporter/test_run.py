"""Tests for the hourly exporter's streaming path.

Every failure mode covered here is silent. A truncated ClickHouse response
arrives after HTTP 200 has already promised success; a partially written
Parquet file is a valid Parquet file with fewer rows in it; an empty hour that
publishes an empty object looks exactly like an hour that was collected and
happened to be quiet. None of them raise on their own, and all of them produce
data that reads cleanly and is wrong.

Run from this directory:

    pytest

ClickHouse is mocked at the HTTP boundary rather than run, so these are fast
and need no services. The end-to-end check against a real 153M-row hour is a
separate exercise, documented in polym2's docs/collector-selfhost.md.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import run

HOUR = datetime(2026, 8, 5, 16, tzinfo=timezone.utc)
KEY = "polymarket_orderbook_2026-08-05T16.parquet"


# ---------- helpers ----------


def make_batches(total_rows: int, per_batch: int = 100) -> tuple[pa.Schema, list[pa.RecordBatch]]:
    """Build batches shaped like the exporter's SELECT: a delta-encoded integer
    timestamp column and a dictionary-encoded string column."""
    schema = pa.schema(
        [
            pa.field("timestamp_received", pa.int64()),
            pa.field("market", pa.string()),
        ]
    )
    batches = []
    made = 0
    while made < total_rows:
        n = min(per_batch, total_rows - made)
        batches.append(
            pa.RecordBatch.from_arrays(
                [
                    pa.array(range(made, made + n), type=pa.int64()),
                    pa.array([f"0x{i:04d}" for i in range(made, made + n)], type=pa.string()),
                ],
                schema=schema,
            )
        )
        made += n
    return schema, batches


def arrow_stream_bytes(schema: pa.Schema, batches: list[pa.RecordBatch]) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        for b in batches:
            writer.write_batch(b)
    return sink.getvalue().to_pybytes()


class FakeResponse:
    """Stands in for a streaming `requests.Response`.

    Deliberately hands out *small, ragged* chunks. urllib3 promises only "up to
    n" bytes and Arrow demands exactly n, so a fake that always returns the
    full request would hide the bug `_ResponseStream` exists to prevent.
    """

    def __init__(self, data: bytes, chunk: int = 7) -> None:
        self._data = data
        self._chunk = chunk
        self.closed = False

    def iter_content(self, chunk_size: int = 1):
        for i in range(0, len(self._data), self._chunk):
            yield self._data[i : i + self._chunk]

    def raise_for_status(self) -> None:
        pass

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc) -> None:
        self.closed = True


@pytest.fixture
def sink(tmp_path):
    s = run.LocalSink(str(tmp_path))
    s.ensure_bucket()
    return s


def wire(monkeypatch, *, count: int, stream: bytes) -> None:
    """Point the exporter at a fake ClickHouse returning `count` and `stream`."""
    monkeypatch.setattr(run, "query_hour_row_count", lambda hour: count)
    monkeypatch.setattr(run.requests, "post", lambda *a, **k: FakeResponse(stream))


# ---------- the file that must never appear ----------


def test_truncated_stream_leaves_no_file(monkeypatch, sink, tmp_path):
    """ClickHouse says 500 rows, the body stops after 200.

    This is the exact shape of the failure seen in production: HTTP 200 is sent
    before the first batch, so the status line cannot tell us anything. Only
    the row count can.
    """
    schema, batches = make_batches(200)
    wire(monkeypatch, count=500, stream=arrow_stream_bytes(schema, batches))

    with pytest.raises(RuntimeError, match="row count mismatch"):
        run.stream_hour(sink, KEY, HOUR)

    assert list(tmp_path.iterdir()) == [], "a short export must leave nothing behind"


def test_row_count_mismatch_removes_the_partial(monkeypatch, sink, tmp_path):
    """Even one row short must not publish, and must not leave a .part either."""
    schema, batches = make_batches(999)
    wire(monkeypatch, count=1000, stream=arrow_stream_bytes(schema, batches))

    with pytest.raises(RuntimeError):
        run.stream_hour(sink, KEY, HOUR)

    assert not (tmp_path / KEY).exists()
    assert not (tmp_path / (KEY + ".part")).exists()


def test_trailing_bytes_are_rejected(monkeypatch, sink, tmp_path):
    """ClickHouse appends its exception text to a body it already started.

    Arrow stops at the end-of-stream marker and never looks further, so
    whatever follows is invisible to it. The row count would not catch this
    case either — the rows before the failure are all there.
    """
    schema, batches = make_batches(300)
    stream = arrow_stream_bytes(schema, batches) + b"Code: 241. DB::Exception: ..."
    wire(monkeypatch, count=300, stream=stream)

    with pytest.raises(RuntimeError, match="trailing bytes"):
        run.stream_hour(sink, KEY, HOUR)


def test_ragged_chunks_do_not_corrupt_the_read(monkeypatch, sink, tmp_path):
    """Arrow asks for exact byte counts; the transport delivers what it likes.

    Regression for handing the raw response straight to pyarrow, which works
    only as long as every read happens to come back full.
    """
    schema, batches = make_batches(1500, per_batch=53)
    monkeypatch.setattr(run, "query_hour_row_count", lambda hour: 1500)
    monkeypatch.setattr(
        run.requests, "post",
        lambda *a, **k: FakeResponse(arrow_stream_bytes(schema, batches), chunk=3),
    )

    assert run.stream_hour(sink, KEY, HOUR) == 1500
    assert pq.read_table(tmp_path / KEY).num_rows == 1500


def test_empty_hour_writes_nothing(monkeypatch, sink, tmp_path):
    """An hour with no rows returns None and creates no object.

    run_loop relies on this: it must not advance past the hour, so a gap is
    re-polled rather than frozen as an empty file.
    """
    schema, _ = make_batches(0)
    wire(monkeypatch, count=0, stream=arrow_stream_bytes(schema, []))

    assert run.stream_hour(sink, KEY, HOUR) is None
    assert list(tmp_path.iterdir()) == []


def test_export_hour_reports_empty_as_not_exported(monkeypatch, sink):
    monkeypatch.setattr(run, "stream_hour", lambda *a: None)
    assert run.export_hour(sink, HOUR) is False


# ---------- the file that must appear, whole ----------


def test_complete_hour_round_trips(monkeypatch, sink, tmp_path):
    schema, batches = make_batches(2500, per_batch=97)
    wire(monkeypatch, count=2500, stream=arrow_stream_bytes(schema, batches))

    assert run.stream_hour(sink, KEY, HOUR) == 2500

    out = tmp_path / KEY
    assert out.exists()
    table = pq.read_table(out)
    assert table.num_rows == 2500
    assert table.column("timestamp_received").to_pylist()[:3] == [0, 1, 2]
    assert not (tmp_path / (KEY + ".part")).exists()


def test_row_groups_are_bounded(monkeypatch, sink, tmp_path):
    """Peak memory is one row group, so the file must actually contain several.

    If this ever collapses back to a single row group the exporter is holding
    the hour again, which is the bug this whole path exists to prevent.
    """
    monkeypatch.setattr(run, "PARQUET_ROW_GROUP_ROWS", 500)
    schema, batches = make_batches(2000, per_batch=100)
    wire(monkeypatch, count=2000, stream=arrow_stream_bytes(schema, batches))

    run.stream_hour(sink, KEY, HOUR)

    assert pq.ParquetFile(tmp_path / KEY).num_row_groups >= 4


def test_encoding_survives_streaming(monkeypatch, sink, tmp_path):
    """The archive's encoding is applied per row group, not to a whole table.

    Worth pinning: file size is the reason the re-encode exists, and a writer
    opened without these options would still produce a perfectly readable file.
    """
    schema, batches = make_batches(300)
    wire(monkeypatch, count=300, stream=arrow_stream_bytes(schema, batches))

    run.stream_hour(sink, KEY, HOUR)

    meta = pq.ParquetFile(tmp_path / KEY).metadata
    col = meta.row_group(0).column(0)
    assert "ZSTD" in str(col.compression).upper()
    assert any("DELTA_BINARY_PACKED" in str(e) for e in col.encodings)


# ---------- atomicity ----------


def test_final_name_never_holds_partial_content(monkeypatch, sink, tmp_path):
    """The final name appears only via rename, so a reader cannot catch a prefix.

    rsync pulls this directory while the exporter runs; that is the whole
    reason for the .part dance.
    """
    seen: list[list[str]] = []
    real_replace = os.replace

    def watching_replace(src, dst):
        seen.append([p.name for p in tmp_path.iterdir()])
        return real_replace(src, dst)

    monkeypatch.setattr(run.os, "replace", watching_replace)
    schema, batches = make_batches(400)
    wire(monkeypatch, count=400, stream=arrow_stream_bytes(schema, batches))

    run.stream_hour(sink, KEY, HOUR)

    assert seen, "replace was never called"
    assert KEY not in seen[0], "final name existed before the rename"
    assert (KEY + ".part") in seen[0]

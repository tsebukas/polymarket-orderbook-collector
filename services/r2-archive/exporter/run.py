"""Hourly orderbook snapshot exporter.

Reads one hour of orderbook rows from ClickHouse, re-encodes to Parquet
with DELTA_BINARY_PACKED on integer timestamp columns and ZSTD(9)
dictionary encoding elsewhere, and writes it to Cloudflare R2 — or, when
``LOCAL_OUTPUT_DIR`` is set, to that directory instead, in which case no R2
credentials are needed. ClickHouse's own FORMAT Parquet writer never emits
DELTA, so we fetch FORMAT ArrowStream and re-encode client-side (pass 6 in
docs/data-dump-optimizations.md).

The hour is **streamed**, never held: the ArrowStream response is consumed
batch by batch and written a row group at a time, so peak memory is
``PARQUET_ROW_GROUP_ROWS`` and not the size of the export. This matters at
volume — an earlier version buffered the response body, the Arrow table and
the finished Parquet file in turn, and the kernel killed it at 5.5 GB on an
hour of 25.8M rows.

Because ClickHouse returns HTTP 200 before the first batch, a mid-stream
failure looks like a body that simply ends. Every export therefore checks the
rows written against ``SELECT count()`` for the same hour and refuses to
publish on a mismatch; see [`stream_hour`].

Profiles, selected via ``EXPORTER_PROFILE`` env (default ``polymarket``):

* ``polymarket`` — rewrites the raw-JSON ``polymarket_orderbook_rust``
  source table into Schema D in the same SELECT: event-specific fields
  as ``Nullable(...)`` only on owning event types, ``bids`` / ``asks``
  as a single ``Nullable(String)`` holding the raw JSON depth (``NULL``
  outside ``book`` events).

* ``kalshi`` — pass-through ``SELECT *`` from the already-typed
  ``kalshi_orderbook`` table (which natively uses ``Nullable`` columns
  for non-owning event types — no JSON transform needed).

* ``limitless`` — pass-through ``SELECT *`` from the already-typed
  ``limitless_orderbook_rust`` table.

* ``opinion`` — pass-through ``SELECT *`` from the already-typed
  ``opinion_orderbook`` table.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, Iterator

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

log = logging.getLogger(__name__)

# ClickHouse
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "polymarket_orderbook_rust")
CLICKHOUSE_HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"

# Cloudflare R2
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "")

# Local filesystem destination. When set, snapshots are written here and R2
# is not used (and its credentials are not required) — for running the
# collector privately, where the archive is a directory rather than a bucket.
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "")

# Export
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9
# Rows per row group. This is the knob that sets peak memory: the exporter
# holds one group's worth of Arrow batches, not the hour.
#
# 1048576 is not a round-ish guess, it is Arrow's own default
# `max_row_group_length`, which is what the buffered writer was already
# clamping to. Keeping it means the streamed file has the same row-group
# boundaries as every file the archive already holds. Shrinking it costs
# dictionary restarts (a dictionary is built per column per row group) and
# footer growth, not compression itself — zstd works per data page either way.
PARQUET_ROW_GROUP_ROWS = int(os.environ.get("PARQUET_ROW_GROUP_ROWS", "1048576"))
# Bytes pulled from the socket at a time while streaming the response.
STREAM_CHUNK_BYTES = 1 << 20
EXPORT_DELAY_MINUTES = int(os.environ.get("EXPORT_DELAY_MINUTES", "5"))
EXPORT_LAG_HOURS = int(os.environ.get("EXPORT_LAG_HOURS", "1"))
LOOP_CHECK_INTERVAL_SECONDS = int(os.environ.get("LOOP_CHECK_INTERVAL_SECONDS", "60"))
QUERY_MAX_RETRIES = 10
QUERY_RETRY_DELAY_SECONDS = 10

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ---------- Profile ----------


@dataclass(frozen=True)
class Profile:
    """Per-exchange exporter configuration."""

    name: str
    default_filename_prefix: str
    delta_encoded_columns: tuple[str, ...]
    default_select_order_by: tuple[str, ...]
    select_template: str


# Event-type ownership for the polymarket schema-D transform. Event
# types populate only the columns they own; everything else is NULL.
# Mirror of the table in docs/data-dump-optimizations.md.
BOOK_EVENTS = ("book",)
TRADE_LIKE_EVENTS = ("price_change", "last_trade_price")
PRICE_CHANGE_EVENTS = ("price_change",)
LAST_TRADE_EVENTS = ("last_trade_price",)
TICK_SIZE_EVENTS = ("tick_size_change",)


def _in_list(events: tuple[str, ...]) -> str:
    """Render a tuple of event types as a SQL ``IN ('a','b')`` fragment."""
    quoted = ",".join(f"'{e}'" for e in events)
    return f"IN ({quoted})"


POLYMARKET_SELECT_TEMPLATE = f"""
SELECT
    timestamp_received,
    timestamp,
    toFixedString(market, 66)                                      AS market,
    event_type,
    JSONExtractString(data, 'asset_id')                            AS asset_id,

    if(event_type {_in_list(BOOK_EVENTS)},
       JSONExtractString(data, 'bids'), NULL)                      AS bids,
    if(event_type {_in_list(BOOK_EVENTS)},
       JSONExtractString(data, 'asks'), NULL)                      AS asks,

    if(event_type {_in_list(TRADE_LIKE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'price'), 4),
       NULL)                                                       AS price,
    if(event_type {_in_list(TRADE_LIKE_EVENTS)},
       toDecimal64OrZero(JSONExtractString(data, 'size'), 6),
       NULL)                                                       AS size,
    if(event_type {_in_list(TRADE_LIKE_EVENTS)},
       JSONExtractString(data, 'side'), NULL)                      AS side,

    if(event_type {_in_list(PRICE_CHANGE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'best_bid'), 4),
       NULL)                                                       AS best_bid,
    if(event_type {_in_list(PRICE_CHANGE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'best_ask'), 4),
       NULL)                                                       AS best_ask,

    if(event_type {_in_list(LAST_TRADE_EVENTS)},
       toUInt16OrZero(JSONExtractString(data, 'fee_rate_bps')),
       NULL)                                                       AS fee_rate_bps,
    if(event_type {_in_list(LAST_TRADE_EVENTS)},
       JSONExtractString(data, 'transaction_hash'), NULL)          AS transaction_hash,

    if(event_type {_in_list(TICK_SIZE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'old_tick_size'), 4),
       NULL)                                                       AS old_tick_size,
    if(event_type {_in_list(TICK_SIZE_EVENTS)},
       toDecimal32OrZero(JSONExtractString(data, 'new_tick_size'), 4),
       NULL)                                                       AS new_tick_size
FROM {{source_table}}
WHERE timestamp_received >= toDateTime64('{{target}}', 3)
  AND timestamp_received <  toDateTime64('{{target}}', 3) + INTERVAL 1 HOUR
ORDER BY {{order_by}}
FORMAT ArrowStream
"""

PASSTHROUGH_SELECT_TEMPLATE = """
SELECT * FROM {source_table}
WHERE timestamp_received >= toDateTime64('{target}', 3)
  AND timestamp_received <  toDateTime64('{target}', 3) + INTERVAL 1 HOUR
ORDER BY {order_by}
FORMAT ArrowStream
"""

PROFILES: dict[str, Profile] = {
    "polymarket": Profile(
        name="polymarket",
        default_filename_prefix="polymarket_orderbook_",
        delta_encoded_columns=("timestamp", "timestamp_received", "fee_rate_bps"),
        default_select_order_by=("market", "asset_id", "timestamp_received"),
        select_template=POLYMARKET_SELECT_TEMPLATE,
    ),
    "kalshi": Profile(
        name="kalshi",
        default_filename_prefix="kalshi_orderbook_",
        delta_encoded_columns=("timestamp", "timestamp_received"),
        default_select_order_by=("market_ticker", "timestamp_received"),
        select_template=PASSTHROUGH_SELECT_TEMPLATE,
    ),
    "limitless": Profile(
        name="limitless",
        default_filename_prefix="limitless_orderbook_",
        delta_encoded_columns=(
            "timestamp",
            "timestamp_received",
            "fee_rate_bps",
            "receive_sequence",
            "row_index",
        ),
        default_select_order_by=(
            "market",
            "asset_id",
            "timestamp_received",
            "receive_sequence",
            "row_index",
        ),
        select_template=PASSTHROUGH_SELECT_TEMPLATE,
    ),
    "opinion": Profile(
        name="opinion",
        default_filename_prefix="opinion_orderbook_",
        delta_encoded_columns=(
            "timestamp",
            "timestamp_received",
            "receive_sequence",
            "row_index",
        ),
        default_select_order_by=(
            "market",
            "asset_id",
            "timestamp_received",
            "receive_sequence",
            "row_index",
        ),
        select_template=PASSTHROUGH_SELECT_TEMPLATE,
    ),
}

EXPORTER_PROFILE = os.environ.get("EXPORTER_PROFILE", "polymarket")
if EXPORTER_PROFILE not in PROFILES:
    raise SystemExit(
        f"Unknown EXPORTER_PROFILE={EXPORTER_PROFILE!r}; "
        f"expected one of {sorted(PROFILES)}"
    )
PROFILE = PROFILES[EXPORTER_PROFILE]

FILENAME_PREFIX = os.environ.get("FILENAME_PREFIX", PROFILE.default_filename_prefix)


def _env_column_list(name: str, default: tuple[str, ...]) -> list[str]:
    """Read a JSON array env var containing ClickHouse column identifiers."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return list(default)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{name} must be a JSON array of column names") from e

    if not isinstance(parsed, list) or not parsed:
        raise ValueError(f"{name} must be a non-empty JSON array of column names")

    columns: list[str] = []
    for value in parsed:
        if not isinstance(value, str):
            raise ValueError(f"{name} must contain only string column names")
        column = value.strip()
        if not IDENTIFIER_RE.fullmatch(column):
            raise ValueError(f"{name} contains invalid column identifier: {value!r}")
        columns.append(column)

    return columns


SELECT_ORDER_BY = _env_column_list("SELECT_ORDER_BY", PROFILE.default_select_order_by)


# ---------- ClickHouse ----------


def _ch_query(query: str, timeout: int = 60) -> requests.Response:
    """POST a query to ClickHouse and return the response."""
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
    resp = requests.post(CLICKHOUSE_HTTP_URL, data=query.encode(), auth=auth, timeout=timeout)
    resp.raise_for_status()
    return resp


def query_earliest_hour() -> datetime | None:
    """Return the earliest hour with data, or None if the table is empty.

    The table is partitioned by ``toStartOfHour(timestamp_received)``, so
    ``min(timestamp_received)`` is O(1) via partition pruning.
    """
    for attempt in range(1, QUERY_MAX_RETRIES + 1):
        try:
            resp = _ch_query(
                f"SELECT toStartOfHour(min(timestamp_received)) FROM {CLICKHOUSE_TABLE} "
                "FORMAT TabSeparated"
            )
            text = resp.text.strip()
            if not text or text.startswith("1970"):
                return None
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception as e:
            if attempt == QUERY_MAX_RETRIES:
                raise
            log.warning(
                "ClickHouse query failed (attempt %d/%d): %s — retrying in %ds",
                attempt, QUERY_MAX_RETRIES, e, QUERY_RETRY_DELAY_SECONDS,
            )
            time.sleep(QUERY_RETRY_DELAY_SECONDS)
    return None


def query_hour_row_count(hour: datetime) -> int:
    """Count the rows ClickHouse holds for one hour.

    Read before the export and compared against what was actually written, so
    a short read cannot pass as a complete file. See [`stream_hour`].
    """
    target = hour.strftime("%Y-%m-%d %H:00:00")
    resp = _ch_query(
        f"SELECT count() FROM {CLICKHOUSE_TABLE} "
        f"WHERE timestamp_received >= toDateTime64('{target}', 3) "
        f"AND timestamp_received < toDateTime64('{target}', 3) + INTERVAL 1 HOUR "
        "FORMAT TabSeparated",
        timeout=120,
    )
    return int(resp.text.strip())


class _ResponseStream:
    """Exact-read file adapter over a streaming HTTP response.

    Arrow's IPC reader asks for N bytes and treats a short return as a corrupt
    stream — it calls ``read`` once and does not loop. ``urllib3`` promises
    only *up to* N, so handing it ``resp.raw`` works by luck of the common path
    rather than by contract, and stops working the moment a transfer encoding
    is involved. This loops until it has N bytes or the body genuinely ends.

    Reading through ``iter_content`` also means a mid-transfer failure surfaces
    as a ``requests`` exception rather than a urllib3 error leaking through the
    Arrow layer.
    """

    def __init__(self, resp: requests.Response, chunk_size: int = STREAM_CHUNK_BYTES) -> None:
        self._chunks = resp.iter_content(chunk_size)
        self._buf = bytearray()
        self._pos = 0
        self.closed = False

    def _pull(self) -> bool:
        """Append one chunk. False once the body is exhausted."""
        for chunk in self._chunks:
            if chunk:
                self._buf += chunk
                return True
        return False

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            while self._pull():
                pass
            n = len(self._buf)
        while len(self._buf) < n:
            if not self._pull():
                break
        out = bytes(self._buf[:n])
        del self._buf[:n]
        self._pos += len(out)
        return out

    def assert_exhausted(self) -> None:
        """Raise if anything follows what Arrow consumed.

        Arrow stops at the end-of-stream marker without looking further. If the
        server appended an error after starting a successful body — which
        ClickHouse does — or if the connection died, the remainder shows up
        here or as a ``ChunkedEncodingError`` from ``iter_content``.
        """
        if self._buf or self._pull():
            raise RuntimeError(
                "trailing bytes after the Arrow stream ended — "
                "the response was not what it claimed to be"
            )

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def close(self) -> None:
        self.closed = True


def _hour_query(hour: datetime) -> str:
    """Build the profile's SELECT for one hour."""
    return PROFILE.select_template.format(
        source_table=CLICKHOUSE_TABLE,
        target=hour.strftime("%Y-%m-%d %H:00:00"),
        order_by=", ".join(SELECT_ORDER_BY),
    )


def _parquet_writer(sink: BinaryIO, schema: pa.Schema) -> pq.ParquetWriter:
    """Open a writer with the archive's encoding, derived from the stream's schema.

    Same encoding the whole archive was built with: DELTA_BINARY_PACKED on the
    integer timestamp columns, ZSTD(9) dictionary elsewhere. ClickHouse's own
    FORMAT Parquet never emits DELTA, which is why the re-encode exists at all.
    """
    delta_cols = [c for c in PROFILE.delta_encoded_columns if c in schema.names]
    dict_cols = [c for c in schema.names if c not in delta_cols]
    return pq.ParquetWriter(
        sink,
        schema,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        use_dictionary=dict_cols,
        column_encoding={c: "DELTA_BINARY_PACKED" for c in delta_cols},
        data_page_version="2.0",
    )


def stream_hour(client: Destination, key: str, hour: datetime) -> int | None:
    """Stream one hour from ClickHouse straight into the destination.

    Returns the number of rows written, or ``None`` for an empty hour (no file
    is created, and the caller keeps polling).

    Nothing here holds the hour. ClickHouse's ArrowStream response is consumed
    batch by batch and handed to a [`pq.ParquetWriter`] a row group at a time,
    so peak memory is [`PARQUET_ROW_GROUP_ROWS`] rather than the whole export.
    The previous version read the response body, the Arrow table and the
    finished Parquet file into memory in turn, and was killed by the kernel at
    5.5 GB on a 25.8M-row hour.

    **A truncated read must never become a short file.** ClickHouse sends
    HTTP 200 before the first batch, so a mid-stream failure arrives as a body
    that simply stops; the status line has already promised success. That is
    why the row count is checked against [`query_hour_row_count`] and a
    mismatch raises: the destination's context manager then discards the
    partial file rather than publishing it.
    """
    expected = query_hour_row_count(hour)
    if expected == 0:
        return None

    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None
    written = 0
    with requests.post(
        CLICKHOUSE_HTTP_URL,
        data=_hour_query(hour).encode(),
        auth=auth,
        # Per socket read, not total. The first read blocks for the whole
        # server-side sort, which is minutes on a full hour.
        timeout=(30, 3600),
        stream=True,
        # No compression layer to unwrap: ClickHouse only compresses when the
        # client asks, and one header removes a whole class of framing bug.
        headers={"Accept-Encoding": "identity"},
    ) as resp:
        resp.raise_for_status()
        body = _ResponseStream(resp)

        with pa.ipc.open_stream(body) as reader:
            schema = reader.schema
            with client.open_write(key) as sink:
                writer = _parquet_writer(sink, schema)
                try:
                    pending: list[pa.RecordBatch] = []
                    pending_rows = 0
                    for batch in reader:
                        if batch.num_rows == 0:
                            continue
                        pending.append(batch)
                        pending_rows += batch.num_rows
                        # Slice to exactly the target. Flushing "at least N"
                        # would hand the writer N plus a partial batch, which
                        # it splits into a full group and a runt — one small
                        # row group per flush, which is the layout this whole
                        # loop exists to avoid.
                        while pending_rows >= PARQUET_ROW_GROUP_ROWS:
                            table = pa.Table.from_batches(pending, schema)
                            writer.write_table(
                                table.slice(0, PARQUET_ROW_GROUP_ROWS),
                                row_group_size=PARQUET_ROW_GROUP_ROWS,
                            )
                            written += PARQUET_ROW_GROUP_ROWS
                            rest = table.slice(PARQUET_ROW_GROUP_ROWS)
                            pending, pending_rows = rest.to_batches(), rest.num_rows
                    if pending_rows:
                        writer.write_table(
                            pa.Table.from_batches(pending, schema),
                            row_group_size=PARQUET_ROW_GROUP_ROWS,
                        )
                        written += pending_rows
                finally:
                    # Closes even while an exception propagates, so a truncated
                    # stream still gets a valid footer written over short data.
                    # That file is only harmless because open_write discards on
                    # exception — do not move the commit into a finally.
                    writer.close()

                if written != expected:
                    raise RuntimeError(
                        f"row count mismatch for {key}: ClickHouse reported {expected}, "
                        f"wrote {written} — refusing to publish a partial file"
                    )

        body.assert_exhausted()

    return written


# ---------- R2 ----------


class R2Client:
    """Thin S3-compatible client for Cloudflare R2."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not yet exist."""
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                self._client.create_bucket(Bucket=self._bucket)
                log.info("Created bucket %s", self._bucket)
            else:
                raise

    def list_keys(self) -> set[str]:
        """Return the set of exported parquet object keys."""
        keys: set[str] = set()
        kwargs: dict = {"Bucket": self._bucket, "Prefix": FILENAME_PREFIX}
        while True:
            resp = self._client.list_objects_v2(**kwargs)
            keys.update(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                return keys
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]

    @contextmanager
    def open_write(self, key: str) -> Iterator[BinaryIO]:
        """Yield a file to write the object into, uploading it on clean exit.

        Staged through a temp file rather than memory, because the caller
        streams an hour that does not fit. Nothing is uploaded if the body
        raises, so a failed export leaves no object behind.
        """
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        try:
            with tmp:
                yield tmp
            self._client.upload_file(tmp.name, self._bucket, key)
        finally:
            os.unlink(tmp.name)


# ---------- Local filesystem ----------


class LocalSink:
    """Filesystem stand-in for [`R2Client`], selected by ``LOCAL_OUTPUT_DIR``.

    Same three-method surface, so ``backfill`` and ``run_loop`` do not care
    which destination they were handed. Writes go through a ``.part`` file and
    an ``os.replace``, so a reader — or an rsync pulling the directory while
    the exporter runs — never observes a half-written snapshot.
    """

    def __init__(self, directory: str) -> None:
        self._dir = Path(directory)

    def ensure_bucket(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        log.info("Writing snapshots to %s", self._dir)
        # A killed process leaves its .part behind: open_write cleans up on an
        # exception, but nothing runs on SIGKILL, and this exporter has been
        # OOM-killed mid-write before. list_keys already ignores them, so these
        # are wasted disk rather than a correctness problem — but on a full
        # hour that is gigabytes of it.
        for stale in self._dir.glob(f"{FILENAME_PREFIX}*.parquet.part"):
            log.warning("Removing stale partial export %s", stale.name)
            stale.unlink(missing_ok=True)

    def list_keys(self) -> set[str]:
        return {p.name for p in self._dir.glob(f"{FILENAME_PREFIX}*.parquet")}

    @contextmanager
    def open_write(self, key: str) -> Iterator[BinaryIO]:
        """Yield the ``.part`` file, renaming it into place on clean exit.

        The rename is the whole point: a reader — or an rsync pulling this
        directory while the exporter runs — either sees the finished snapshot
        or nothing at all, never a prefix of one. If the body raises, the
        partial file is removed and the final name is never created, which is
        what makes a failed or short export safe rather than silently wrong.
        """
        final = self._dir / key
        partial = final.with_suffix(final.suffix + ".part")
        try:
            with open(partial, "wb") as fh:
                yield fh
            os.replace(partial, final)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise


# Either destination satisfies the ensure_bucket / list_keys / open_write
# surface that the orchestration below uses; nothing there cares which one it
# holds.
Destination = R2Client | LocalSink


# ---------- Export orchestration ----------


def hour_to_filename(hour: datetime) -> str:
    """Convert a datetime to the standard snapshot filename."""
    return f"{FILENAME_PREFIX}{hour.strftime('%Y-%m-%dT%H')}.parquet"


def latest_exportable_hour() -> datetime:
    """Return the latest hour that is fully complete.

    Computed as ``current_hour - EXPORT_LAG_HOURS`` (default 1 hour).
    """
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now - timedelta(hours=EXPORT_LAG_HOURS)


def export_hour(client: Destination, hour: datetime) -> bool:
    """Stream one hour from ClickHouse into the destination.

    Returns True if an object was written, False if the hour has no rows
    (the caller should keep polling until data appears).

    The elapsed time is logged deliberately. The export has one hour to write
    an hour, and if it ever stops fitting the exporter falls behind for good.
    """
    filename = hour_to_filename(hour)
    log.info("Exporting %s", filename)
    started = time.monotonic()
    rows = stream_hour(client, filename, hour)
    if rows is None:
        log.info("Skipping %s: 0 rows, will retry next tick", filename)
        return False
    log.info("Wrote %s (%d rows, %.1fs)", filename, rows, time.monotonic() - started)
    return True


def backfill(client: Destination) -> None:
    """Export every missing hour from ClickHouse to R2."""
    earliest = query_earliest_hour()
    if earliest is None:
        log.warning("No data in ClickHouse yet")
        return

    latest = latest_exportable_hour()
    existing = client.list_keys()

    missing: list[datetime] = []
    current = earliest
    while current <= latest:
        if hour_to_filename(current) not in existing:
            missing.append(current)
        current += timedelta(hours=1)

    log.info(
        "Backfill: %d missing hours (%s to %s), %d already exported",
        len(missing), earliest.isoformat(), latest.isoformat(), len(existing),
    )

    for i, hour in enumerate(missing, 1):
        try:
            export_hour(client, hour)
            log.info("Backfill progress: %d/%d", i, len(missing))
        except Exception as e:
            log.error("Failed to export %s: %s", hour.isoformat(), e)


def run_loop(client: Destination) -> None:
    """Steady-state loop: export each new hour shortly after it completes.

    Advances only on successful upload — empty hours are re-polled on the
    next tick so gaps never produce zero-row objects in R2.
    """
    log.info("Entering steady-state loop (check every %ds)", LOOP_CHECK_INTERVAL_SECONDS)
    next_hour = latest_exportable_hour() + timedelta(hours=1)

    while True:
        time.sleep(LOOP_CHECK_INTERVAL_SECONDS)
        now = datetime.now(timezone.utc)
        latest = latest_exportable_hour()
        if now.minute < EXPORT_DELAY_MINUTES:
            continue
        while next_hour <= latest:
            try:
                if not export_hour(client, next_hour):
                    break
                next_hour += timedelta(hours=1)
            except Exception as e:
                log.error("Failed to export %s: %s", next_hour.isoformat(), e)
                break


def main() -> None:
    """Start the R2 snapshot exporter."""
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    client: Destination
    if LOCAL_OUTPUT_DIR:
        client = LocalSink(LOCAL_OUTPUT_DIR)
        client.ensure_bucket()
        log.info(
            "Exporting to %s, profile=%s, source_table=%s, filename_prefix=%s, order_by=%s",
            LOCAL_OUTPUT_DIR, PROFILE.name, CLICKHOUSE_TABLE, FILENAME_PREFIX, SELECT_ORDER_BY,
        )
    else:
        missing = [v for v in ("R2_ENDPOINT", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_BUCKET")
                   if not globals()[v]]
        if missing:
            log.error(
                "Missing required environment variables: %s "
                "(or set LOCAL_OUTPUT_DIR to export to the filesystem instead)",
                ", ".join(missing),
            )
            sys.exit(1)

        client = R2Client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET)
        client.ensure_bucket()
        log.info(
            "Connected to R2 at %s, bucket=%s, profile=%s, source_table=%s, filename_prefix=%s, order_by=%s",
            R2_ENDPOINT, R2_BUCKET, PROFILE.name, CLICKHOUSE_TABLE, FILENAME_PREFIX, SELECT_ORDER_BY,
        )

    backfill(client)
    run_loop(client)


if __name__ == "__main__":
    main()

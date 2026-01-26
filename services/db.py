from typing import Callable
import threading

import polars as pl
from pymongo import MongoClient

from models.jobs import JobConfig

Logger = Callable[[str], None]


class ProcessingState:
    """Thread-safe container for tracking processed rows during CSV streaming."""

    def __init__(self):
        self.lock = threading.Lock()
        self.rows_processed = 0

    def add_rows(self, count: int) -> None:
        """Add rows to the counter (thread-safe)."""
        with self.lock:
            self.rows_processed += count

    def get_and_reset(self) -> int:
        """Get delta of processed rows and reset counter (thread-safe)."""
        with self.lock:
            delta = self.rows_processed
            self.rows_processed = 0
            return delta


def process_csv_stream_to_mongo(
    stream_obj,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    batch_size: int = 5000,
    block_size_bytes: int = 16 * 1024 * 1024,
    client_id: int | None = None,
    period: str | None = None,
    log: Logger | None = None,
    config: JobConfig | None = None,
    processing_state: ProcessingState | None = None,
):
    """Read CSV in streaming record batches and insert into Mongo in batches.

    Uses Polars for efficient CSV parsing with full configuration support:
    - Custom delimiters, encodings, column names, and data types
    - Skip rows functionality
    - Encoding error handling
    - Dynamic batch sizing

    Optionally adds client_id (as 'ctr') and period (as 'crx') to each document.
    """
    client = MongoClient(mongo_uri)
    collection = client[mongo_db][mongo_collection]

    # Parse configuration
    delimiter = config.delimitador if config and config.delimitador else ","
    encoding = config.codec if config and config.codec else "utf-8"
    skip_rows = config.skiprows if config and config.skiprows else 0
    column_names = config.campos if config and config.campos else None
    encoding_errors = config.merror if config and config.merror else "strict"

    # Determine chunk size from config.size or use batch_size
    chunk_size = batch_size
    if config and config.size:
        try:
            chunk_size = int(config.size)
        except (ValueError, TypeError):
            if log:
                log(f"Invalid size '{config.size}', using default {batch_size}")

    # Build Polars schema from reglas (rules)
    schema_overrides = None
    if config and config.reglas:
        schema_overrides = _convert_pandas_dtypes_to_polars(config.reglas)
        if log and schema_overrides:
            log(f"Applied schema overrides: {list(schema_overrides.keys())}")

    # Configure Polars reader
    has_header = skip_rows == 0 and column_names is None

    try:
        # Read CSV with Polars in batches
        df_reader = pl.read_csv_batched(
            stream_obj,
            separator=delimiter,
            has_header=has_header,
            new_columns=column_names,
            skip_rows=skip_rows,
            encoding=encoding,
            ignore_errors=(encoding_errors != "strict"),
            schema_overrides=schema_overrides,
            infer_schema_length=10000,
            batch_size=chunk_size,
            low_memory=False,
        )

        total_rows = 0
        chunk_number = 0

        # Process each batch
        batches = df_reader.next_batches(10)  # Get up to 10 batches at a time
        while batches:
            for df in batches:
                chunk_number += 1

                # Add metadata fields
                if client_id is not None:
                    df = df.with_columns(pl.lit(client_id).alias("ctr"))
                if period is not None:
                    # Convert period to int if it's numeric
                    try:
                        period_value = int(period)
                    except (ValueError, TypeError):
                        period_value = period
                    df = df.with_columns(pl.lit(period_value).alias("crx"))

                # Convert to dictionaries and insert
                docs = df.to_dicts()
                rows_in_batch = len(docs)

                if docs:
                    collection.bulk_write(docs, ordered=False)
                    total_rows += rows_in_batch

                    # Update shared processing state if provided
                    if processing_state:
                        processing_state.add_rows(rows_in_batch)

                    if log and (chunk_number % 10 == 0 or chunk_number == 1):
                        log(
                            f"Chunk {chunk_number}: inserted {rows_in_batch} rows (total: {total_rows})"
                        )

            batches = df_reader.next_batches(10)

        if log:
            log(
                f"✓ Completed: {total_rows} total rows inserted in {chunk_number} chunks"
            )

    except Exception as e:
        if log:
            log(f"Error during CSV processing: {str(e)}")
        raise Exception(f"Error during CSV processing: {str(e)}")
    finally:
        client.close()


def _convert_pandas_dtypes_to_polars(
    pandas_dtypes: dict[str, str],
) -> dict[str, pl.DataType]:
    """Convert pandas dtype strings to Polars data types.

    Handles common pandas dtype specifications:
    - 'str', 'object' -> pl.Utf8
    - 'int', 'int64', 'Int64' -> pl.Int64
    - 'int32', 'Int32' -> pl.Int32
    - 'float', 'float64' -> pl.Float64
    - 'float32' -> pl.Float32
    - 'bool' -> pl.Boolean
    - 'datetime64[ns]' -> pl.Datetime
    """
    type_mapping = {
        "str": pl.Utf8,
        "string": pl.Utf8,
        "object": pl.Utf8,
        "int": pl.Int64,
        "int64": pl.Int64,
        "Int64": pl.Int64,
        "int32": pl.Int32,
        "Int32": pl.Int32,
        "int16": pl.Int16,
        "Int16": pl.Int16,
        "int8": pl.Int8,
        "Int8": pl.Int8,
        "float": pl.Float64,
        "float64": pl.Float64,
        "float32": pl.Float32,
        "bool": pl.Boolean,
        "boolean": pl.Boolean,
        "datetime64": pl.Datetime("ms"),
        "datetime64[ns]": pl.Datetime("ns"),
    }

    result = {}
    for col, dtype_str in pandas_dtypes.items():
        dtype_str_lower = dtype_str.lower().strip()
        if dtype_str_lower in type_mapping:
            result[col] = type_mapping[dtype_str_lower]
        elif dtype_str in type_mapping:
            result[col] = type_mapping[dtype_str]

    return result

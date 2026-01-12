from typing import Callable

import pyarrow as pa
import pyarrow.csv as pacsv
import polars as pl
from pymongo import MongoClient

Logger = Callable[[str], None]


def process_csv_stream_to_mongo(
    stream_obj,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    batch_size: int = 5000,
    block_size_bytes: int = 16 * 1024 * 1024,
    log: Logger | None = None,
):
    """Read CSV in streaming record batches and insert into Mongo in batches."""
    client = MongoClient(mongo_uri)
    collection = client[mongo_db][mongo_collection]

    read_opts = pacsv.ReadOptions(block_size=block_size_bytes)
    convert_opts = pacsv.ConvertOptions()

    arrow_stream = pa.input_stream(stream_obj)
    reader = pacsv.open_csv(
        arrow_stream, read_options=read_opts, convert_options=convert_opts
    )

    total_rows = 0
    buffer_docs = []

    for record_batch in reader:
        table = pa.Table.from_batches([record_batch])
        df = pl.from_arrow(table)
        docs = df.to_dicts()
        buffer_docs.extend(docs)

        if len(buffer_docs) >= batch_size:
            collection.insert_many(buffer_docs, ordered=False)
            total_rows += len(buffer_docs)
            if log:
                log(f"Inserted {total_rows} rows so far...")
            buffer_docs = []

    if buffer_docs:
        collection.insert_many(buffer_docs)
        total_rows += len(buffer_docs)

    if log:
        log(f"Finished inserting. Total rows: {total_rows}")

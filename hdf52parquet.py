import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def convert_hdf5_to_parquet(h5_file, parquet_file, chunksize=100000):

    stream = pd.read_hdf(h5_file, chunksize=chunksize)

    for i, chunk in enumerate(stream):
        print("Chunk {}".format(i))

        if i == 0:
            # Infer schema and open parquet file on first chunk
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')

        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)

    parquet_writer.close()


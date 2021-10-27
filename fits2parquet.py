import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table
import parsl
from condor import get_config
import glob
import os
from parsl import python_app


def convert_fits_to_parquet(fits_files, parquet_dir):

    config = get_config('htcondor', '/home/singulani/projects/axs/env.sh')
    parsl.load(config)

    procs = list()

    for fits in glob.glob(fits_files):
        procs.append(create(fits, parquet_dir))

    for proc in procs:
        proc.result()

    parsl.clear()

@python_app
def create(fits, parquet_dir):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from astropy.table import Table
    import os

    dat = Table.read(fits, format='fits')
    dff = dat.to_pandas()

    parquet_file = f'{parquet_dir}/{os.path.basename(fits).replace(".fits", ".parquet")}'

    table = pa.Table.from_pandas(df=dff)
    pq.write_table(table, parquet_file)


convert_fits_to_parquet('/lustre/t1/tmp/Y6A2_GOLD/cats/*.fits', '/lustre/t1/tmp/Y6A2_GOLD_PARQUET')

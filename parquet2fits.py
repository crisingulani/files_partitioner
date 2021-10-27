import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table
import parsl
from condor import get_config
import glob
import os
from parsl import python_app


def convert_parquet_to_fits(parquet_files, fits_dir):
    config = get_config('htcondor', '/home/singulani/projects/axs/env.sh')
    parsl.load(config)

    procs = list()

    for fl in glob.glob(parquet_files):
        procs.append(create(fl, fits_dir))

    for proc in procs:
        proc.result()

    parsl.clear()

@python_app
def create(fl, fits_dir):
    import pyarrow.parquet as pq
    from astropy.table import Table

    fits = f'{fits_dir}/{os.path.basename(fl).replace(".parquet", ".fits")}'

    table = pq.read_table(fl)
    tp = table.to_pandas()
    del(table)
    tn = tp.to_numpy()
    ta = Table(tn, names=tp.columns, dtype=tp.dtypes)
    del(tp)
    del(tn)

    for col in ta.itercols():
        if col.dtype.kind == 'O':
            ta[col.name] = ta.field(col.name).astype('unicode')

    ta.write(fits)
    del(ta)


convert_parquet_to_fits('/lustre/t1/tmp/Y6A2_GOLD_PARQUET/BALANCED/*.parquet', '/lustre/t1/tmp/Y6A2_GOLD_FITS/BALANCED/')

# Specific file for balanced partitioning of the DES Y6A2 GOLD
# Number of partitions: 26
# Format: parquet

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName(
    'Y6A2.FirstPartitioning'
).getOrCreate()

start = datetime.datetime.now()
df = spark.read.parquet(
    'file:///lustre/t1/tmp/Y6A2_GOLD_PARQUET/TILENAME/*.parquet'
)
df.select(
    col('COADD_OBJECT_ID').alias('COADD_OBJECTS_ID'),
    'RA',
    'DEC',
    'TILENAME',
    col('EBV_SFD98').alias('EBV'),
    'HPIX_32',
    'HPIX_64',
    'HPIX_1024',
    'HPIX_4096',
    'HPIX_16384',
    col('BDF_MAG_ERR_G').alias('SOF_BDF_MAG_ERR_G'),
    col('BDF_MAG_ERR_I').alias('SOF_BDF_MAG_ERR_I'),
    col('BDF_MAG_ERR_R').alias('SOF_BDF_MAG_ERR_R'),
    col('BDF_MAG_ERR_Y').alias('SOF_BDF_MAG_ERR_Y'),
    col('BDF_MAG_ERR_Z').alias('SOF_BDF_MAG_ERR_Z'),
    col('BDF_MAG_G_CORRECTED').alias('SOF_BDF_MAG_G_CORRECTED'),
    col('BDF_MAG_I_CORRECTED').alias('SOF_BDF_MAG_I_CORRECTED'),
    col('BDF_MAG_R_CORRECTED').alias('SOF_BDF_MAG_R_CORRECTED'),
    col('BDF_MAG_Y_CORRECTED').alias('SOF_BDF_MAG_Y_CORRECTED'),
    col('BDF_MAG_Z_CORRECTED').alias('SOF_BDF_MAG_Z_CORRECTED'),
    col('MAGERR_AUTO_G'),
    col('MAGERR_AUTO_I'),
    col('MAGERR_AUTO_R'),
    col('MAGERR_AUTO_Y'),
    col('MAGERR_AUTO_Z'),
    col('MAG_AUTO_G'),
    col('MAG_AUTO_I'),
    col('MAG_AUTO_R'),
    col('MAG_AUTO_Y'),
    col('MAG_AUTO_Z'),
    'SPREADERR_MODEL_I',
    'SPREADERR_MODEL_R',
    'SPREADERR_MODEL_Z',
    'SPREAD_MODEL_I',
    'SPREAD_MODEL_R',
    'SPREAD_MODEL_Z',
    'WAVG_SPREAD_MODEL_I',
    'WAVG_SPREAD_MODEL_R',
    'WAVG_SPREAD_MODEL_Z'
).repartition(26).write.format('delta').save(
    'file:///lustre/t1/tmp/Y6A2_GOLD_PARQUET/BALANCED/'
)
print('-> runtime: {}'.format(str(datetime.datetime.now() - start)))

spark.stop()

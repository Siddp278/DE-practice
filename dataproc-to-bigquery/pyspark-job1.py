from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import sys

spark = SparkSession.builder.appName("practice").getOrCreate()

dy = spark.read.csv(
    sys.argv[1],
    inferSchema=True,
    header=True
)

dy_subset = dy.select(['ACCT_ID', 'ALPHA', 'OWNER_NAME', 
                      'LAST_FULL_INSP_DT', 'BIN', 'LATITUDE','LONGITUDE',
                        'POSTCODE', 'Street', 'Census Tract', 'NTA'])

dy_subset = dy_subset.na.drop(how = "any", subset=['POSTCODE', 'Street', 
                              'ALPHA', 'LATITUDE', 'LONGITUDE'])

dy_subset = dy_subset.na.fill("placeholder owner name", "OWNER_NAME")
dy_subset = dy_subset.na.fill("12/08/2024", 'LAST_FULL_INSP_DT')

dy_subset.select(to_timestamp(dy_subset['LAST_FULL_INSP_DT']
    , 'yyyy-MM-dd').alias('dt'))

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "data-storage-unique"
spark.conf.set('temporaryGcsBucket', bucket)

# Save the data to BigQuery
dy_subset.write.format('bigquery') \
  .option('table', sys.argv[2] + ".fdny_table") \
  .save()
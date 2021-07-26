# to access bigquery's table data into pyspark dataframe  

from google.cloud import bigquery
from pyspark.sql import SparkSession

spark= SparkSession.builder.master('local[*]').appName('Test_bg').getOrCreate()
bq_client = bigquery.Client()
raw_data =  bq_client.query("SELECT  * FROM `tokyo-comfort-320706.Orcl_dataset.dept` LIMIT 1000")
df  = spark.createDataFrame(raw_data.result().to_dataframe())
df.show(1000, truncate=False)


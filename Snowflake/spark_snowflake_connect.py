from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('DF_with_Schema').getOrCreate()
df = spark.read.format("net.snowflake.spark.snowflake") \
    .option("sfurl", "") \
    .option("sfAccount", "")\
    .option("sfUser", "")\
    .option("sfPassword", "")\
    .option("sfRole", "sysadmin")\
    .option("sfDatabase", "CITIBIKE")\
    .option("sfSchema", "public").option("query", "select * from citibike.public.trips limit 100").load()
df.show(1000, truncate=False)

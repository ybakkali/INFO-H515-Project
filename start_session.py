from pyspark.sql import SparkSession

SPARK = SparkSession.builder.appName("senpais").getOrCreate()

# -*- coding: utf-8 -*-




import os
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType




pfad = os.path.abspath(".")
dateipfad = pfad +  "\\2_curated\\daten\\"

datei_OSM = "germany_20180915_nodes.osm"

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("osm daten laden und in parquet konvertieren")\
    .getOrCreate()

osm_nodes = spark.read.format("xml").option("rootTag", "osm").load(dateipfad)

osm_nodes.printSchema()
osm_nodes.show()
osm_nodes.count()

osm_nodes.write.parquet(dateipfad + "germany_20180915_nodes.parquet")



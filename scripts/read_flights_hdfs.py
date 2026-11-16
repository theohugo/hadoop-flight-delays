#!/usr/bin/env python3
"""Lecture de contrôle utilisée pendant la panne d'un DataNode."""
import json
from pathlib import Path

from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[2]").appName("J1_crash_flights")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
try:
    manifest = json.loads(Path("/workspace/reports/data_manifest.json").read_text())
    df = spark.read.option("header", True).csv("/lakehouse/bronze/raw/flight-delays/flights.csv")
    count = df.count()
    assert count == manifest["flight_rows"], (count, manifest["flight_rows"])
    result = {"rows": count, "spark_partitions": df.rdd.getNumPartitions(),
              "spark_version": spark.version}
    print("FLIGHTS_READ_OK=" + json.dumps(result), flush=True)
finally:
    spark.stop()

package com.tfedorov.icebergdemo

import org.apache.spark.sql.SparkSession

object ReadParquet extends App{

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


  spark.read.parquet("/Users/tfedorov/IdeaProjects/IcebergDemo/src/main/resources/output_iceberg/catalog/harry_ns/input_table/data/Gender=null/00042-202-bb722e8c-3c45-4168-a033-d466bc01262c-00001.parquet").show
}

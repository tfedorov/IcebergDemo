package com.tfedorov.icebergdemo

import org.apache.spark.sql.SparkSession

object SparkLocalExtractor {
//  val ICEBERG_TEST_PATH = "src/test/resources/iceberg/"
  val WAREHOUSE_DIR_PATH: String = "/Users/tfedorov/IdeaProjects/IcebergDemo/src/test/resources/iceberg/"
  val SPARK_CATALOG_PATH: String = WAREHOUSE_DIR_PATH + "spark_catalog/"

  lazy val icebergSpark: SparkSession = SparkSession
    .builder()
    .config("spark.driver.host", "localhost")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR_PATH)
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", SPARK_CATALOG_PATH)
    .config("spark.sql.catalog.iceberg_test", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    // Iceberg configs
    .master("local[*]")
    .getOrCreate()

}

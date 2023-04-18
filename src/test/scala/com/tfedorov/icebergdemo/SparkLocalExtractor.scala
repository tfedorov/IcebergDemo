package com.tfedorov.icebergdemo

//import org.apache.spark.sql.SparkSession

object SparkLocalExtractor {
//  val ADT_TEST_PATH = "src/test/resources/adt/"
//
//  lazy val localSession: SparkSession = {
//    val session = SparkSession
//      .builder()
//      .config("spark.driver.host", "localhost")
//      .config("spark.sql.warehouse.dir", "src/test/resources/warehouse/dir")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
//      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
//      .config("spark.sql.catalog.spark_catalog.warehouse", "src/test/resources/spark_catalog/dir")
//      .config("spark.sql.catalog.iceberg_test", "org.apache.iceberg.spark.SparkCatalog")
//      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
//
//      // Iceberg configs
//      .config("spark.sql.warehouse.dir", ADT_TEST_PATH + "warehouse/dir")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
//      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
//      .config("spark.sql.catalog.spark_catalog.warehouse", ADT_TEST_PATH + "spark_catalog/dir")
//      .config("spark.sql.catalog.iceberg_test", "org.apache.iceberg.spark.SparkCatalog")
//      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
//      .master("local[*]")
//      .getOrCreate()
////    HdfsIOUtils.delete(ADT_TEST_PATH)(session)
//    session
//  }

}

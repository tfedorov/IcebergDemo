/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package com.tfedorov.icebergdemo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object DemoApp extends Logging {

  def main(args: Array[String]): Unit = {
//    createTable()
    initialInsert()
//    upsertData()
//    timeTravelExample("2640772649640991918")
//    expireSnapshot()
  }

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    // Iceberg specific configs
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.harry_ns", "org.apache.iceberg.spark.SparkCatalog")
    // • HadoopCatalog supports tables that are stored in HDFS or your local file system.
    // • HiveCatalog uses a Hive Metastore to keep track of your Iceberg table by storing a reference to the latest metadata file.
    .config("spark.sql.catalog.harry_ns.type", "hadoop")
    .config("spark.sql.catalog.harry_ns.warehouse", "src/main/resources/output_iceberg/catalog/harry_ns/")
    .config("spark.sql.warehouse.dir", "src/main/resources/output_iceberg")
    .getOrCreate()

  /**
    * Create an Iceberg table
    */
  def createTable(): Unit = {

    val createTableSQL =
      """
     CREATE TABLE IF NOT EXISTS harry_ns.input_table (Id  string,
          Name  string,
          Gender  string,
          Job  string,
          House  string,
          Wand  string,
          Patronus  string,
          Species  string,
          Blood_status  string,
          Hair_colour  string,
          Eye_colour  string,
          Loyalty  string,
          Skills  string,
          Birth  string,
          Death  string
          )
        USING iceberg
        PARTITIONED BY (Gender)
        TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '0')
        """
    //TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '1')
    spark.sql(createTableSQL)

    log.info("Snapshots after creation")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show

  }

  /**
    * Read input data Harry Potter from semicolon separated file
    * Fill the Iceberg table by
    */
  def initialInsert(): Unit = {

    val inputDF: DataFrame = spark.read
    // .option("delimiter", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiLine", "true")
      //.csv("src/main/resources/input_data/CharactersOriginal.ssv")
      .csv("src/main/resources/input_data/Characters.csv")

    inputDF.printSchema()

    inputDF.createGlobalTempView("input_data")

    val insertSQL =
      """
     INSERT INTO harry_ns.input_table
        SELECT Id,Name,Gender,Job,House,Wand,Patronus,Species,Blood_status,Hair_colour,Eye_colour,Loyalty,Skills,Birth,Death
      FROM global_temp.input_data
      """
    spark.sql(insertSQL)

    log.info("Snapshots after insert")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show
    log.info("Files after insert")
    spark
      .sql("SELECT file_path,file_format,partition,record_count,null_value_counts FROM harry_ns.input_table.files")
      .show
  }

  def upsertData(): Unit = {

    val selectedRowDF: DataFrame =
      spark.sql(
        """
SELECT
  1 as Id,
 'Harry James Potter' AS  Name,
  'Male' AS Gender,
  'Student' AS Job,
  'Gryffindor' AS House,
  'Holly  phoenix feather' AS Wand,
  'Stag' AS Patronus,
  'Human' AS Species,
  'II+' AS Blood_status,
  'blondie' AS Hair_colour,
  'Bright green' AS Eye_colour,
  'Albus Dumbledore' AS Loyalty,
  'Parseltongue' AS Skills,
  '31 July 1980' AS Birth,
  '' AS Death
"""
      )
    selectedRowDF.show
    selectedRowDF.createGlobalTempView("changed_data")

    spark.sql(
      """
MERGE INTO harry_ns.input_table base USING global_temp.changed_data incr
    ON base.id = incr.id
WHEN MATCHED THEN
    UPDATE SET base.Hair_colour = incr.Hair_colour
 """
    )
    spark.sql("SELECT gender, count(*) FROM harry_ns.input_table GROUP BY gender").show

    log.info("Harries after upsert")
    spark.sql("SELECT * FROM harry_ns.input_table where name like 'Harry%'").show
    log.info("Snapshots after upsert")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show
    log.info("Files after upsert")
    spark
      .sql("SELECT file_path,file_format,partition,record_count,null_value_counts FROM harry_ns.input_table.files")
      .show
  }

  def timeTravelExample(snapshotVersion: String): Unit = {
    log.info("1,777 in current snapshot")
    spark.sql("SELECT id, gender, name, Hair_colour FROM harry_ns.input_table WHERE id in (1)").show

    log.info("1,777 in snapshot" + snapshotVersion)
    spark
      .sql(
        s"""
        SELECT id, gender, name, Hair_colour 
        FROM harry_ns.input_table VERSION AS OF $snapshotVersion
        WHERE id in (1)"""
      )
      .show

    log.info("Ids not present in previous snapshot")
    spark
      .sql(
        s"""
        SELECT id, gender, name, Hair_colour
        FROM harry_ns.input_table
        WHERE id not in ( SELECT id  FROM harry_ns.input_table VERSION AS OF $snapshotVersion)"""
      )
      .show

  }

  //https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files
  def expireSnapshot(): Unit = {
    log.info("Snapshots before clearing")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show
    spark
      .sql(
        """
        CALL harry_ns.system.expire_snapshots(
          table => 'harry_ns.input_table'
          , older_than => TIMESTAMP 'now'
          , retain_last => 1
        )"""
      )
      .show

    log.info("Snapshots after clearing")
    spark.sql("SELECT * FROM harry_ns.input_table.snapshots").show

//    spark.sql("SELECT * FROM harry_ns.input_table.manifests").show
//    spark
//      .sql("CALL harry_ns.system.rewrite_manifests('harry_ns.input_table')")
//      //.sql("CALL harry_ns.system.rewrite_data_files('harry_ns.input_table')")
//      .show
//    spark.sql("SELECT * FROM harry_ns.input_table.manifests").show
  }

}

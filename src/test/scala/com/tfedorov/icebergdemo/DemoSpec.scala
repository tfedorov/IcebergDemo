package com.tfedorov.icebergdemo

import com.tfedorov.icebergdemo.SparkLocalExtractor.icebergSpark
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DemoSpec extends AnyFreeSpec with Matchers {

  "GIVEN iceberg catalogue WHEN created new table THEN count of row should be 0" in {
//    val spark: SparkSession = icebergSpark
//    spark.sql(
//      """CREATE NAMESPACE IF NOT EXISTS harry_ns"""
//    )
//    spark.sql(
//      """CREATE TABLE IF NOT EXISTS harry_ns.integrated_table (key string, value string)
//        USING iceberg PARTITIONED BY (key)"""
//    )
//    spark.sql("SELECT * FROM harry_ns.integrated_table").count() shouldBe 0
  }

}

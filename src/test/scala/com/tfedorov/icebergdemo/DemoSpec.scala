package com.tfedorov.icebergdemo

import com.tfedorov.icebergdemo.SparkLocalExtractor.localSession
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DemoSpec extends AnyFreeSpec with Matchers {

  "2 + 2 spec" in {
    val spark:SparkSession = localSession
    spark.sql(
      """CREATE NAMESPACE IF NOT EXISTS harry"""
    )
    spark.sql(
      """CREATE TABLE IF NOT EXISTS harry.integrated_table (key string, value string)
        USING iceberg PARTITIONED BY (key)
        LOCATION 'src/test/resources/iceberg/spark_catalog/dir/harry/integrated_table'"""
    )

    2 + 2 shouldBe 4
  }

}

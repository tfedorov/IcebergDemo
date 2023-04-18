package com.tfedorov.icebergdemo

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DemoSpec extends AnyFreeSpec with Matchers {

  "2 + 2 spec" in {
    2 + 2 shouldBe 4
  }

}

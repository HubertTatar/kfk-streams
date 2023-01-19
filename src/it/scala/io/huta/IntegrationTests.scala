package io.huta

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class IntegrationTests
  extends AnyFunSpec
  with Matchers {

  describe("Integration test") {
    it("should run") {
      true shouldBe true
    }
  }
}

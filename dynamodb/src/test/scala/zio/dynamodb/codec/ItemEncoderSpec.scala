package zio.dynamodb.codec

import zio.dynamodb.Item
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, ZSpec, _ }

object ItemEncoderSpec extends DefaultRunnableSpec with CodecTestFixtures {
  override def spec: ZSpec[Environment, Failure] = suite("ItemEncoder Suite")(mainSuite)

  val mainSuite: ZSpec[Environment, Failure] = suite("Main Suite")(
    test("encodes List") {
      val expectedItem: Item = Item("nums" -> List(1, 2, 3))

      val item = ItemEncoder.toItem(CaseClassOfList(List(1, 2, 3)))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes simple Optional Item") {
      val expectedItem: Item = Item("id" -> 2, "name" -> "Avi", "opt" -> null)

      val item = ItemEncoder.toItem(SimpleCaseClass3Option(2, "Avi", opt = None))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes simple Item") {
      val expectedItem: Item = Item("id" -> 2, "name" -> "Avi", "flag" -> true)

      val item = ItemEncoder.toItem(SimpleCaseClass3(2, "Avi", flag = true))

      assert(item)(equalTo(expectedItem))
    },
    test("encodes nested Items") {
      val expectedItem: Item = Item("id" -> 1, "nested" -> Item("id" -> 2, "name" -> "Avi", "flag" -> true))

      val item = ItemEncoder.toItem(NestedCaseClass2(1, SimpleCaseClass3(2, "Avi", flag = true)))

      assert(item)(equalTo(expectedItem))
    }
  )
}

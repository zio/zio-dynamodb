package zio.dynamodb.codec

import zio.dynamodb.Item
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, ZSpec, _ }

object ItemDecoderSpec extends DefaultRunnableSpec with CodecTestFixtures {
  override def spec: ZSpec[Environment, Failure] = suite("ItemDecoder Suite")(mainSuite)

  val mainSuite: ZSpec[Environment, Failure] = suite("Decoder Suite")(
    test("decoded simple case class") {
      val expected = SimpleCaseClass3(2, "Avi", flag = true)

      val actual = ItemDecoder.fromItem[SimpleCaseClass3](Item("id" -> 2, "name" -> "Avi", "flag" -> true))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes nested Items") {
      val expected = NestedCaseClass2(1, SimpleCaseClass3(2, "Avi", flag = true))

      val actual = ItemDecoder.fromItem[NestedCaseClass2](
        Item("id" -> 1, "nested" -> Item("id" -> 2, "name" -> "Avi", "flag" -> true))
      )

      assert(actual)(isRight(equalTo(expected)))
    }
  )
}

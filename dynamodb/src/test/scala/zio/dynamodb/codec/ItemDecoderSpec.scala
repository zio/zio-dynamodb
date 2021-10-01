package zio.dynamodb.codec

import zio.dynamodb.{ AttrMap, Item }
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, ZSpec, _ }

import java.time.Instant

object ItemDecoderSpec extends DefaultRunnableSpec with CodecTestFixtures {
  override def spec: ZSpec[Environment, Failure] =
    suite("ItemDecoder Suite")(mainSuite)

  val mainSuite: ZSpec[Environment, Failure] = suite("Decoder Suite")(
    test("decoded list") {
      val expected = CaseClassOfList(List(1, 2))

      val actual = ItemDecoder.fromItem[CaseClassOfList](Item("nums" -> List(1, 2)))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded option of Some") {
      val expected = CaseClassOfOption(Some(42))

      val actual = ItemDecoder.fromItem[CaseClassOfOption](Item("opt" -> 42))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded option of None") {
      val expected = CaseClassOfOption(None)

      val actual = ItemDecoder.fromItem[CaseClassOfOption](Item("opt" -> null))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded nested option of Some") {
      val expected = CaseClassOfNestedOption(Some(Some(42)))

      val actual = ItemDecoder.fromItem[CaseClassOfNestedOption](Item("opt" -> 42))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded nested option of None") {
      val expected = CaseClassOfNestedOption(None)

      val actual = ItemDecoder.fromItem[CaseClassOfNestedOption](Item("opt" -> null))

      assert(actual)(isRight(equalTo(expected)))
    },
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
    },
    test("decodes Instant") {
      val expected = CaseClassOfInstant(Instant.parse("2021-09-28T00:00:00Z"))

      val actual = ItemDecoder.fromItem[CaseClassOfInstant](Item("instant" -> "2021-09-28T00:00:00Z"))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes Ok ADT") {
      val expected = CaseClassOfStatus(Ok(List("1", "2")))

      val actual =
        ItemDecoder.fromItem[CaseClassOfStatus](Item("status" -> Item("Ok" -> Item("response" -> List("1", "2")))))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes Pending case object ADT") {
      val expected = CaseClassOfStatus(Pending)

      val actual =
        ItemDecoder.fromItem[CaseClassOfStatus](Item("status" -> Item("Pending" -> null)))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decode tuple3") {
      val item     = new AttrMap(Map("tuple" -> toList(toTuple(1, 2), toNum(3))))
      val expected = CaseClassOfTuple3((1, 2, 3))

      val actual = ItemDecoder.fromItem[CaseClassOfTuple3](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes map") {
      val item     = AttrMap(Map("map" -> toList(toTuple("One", 1), toTuple("Two", 2))))
      val expected = CaseClassOfMapOfInt(Map("One" -> 1, "Two" -> 2))

      val actual = ItemDecoder.fromItem[CaseClassOfMapOfInt](item)

      assert(actual)(isRight(equalTo(expected)))
    }
  )

}

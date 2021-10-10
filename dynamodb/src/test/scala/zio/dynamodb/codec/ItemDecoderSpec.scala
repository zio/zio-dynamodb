package zio.dynamodb.codec

import zio.dynamodb.{ AttrMap, AttributeValue, Decoder, DynamoDBQuery, Item }
import zio.test.assertTrue
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, ZSpec, _ }

import java.time.Instant
import scala.collection.immutable.ListMap

object ItemDecoderSpec extends DefaultRunnableSpec with CodecTestFixtures {
  override def spec: ZSpec[Environment, Failure] =
    suite("ItemDecoder Suite")(mainSuite)

  val mainSuite: ZSpec[Environment, Failure] = suite("Decoder Suite")(
    test("decodes generic record") {
      val expected: Map[String, Any] = ListMap("foo" -> "FOO", "bar" -> 1)

      val actual: Either[String, Map[String, Any]] = Decoder.decoder(recordSchema)(
        AttributeValue.Map(Map(toAvString("foo") -> toAvString("FOO"), toAvString("bar") -> toAvNum(1)))
      )

      assertTrue(actual == Right(expected))
    },
    test("encodes enumeration") {

      val actual = Decoder.decoder(enumSchema)(AttributeValue.List(List(toAvString("string"), toAvString("FOO"))))

      assertTrue(actual == Right("string" -> "FOO"))
    },
    test("decoded list") {
      val expected = CaseClassOfList(List(1, 2))

      val actual = DynamoDBQuery.fromItem[CaseClassOfList](Item("nums" -> List(1, 2)))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded option of Some") {
      val expected = CaseClassOfOption(Some(42))

      val actual = DynamoDBQuery.fromItem[CaseClassOfOption](Item("opt" -> 42))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded option of None") {
      val expected = CaseClassOfOption(None)

      val actual = DynamoDBQuery.fromItem[CaseClassOfOption](Item("opt" -> null))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded nested option of Some") {
      val expected = CaseClassOfNestedOption(Some(Some(42)))

      val actual = DynamoDBQuery.fromItem[CaseClassOfNestedOption](Item("opt" -> 42))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded nested option of None") {
      val expected = CaseClassOfNestedOption(None)

      val actual = DynamoDBQuery.fromItem[CaseClassOfNestedOption](Item("opt" -> null))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded simple case class") {
      val expected = SimpleCaseClass3(2, "Avi", flag = true)

      val actual = DynamoDBQuery.fromItem[SimpleCaseClass3](Item("id" -> 2, "name" -> "Avi", "flag" -> true))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes nested Items") {
      val expected = NestedCaseClass2(1, SimpleCaseClass3(2, "Avi", flag = true))

      val actual = DynamoDBQuery.fromItem[NestedCaseClass2](
        Item("id" -> 1, "nested" -> Item("id" -> 2, "name" -> "Avi", "flag" -> true))
      )

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes Instant") {
      val expected = CaseClassOfInstant(Instant.parse("2021-09-28T00:00:00Z"))

      val actual = DynamoDBQuery.fromItem[CaseClassOfInstant](Item("instant" -> "2021-09-28T00:00:00Z"))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes Ok ADT") {
      val expected = CaseClassOfStatus(Ok(List("1", "2")))

      val actual =
        DynamoDBQuery.fromItem[CaseClassOfStatus](Item("status" -> Item("Ok" -> Item("response" -> List("1", "2")))))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes Pending case object ADT") {
      val expected = CaseClassOfStatus(Pending)

      val actual =
        DynamoDBQuery.fromItem[CaseClassOfStatus](Item("status" -> Item("Pending" -> null)))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decode tuple2") {
      val item     = new AttrMap(Map("tuple2" -> toAvTuple("1", 2)))
      val expected = CaseClassOfTuple2(("1", 2))

      val actual = DynamoDBQuery.fromItem[CaseClassOfTuple2](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decode tuple3") {
      val item     = new AttrMap(Map("tuple" -> toAvList(toAvTuple(1, 2), toAvNum(3))))
      val expected = CaseClassOfTuple3((1, 2, 3))

      val actual = DynamoDBQuery.fromItem[CaseClassOfTuple3](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes map") {
      val item     = AttrMap(Map("map" -> toAvList(toAvTuple("One", 1), toAvTuple("Two", 2))))
      val expected = CaseClassOfMapOfInt(Map("One" -> 1, "Two" -> 2))

      val actual = DynamoDBQuery.fromItem[CaseClassOfMapOfInt](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes Either Right") {
      val item     = Item("either" -> Item("Right" -> 1))
      val expected = CaseClassOfEither(Right(1))

      val actual = DynamoDBQuery.fromItem[CaseClassOfEither](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes Either Left") {
      val item     = Item("either" -> Item("Left" -> "boom"))
      val expected = CaseClassOfEither(Left("boom"))

      val actual = DynamoDBQuery.fromItem[CaseClassOfEither](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes List of case class") {
      val item     = Item("elements" -> List(Item("id" -> 1, "name" -> "Avi", "flag" -> true)))
      val expected = CaseClassOfListOfCaseClass(List(SimpleCaseClass3(1, "Avi", flag = true)))

      val actual = DynamoDBQuery.fromItem[CaseClassOfListOfCaseClass](item)

      assert(actual)(isRight(equalTo(expected)))
    }
  )

}

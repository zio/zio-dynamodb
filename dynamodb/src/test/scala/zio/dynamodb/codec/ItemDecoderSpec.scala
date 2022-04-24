package zio.dynamodb.codec

import zio.dynamodb._
import zio.dynamodb.codec.Invoice.PreBilled
import zio.test.Assertion._
import zio.test._

import java.time.Instant
import scala.collection.immutable.ListMap
import zio.test.ZIOSpecDefault

object ItemDecoderSpec extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemDecoder Suite")(mainSuite)

  val mainSuite = suite("Decoder Suite")(
    test("decodes generic record") {
      val expected: Map[String, Any] = ListMap("foo" -> "FOO", "bar" -> 1)

      val actual: Either[String, Map[String, Any]] = Codec.decoder(recordSchema)(
        AttributeValue.Map(Map(toAvString("foo") -> toAvString("FOO"), toAvString("bar") -> toAvNum(1)))
      )

      assertTrue(actual == Right(expected))
    },
    test("encodes enumeration") {

      val actual = Codec.decoder(enumSchema)(AttributeValue.Map(Map(toAvString("string") -> toAvString("FOO"))))

      assertTrue(actual == Right("FOO"))
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
    test("decoded option of None in CaseClass2 represented by a missing value") {
      val expected = CaseClass2OfOption(1, None)

      val actual = DynamoDBQuery.fromItem[CaseClass2OfOption](Item("num" -> 1))

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
    },
    test("decodes enum with discriminator annotation") {
      val item: Item =
        Item(
          Map(
            "enum" -> AttributeValue.Map(
              Map(
                AttributeValue.String("value")              -> AttributeValue.String("foobar"),
                AttributeValue.String("funkyDiscriminator") -> AttributeValue.String("StringValue")
              )
            )
          )
        )

      val actual = DynamoDBQuery.fromItem[WithDiscriminatedEnum](item)

      assert(actual)(isRight(equalTo(WithDiscriminatedEnum(WithDiscriminatedEnum.StringValue("foobar")))))
    },
    test("decodes enum with discriminator annotation and id annotation on a case class field ") {
      val item: Item =
        Item(
          Map(
            "enum" -> AttributeValue.Map(
              Map(
                AttributeValue.String("funky_value")        -> AttributeValue.String("foobar"),
                AttributeValue.String("funkyDiscriminator") -> AttributeValue.String("StringValue2")
              )
            )
          )
        )

      val actual = DynamoDBQuery.fromItem[WithDiscriminatedEnum](item)

      assert(actual)(isRight(equalTo(WithDiscriminatedEnum(WithDiscriminatedEnum.StringValue2("foobar")))))
    },
    test("decodes enum with discriminator annotation and an id annotation on a case class") {
      val item: Item =
        Item(
          Map(
            "enum" -> AttributeValue.Map(
              Map(
                AttributeValue.String("value")              -> AttributeValue.Number(BigDecimal(1)),
                AttributeValue.String("funkyDiscriminator") -> AttributeValue.String("ival")
              )
            )
          )
        )

      val actual = DynamoDBQuery.fromItem[WithDiscriminatedEnum](item)

      assert(actual)(isRight(equalTo(WithDiscriminatedEnum(WithDiscriminatedEnum.IntValue(1)))))
    },
    test("decodes enum with discriminator annotation and case object as item without a id annotation") {
      val item: Item =
        Item(
          Map(
            "enum" -> AttributeValue.Map(
              Map(
                AttributeValue.String("funkyDiscriminator") -> AttributeValue.String("ONE")
              )
            )
          )
        )

      val actual = DynamoDBQuery.fromItem[WithDiscriminatedEnum](item)

      assert(actual)(isRight(equalTo(WithDiscriminatedEnum(WithDiscriminatedEnum.ONE))))
    },
    test("decodes enum with discriminator annotation and case object as item with a id annotation of '2'") {
      val item: Item =
        Item(
          Map(
            "enum" -> AttributeValue.Map(
              Map(
                AttributeValue.String("funkyDiscriminator") -> AttributeValue.String("2")
              )
            )
          )
        )

      val actual = DynamoDBQuery.fromItem[WithDiscriminatedEnum](item)

      assert(actual)(isRight(equalTo(WithDiscriminatedEnum(WithDiscriminatedEnum.TWO))))
    },
    test("decodes top level enum with discriminator annotation") {
      val item: Item =
        Item(
          Map(
            "id"                 -> AttributeValue.Number(BigDecimal(1)),
            "s"                  -> AttributeValue.String("foobar"),
            "funkyDiscriminator" -> AttributeValue.String("PreBilled")
          )
        )

      val actual = DynamoDBQuery.fromItem[Invoice](item)

      assert(actual)(isRight(equalTo(PreBilled(id = 1, s = "foobar"))))
    },
    test("decodes case object only enum with id annotation") {
      val item: Item = Item(Map("enum" -> AttributeValue.String("ONE")))

      val actual = DynamoDBQuery.fromItem[WithCaseObjectOnlyEnum](item)

      assert(actual)(isRight(equalTo(WithCaseObjectOnlyEnum(WithCaseObjectOnlyEnum.ONE))))
    },
    test("decodes case object only enum with enumNameAsValue annotation and id annotation of '2'") {
      val item: Item = Item(Map("enum" -> AttributeValue.String("2")))

      val actual = DynamoDBQuery.fromItem[WithCaseObjectOnlyEnum](item)

      assert(actual)(isRight(equalTo(WithCaseObjectOnlyEnum(WithCaseObjectOnlyEnum.TWO))))
    },
    test("decodes enum and ignores @id annotation at case class level when there is no @discriminator annotation") {
      val item: Item = Item("enum" -> Item(Map("ONE" -> AttributeValue.Null)))

      val actual = DynamoDBQuery.fromItem[WithEnumWithoutDiscriminator](item)

      assert(actual)(isRight(equalTo(WithEnumWithoutDiscriminator(WithEnumWithoutDiscriminator.ONE))))
    },
    test("decodes enum without @discriminator annotation and uses @id field level annotation") {
      val item: Item = Item("enum" -> Item(Map("ONE" -> AttributeValue.Null)))

      val actual = DynamoDBQuery.fromItem[WithEnumWithoutDiscriminator](item)

      assert(actual)(isRight(equalTo(WithEnumWithoutDiscriminator(WithEnumWithoutDiscriminator.ONE))))
    }
  )

}

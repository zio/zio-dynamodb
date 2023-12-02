package zio.dynamodb.codec

import zio.dynamodb._
import zio.dynamodb.codec.Invoice.PreBilled
import zio.test.Assertion._
import zio.test.{ ZIOSpecDefault, _ }

import java.time.Instant
import scala.collection.immutable.ListMap

object ItemDecoderSpec extends ZIOSpecDefault with CodecTestFixtures {
  override def spec = suite("ItemDecoder Suite")(mainSuite, noDiscriminatorSuite)

  private val mainSuite = suite("Decoder Suite")(
    test("decodes generic record") {
      val expected: Map[String, Any] = ListMap("foo" -> "FOO", "bar" -> 1)

      val actual: Either[DynamoDBError, Map[String, Any]] = Codec.decoder(recordSchema)(
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
    test("decoded empty list when field is missing") {
      val expected = CaseClassOfList(List.empty)

      val actual = DynamoDBQuery.fromItem[CaseClassOfList](Item.empty)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded option of Some") {
      val expected = CaseClassOfOption(Some(42))

      val actual = DynamoDBQuery.fromItem[CaseClassOfOption](Item("opt" -> 42))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded option of None where value is null") {
      val expected = CaseClassOfOption(None)

      val actual = DynamoDBQuery.fromItem[CaseClassOfOption](Item("opt" -> null))

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decoded option of None where field is missing") {
      val expected = CaseClassOfOption(None)

      val actual = DynamoDBQuery.fromItem[CaseClassOfOption](Item.empty)

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
      val item     = Item("map" -> Map("One" -> 1, "Two" -> 2))
      val expected = CaseClassOfMapOfInt(Map("One" -> 1, "Two" -> 2))

      val actual = DynamoDBQuery.fromItem[CaseClassOfMapOfInt](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes map when field is missing") {
      val item     = Item.empty
      val expected = CaseClassOfMapOfInt(Map.empty)

      val actual = DynamoDBQuery.fromItem[CaseClassOfMapOfInt](item)(caseClassOfMapOfInt)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes set") {
      val item     = Item("set" -> Set(1, 2))
      val expected = CaseClassOfSetOfInt(Set(1, 2))

      val actual = DynamoDBQuery.fromItem[CaseClassOfSetOfInt](item)

      assert(actual)(isRight(equalTo(expected)))
    },
    test("decodes set when field is missing") {
      val item     = Item.empty
      val expected = CaseClassOfSetOfInt(Set.empty)

      val actual = DynamoDBQuery.fromItem[CaseClassOfSetOfInt](item)

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
    test("decodes enum with @discriminatorName annotation") {
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
    test("decodes enum with @discriminatorName annotation and @fieldName annotation on a case class field ") {
      val item: Item =
        Item(
          Map(
            "enum" -> AttributeValue.Map(
              Map(
                AttributeValue.String("funky_field_name")   -> AttributeValue.String("foobar"),
                AttributeValue.String("funkyDiscriminator") -> AttributeValue.String("StringValue2")
              )
            )
          )
        )

      val actual = DynamoDBQuery.fromItem[WithDiscriminatedEnum](item)

      assert(actual)(isRight(equalTo(WithDiscriminatedEnum(WithDiscriminatedEnum.StringValue2("foobar")))))
    },
    test("decodes enum with @discriminatorName annotation and an @caseName annotation on a case class") {
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
    test("decodes enum with @discriminatorName annotation and case object as item without a @caseName annotation") {
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
    test("decodes enum with @discriminatorName annotation and case object as item with a @caseName annotation of '2'") {
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
    test("decodes top level enum with @discriminatorName annotation") {
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
    test("decodes case object only enum without @caseName annotation") {
      val item: Item = Item(Map("enum" -> AttributeValue.String("ONE")))

      val actual = DynamoDBQuery.fromItem[WithCaseObjectOnlyEnum](item)

      assert(actual)(isRight(equalTo(WithCaseObjectOnlyEnum(WithCaseObjectOnlyEnum.ONE))))
    },
    test("decodes case object only enum with @caseName annotation of '2'") {
      val item: Item = Item(Map("enum" -> AttributeValue.String("2")))

      val actual = DynamoDBQuery.fromItem[WithCaseObjectOnlyEnum](item)

      assert(actual)(isRight(equalTo(WithCaseObjectOnlyEnum(WithCaseObjectOnlyEnum.TWO))))
    },
    test(
      "decodes enum and honours @caseName annotation at case class level when there is no @discriminatorName annotation"
    ) {
      val item: Item = Item("enum" -> Item(Map("1" -> AttributeValue.Null)))

      val actual = DynamoDBQuery.fromItem[WithEnumWithoutDiscriminator](item)

      assert(actual)(isRight(equalTo(WithEnumWithoutDiscriminator(WithEnumWithoutDiscriminator.ONE))))
    },
    test("decodes enum without @discriminatorName annotation and uses @caseName annotation") {
      val item: Item = Item("enum" -> Item(Map("1" -> AttributeValue.Null)))

      val actual = DynamoDBQuery.fromItem[WithEnumWithoutDiscriminator](item)

      assert(actual)(isRight(equalTo(WithEnumWithoutDiscriminator(WithEnumWithoutDiscriminator.ONE))))
    }
  )

  val noDiscriminatorSuite = suite("@noDisriminator Decoder Suite")(
    test("decodes One case class with @fieldName") {
      val item: Item = Item("sumType" -> Item("count" -> 42))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.One(i = 42)))))
    },
    test("decodes Two case class and ignores class level @caseName (which only applies to discriminators)") {
      val item: Item = Item("sumType" -> Item("s" -> "X"))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.Two(s = "X")))))
    },
    test("decodes MinusOne case object") {
      val item: Item = Item(Map("sumType" -> AttributeValue.String("MinusOne")))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.MinusOne))))
    },
    test("decodes Zero case object with @caseName") {
      val item: Item = Item(Map("sumType" -> AttributeValue.String("0")))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(isRight(equalTo(WithNoDiscriminator(NoDiscriminatorEnum.Zero))))
    },
    test("returns a Left of DecodeError when no decoder is not found") {
      val item: Item = Item("sumType" -> Item("FIELD_NOT_IN_ANY_SUBTYPE" -> "X"))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminator](item)

      assert(actual)(
        isLeft(
          equalTo(
            DynamoDBError.DecodingError(message =
              "All sub type decoders failed for Map(Map(String(FIELD_NOT_IN_ANY_SUBTYPE) -> String(X)))"
            )
          )
        )
      )
    },
    test("returns a Left of DecodeError when a more than one decoder is found") {
      val item: Item = Item("sumType" -> Item("i" -> 42))

      val actual = DynamoDBQuery.fromItem[WithNoDiscriminatorError](item)

      assert(actual)(
        isLeft(
          equalTo(
            DynamoDBError.DecodingError(message =
              "More than one sub type decoder succeeded for Map(Map(String(i) -> Number(42)))"
            )
          )
        )
      )
    }
  )

}

package zio.dynamodb.json

import zio.test.Gen
import zio.dynamodb.AttributeValue

object AttributeValueGen {

  private val min = BigDecimal(-100)
  private val max = BigDecimal(100)

  private val anyBigDecimal = Gen.bigDecimal(min, max)

  private val anyString = Gen.alphaNumericStringBounded(1, 15).map(AttributeValue.String)

  private val anyNumber =
    anyBigDecimal.map(AttributeValue.Number)
  private val anyBool   = Gen.boolean.map(AttributeValue.Bool)

  val anyPrimitive: Gen[Any, AttributeValue] =
    Gen.oneOf(
      anyString,
      anyNumber,
      anyBool,
      Gen.const(AttributeValue.Null)
    )

  val anyStringSet: Gen[Any, AttributeValue.StringSet] =
    Gen.setOf(Gen.string).map(AttributeValue.StringSet.apply)

  val anyNumberSet: Gen[Any, AttributeValue.NumberSet] =
    Gen.setOf(anyBigDecimal).map(AttributeValue.NumberSet.apply)

  val anyPrimitiveList = Gen.listOf(anyPrimitive).map(AttributeValue.List.apply)

  lazy val anyMapOfPrimitives: Gen[Any, AttributeValue.Map] =
    Gen.mapOf(anyString, anyPrimitive).map(AttributeValue.Map.apply)

  lazy val anyMapOfMap: Gen[Any, AttributeValue.Map] =
    Gen.mapOf(anyString, anyMapOfPrimitives).map(AttributeValue.Map.apply)

  lazy val anyMap: Gen[Any, AttributeValue] =
    anyMapOfPrimitives.zip(anyMapOfMap).map {
      case (primitives, map) => AttributeValue.Map(primitives.value ++ map.value)
    }

}

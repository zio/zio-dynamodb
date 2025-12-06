package zio.dynamodb.json

import zio.test.Gen
import zio.dynamodb.AttributeValue

object AttributeValueGen {

  private val min = BigDecimal(-1000000000.0000002)
  private val max = BigDecimal(1000000000.0000001)

  private val anyBigDecimal                                    = Gen.bigDecimal(min, max)
  private val anyString                                        = Gen.alphaNumericStringBounded(1, 15).map(AttributeValue.String.apply)
  private val anyNumber                                        =
    anyBigDecimal.map(AttributeValue.Number.apply)
  private val anyBool                                          = Gen.boolean.map(AttributeValue.Bool.apply)
  private val anyPrimitive: Gen[Any, AttributeValue]           =
    Gen.oneOf(
      anyString,
      anyNumber,
      anyBool,
      Gen.const(AttributeValue.Null)
    )
  private val anyMapOfPrimitives: Gen[Any, AttributeValue.Map] =
    Gen.mapOf(anyString, anyPrimitive).map(AttributeValue.Map.apply)

  private val anyPrimitiveList: Gen[Any, AttributeValue.List]      = Gen.listOf(anyPrimitive).map(AttributeValue.List.apply)
  private val anyMapOfPrimitivesList: Gen[Any, AttributeValue.Map] =
    Gen.mapOf(anyString, anyPrimitiveList).map(AttributeValue.Map.apply)
  private val anyListOfMap: Gen[Any, AttributeValue.List]          =
    Gen.listOf(anyMapOfPrimitives).map(AttributeValue.List.apply)
  private val anyMapOfListOfMap: Gen[Any, AttributeValue.Map]      =
    Gen.mapOf(anyString, anyListOfMap).map(AttributeValue.Map.apply)

  private val anyStringSet: Gen[Any, AttributeValue.StringSet]   =
    Gen.setOf(Gen.string).map(AttributeValue.StringSet.apply)
  private val anyNumberSet: Gen[Any, AttributeValue.NumberSet]   =
    Gen.setOf(anyBigDecimal).map(AttributeValue.NumberSet.apply)
  private val anyMapOfPrimitiveSet: Gen[Any, AttributeValue.Map] =
    Gen.mapOf(anyString, Gen.oneOf(anyStringSet, anyNumberSet)).map(AttributeValue.Map.apply)

  private val anyMapOfMap: Gen[Any, AttributeValue.Map] =
    Gen.mapOf(anyString, anyMapOfPrimitives).map(AttributeValue.Map.apply)

  val anyItem: Gen[Any, AttributeValue] =
    anyMapOfPrimitives
      .zip(anyMapOfMap)
      .zip(anyMapOfPrimitiveSet)
      .zip(anyMapOfPrimitivesList)
      .zip(anyMapOfListOfMap)
      .map {
        case (primitives, maps, setsOfPrimitives, listOfPrimitives, listOfMaps) =>
          AttributeValue.Map(
            primitives.value ++ maps.value ++ setsOfPrimitives.value ++ listOfPrimitives.value ++ listOfMaps.value
          )
      }

}

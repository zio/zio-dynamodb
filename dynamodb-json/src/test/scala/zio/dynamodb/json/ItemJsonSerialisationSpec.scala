package zio.dynamodb.json

import zio.Scope
import zio.dynamodb.json.DynamodbJsonCodec.Decoder.decode
import zio.dynamodb.{ AttrMap, AttributeValue }
import zio.json.ast.Json
import zio.json._
import zio.test.Assertion.equalTo
import zio.test.{ assert, assertTrue, check, Spec, TestEnvironment, ZIOSpecDefault }
import zio.ZIO

object ItemJsonSerialisationSpec extends ZIOSpecDefault {

  /*
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
S – String
N – Number // TODO - return error for this????
B – Binary // TODO
BOOL – Boolean
NULL – Null
M – Map
L – List
SS – String Set
NS – Number Set
BS – Binary Set // TODO


  private[dynamodb] final case class Binary(value: Iterable[Byte])              extends AttributeValue
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]]) extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                       extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])      extends AttributeValue

  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue

  private[dynamodb] final case class Number(value: BigDecimal)          extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])  extends AttributeValue
  private[dynamodb] case object Null                                    extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)         extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString]) extends AttributeValue

  type Encoder[A]  = A => AttributeValue
  type Decoder[+A] = AttributeValue => Either[ItemError, A]
   */

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ItemJsonSerialisationSpec")(
      encoderSuite,
      decoderSuite,
      transformSuite,
      endToEndSuite,
      pbtSuite
    )

  val pbtSuite = suite("property based testing suite")(
    test("round trip encode and decode") {
      checkRoundTrip(debug = false)
    }
  )

  private def checkRoundTrip(debug: Boolean) =
    check(AttributeValueGen.anyItem) { avMap =>
      val json = DynamodbJsonCodec.Encoder.encode(avMap)
      val av   = DynamodbJsonCodec.Decoder.decode(json)
      for {
        _      <- if (debug) ZIO.debug(json.toString) else ZIO.unit
        checked = av match {
                    case Right(value)                           =>
                      assertTrue(value == avMap)
                    case Left("empty AttributeValue Map found") =>
                      assertTrue(true)
                    case Left(_)                                =>
                      assertTrue(false)
                  }
      } yield checked
    }

  val encoderSuite = suite("encoder suite")(
    test("encode top level map of primitives") {
      val avMap = AttributeValue.Map.empty +
        ("id"     -> AttributeValue.String("101")) +
        ("count"  -> AttributeValue.Number(BigDecimal(42))) +
        ("isTest" -> AttributeValue.Bool(true))
      val encoded = DynamodbJsonCodec.Encoder.encode(avMap)
      val s       = encoded.toJson
      assert(encoded)(
        equalTo(
          Json.Obj(
            "id"     -> Json.Obj("S" -> Json.Str("101")),
            "count"  -> Json.Obj("N" -> Json.Str("42")),
            "isTest" -> Json.Obj("BOOL" -> Json.Bool(true))
          )
        )
      )
    },
    test("encode nested map") {
      val avMap   = AttributeValue.Map.empty + ("foo" -> (AttributeValue.Map.empty + ("name" -> AttributeValue
        .String("Avi"))))
      val encoded = DynamodbJsonCodec.Encoder.encode(avMap)
      val s       = encoded.toJson
      assert(encoded)(
        equalTo(
          Json.Obj(
            "foo" -> Json.Obj("name" -> Json.Obj("S" -> Json.Str("Avi")))
          )
        )
      )
    },
    test("encode SS") {
      val avMap   = AttributeValue.Map.empty + ("stringSet" -> AttributeValue.StringSet(Set("1", "2")))
      val encoded = DynamodbJsonCodec.Encoder.encode(avMap)
      assert(encoded)(
        equalTo(
          Json.Obj(
            "stringSet" -> Json.Obj("SS" -> Json.Arr(Json.Str("1"), Json.Str("2")))
          )
        )
      )
    },
    test("encode NS") {
      val avMap =
        AttributeValue.Map.empty + ("numberSet" -> AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
      val encoded = DynamodbJsonCodec.Encoder.encode(avMap)
      assert(encoded)(
        equalTo(
          Json.Obj(
            "numberSet" -> Json.Obj("NS" -> Json.Arr(Json.Str("1"), Json.Str("2")))
          )
        )
      )
    },
    test("encode L of String") {
      val avMap =
        AttributeValue.Map.empty + ("listOfString" -> AttributeValue.List(
          List(AttributeValue.String("1"), AttributeValue.String("2"))
        ))
      val encoded = DynamodbJsonCodec.Encoder.encode(avMap)
      assert(encoded)(
        equalTo(
          Json.Obj(
            "listOfString" -> Json.Obj("L" -> Json.Arr(Json.Obj("S" -> Json.Str("1")), Json.Obj("S" -> Json.Str("2"))))
          )
        )
      )
    }
  )

  val decoderSuite     = suite("decoder suite")(
    test("decode top level map of primitives") {
      val s   =
        """{
              "id": {
                  "S": "101"
              },
              "count": {
                  "N": "42"
              },
              "isTest": {
                  "BOOL": true
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("id"     -> AttributeValue.String("101")) +
              ("count"  -> AttributeValue.Number(BigDecimal(42))) +
              ("isTest" -> AttributeValue.Bool(true))
          )
        )
      )
    },
    test("decode SS") {
      val s   =
        """{
              "stringSet": {
                  "SS": ["1", "2"]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("stringSet" -> AttributeValue.StringSet(Set("1", "2")))
          )
        )
      )
    },
    test("decode NS") {
      val s   =
        """{
              "stringSet": {
                  "NS": ["1", "2"]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("stringSet" -> AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
          )
        )
      )
    },
    test("decode M of object") {
      val s   =
        """{
              "map": {
                  "M": { "1": {"foo": {"S": "bar"}}, "2": {"foo": {"S": "baz"}} }
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("map" -> (AttributeValue.Map.empty + ("1" -> obj("bar")) + ("2" -> obj("baz"))))
          )
        )
      )
    },
    test("decode L of string") {
      val s   =
        """{
              "array": {
                  "L": ["1", "2"]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("array" -> AttributeValue.List(List(AttributeValue.String("1"), AttributeValue.String("2"))))
          )
        )
      )
    },
    test("decode L of object") {
      val s   =
        """{
              "array": {
                  "L": [{"foo": {"S": "bar"}},  {"foo": {"S": "baz"}}]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)

      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("array" -> AttributeValue.List(List(obj("bar"), obj("baz"))))
          )
        )
      )
    },
    test("decode nested map") {
      val s   =
        """{
              "foo": {
                    "name": {
                       "S": "Avi"
                     }                    
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty + ("foo" -> (AttributeValue.Map.empty + ("name" -> AttributeValue
              .String("Avi"))))
          )
        )
      )
    },
    test("empty object should return Left with error message") {
      val ast = "{}".fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Left("empty AttributeValue Map found")
        )
      )
    },
    test("empty array should return Left with error message") {
      val ast = "[]".fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Left("top level arrays are not supported, found []")
        )
      )
    }
  )
  val avToAttrMapSuite = suite("AttributeValue to AttrVal")(
    test("AV of top level only to AttrMap") {
      val ast: Json = Json.Obj(
        "id"    -> Json.Obj("S" -> Json.Str("101")),
        "count" -> Json.Obj("N" -> Json.Str("42"))
      )
      decode(ast).flatMap(_.toAttrMap) match {
        case Right(am) =>
          assertTrue(
            am == AttrMap.empty + ("id" -> AttributeValue.String(
              "101"
            )) + ("count"               -> AttributeValue.Number(BigDecimal(42)))
          )
        case _         => assertTrue(false)
      }

    },
    test("AV of nested map to AttrMap") {
      val s   =
        """{
              "foo": {
                  "name": {
                      "S": "Avi"
                    }                    
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      decode(ast).flatMap(_.toAttrMap) match {
        case Right(am) =>
          assertTrue(
            am == AttrMap.empty + ("foo" -> (AttributeValue.Map.empty + ("name" -> AttributeValue.String("Avi"))))
          )
        case _         => assertTrue(false)
      }
    },
    test("AV mixed to AttrMap") {
      val s   =
        """{
              "id": { "S": "101" },
              "nested": {
                    "foo": {
                       "S": "bar"
                     }                    
              },
              "count": { "N": "101" }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      decode(ast).flatMap(_.toAttrMap) match {
        case Right(am) =>
          assertTrue(
            am == AttrMap.empty +
              ("id"     -> AttributeValue.String("101")) +
              ("nested" -> obj("bar")) +
              ("count"  -> AttributeValue.Number(BigDecimal(101)))
          )
        case _         => assertTrue(false)
      }
    }
  )
  val attrMapToAVSuite = suite("AttrMap to AttributeValue")(
    test("top level only AttrMap to AttributeValue") {
      val avMap = AttrMap.empty +
        ("id"     -> AttributeValue.String("101")) +
        ("count"  -> AttributeValue.Number(BigDecimal(42))) +
        ("isTest" -> AttributeValue.Bool(true))
      assert(avMap.toAttributeValue)(
        equalTo(
          AttributeValue.Map.empty +
            ("id"     -> AttributeValue.String("101")) +
            ("count"  -> AttributeValue.Number(BigDecimal(42))) +
            ("isTest" -> AttributeValue.Bool(true))
        )
      )
    },
    test("nested AttrMap to AttributeValue") {
      val avMap = AttrMap.empty + ("foo" -> (AttributeValue.Map.empty + ("name" -> AttributeValue.String("Avi"))))
      assert(avMap.toAttributeValue)(
        equalTo(
          AttributeValue.Map.empty +
            ("foo" -> (AttributeValue.Map.empty + ("name" -> AttributeValue.String("Avi"))))
        )
      )
    }
  )
  val transformSuite   = suite("transform")(avToAttrMapSuite, attrMapToAVSuite)

  val endToEndSuite = suite("AttrMap end to end")(
    test("from AttrMap -> Json string -> AttrMap") {

      val am = AttrMap.empty +
        ("id"     -> AttributeValue.String("101")) +
        ("nested" -> obj("bar")) +
        ("count"  -> AttributeValue.Number(BigDecimal(101)))

      val jsonString = am.toJsonString

      parseItem(jsonString) match {
        case Right(am2) =>
          assertTrue(am2 == am)
        case _          => assertTrue(false)
      }
    }
  )

  def obj(value: String) = AttributeValue.Map.empty + ("foo" -> AttributeValue.String(value))

}

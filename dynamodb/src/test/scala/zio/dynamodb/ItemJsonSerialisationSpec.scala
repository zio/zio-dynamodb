package zio.dynamodb

import zio.dynamodb.JsonCodec.Decoder._
import zio.dynamodb.JsonCodec
import zio.json._
import zio.json.ast.Json
import zio.test.ZIOSpecDefault
import zio.Scope
import zio.test.Spec
import zio.test.TestEnvironment
import zio.test.{ assert, assertTrue }
import zio.test.Assertion.equalTo
//import zio.prelude._

object ItemJsonSerialisationSpec extends ZIOSpecDefault {

  val bookJsonString = """{
    "Id": {
        "N": "101"
    },
    "Title": {
        "S": "Book 101 Title"
    },
    "ISBN": {
        "S": "111-1111111111"
    },
    "Authors": {
        "L": [
            {
                "S": "Author1"
            }
        ]
    }
}"""

  final case class Id(N: String)
  object Id   {
    implicit val decoder: JsonDecoder[Id] = DeriveJsonDecoder.gen[Id]
    implicit val encoder: JsonEncoder[Id] = DeriveJsonEncoder.gen[Id]
  }
  final case class Book(id: Id)
  object Book {
    implicit val decoder: JsonDecoder[Book] = DeriveJsonDecoder.gen[Book]
    implicit val encoder: JsonEncoder[Book] = DeriveJsonEncoder.gen[Book]
  }

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
      endToEndSuite
    )
  val encoderSuite                                         = suite("encoder suite")(
    test("encode top level map") {
      val avMap = AttributeValue.Map.empty +
        ("id"     -> AttributeValue.String("101")) +
        ("count"  -> AttributeValue.Number(BigDecimal(42))) +
        ("isTest" -> AttributeValue.Bool(true))
      val encoded = JsonCodec.Encoder.encode(avMap)
      val s       = encoded.toJson
      println(s"XXXXXXXXXX encoded: $s")
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
      val encoded = JsonCodec.Encoder.encode(avMap)
      val s       = encoded.toJson
      println(s"XXXXXXXXXX encoded: $s")
      assert(encoded)(
        equalTo(
          Json.Obj(
            "foo" -> Json.Obj("name" -> Json.Obj("S" -> Json.Str("Avi")))
          )
        )
      )
    }
  )
  val decoderSuite                                         = suite("decoder suite")(
    test("decode top level map") {
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
              "id": {
                  "S": "101"
              },
              "stringSet": {
                  "SS": ["1", "2"]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("id"        -> AttributeValue.String("101")) +
              ("stringSet" -> AttributeValue.StringSet(Set("1", "2")))
          )
        )
      )
    },
    test("decode NS") {
      val s   =
        """{
              "id": {
                  "S": "101"
              },
              "stringSet": {
                  "NS": ["1", "2"]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("id"        -> AttributeValue.String("101")) +
              ("stringSet" -> AttributeValue.NumberSet(Set(BigDecimal(1), BigDecimal(2))))
          )
        )
      )
    },
    test("decode M of object") {
      val s   =
        """{
              "id": {
                  "S": "101"
              },
              "map": {
                  "M": { "1": {"foo": {"S": "bar"}}, "2": {"foo": {"S": "baz"}} }
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("id"  -> AttributeValue.String("101")) +
              ("map" -> (AttributeValue.Map.empty + ("1" -> obj("bar")) + ("2" -> obj("baz"))))
          )
        )
      )
    },
    test("decode L of string") {
      val s   =
        """{
              "id": {
                  "S": "101"
              },
              "array": {
                  "L": ["1", "2"]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)
      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("id"    -> AttributeValue.String("101")) +
              ("array" -> AttributeValue.List(List(AttributeValue.String("1"), AttributeValue.String("2"))))
          )
        )
      )
    },
    test("decode L of object") {
      val s   =
        """{
              "id": {
                  "S": "101"
              },
              "array": {
                  "L": [{"foo": {"S": "bar"}},  {"foo": {"S": "baz"}}]
              }
          }"""
      val ast = s.fromJson[Json].getOrElse(Json.Null)

      assert(decode(ast))(
        equalTo(
          Right(
            AttributeValue.Map.empty +
              ("id"    -> AttributeValue.String("101")) +
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
  val avToAttrMapSuite                                     = suite("AttributeValue to AttrVal")(
    // TODO: remove JSON strings
    test("AV of top level only to AttrMap") {
      val ast: Json = Json.Obj(
        "id"    -> Json.Obj("S" -> Json.Str("101")),
        "count" -> Json.Obj("N" -> Json.Str("42"))
      )
      decode(ast).flatMap(toAttrMap) match {
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
      decode(ast).flatMap(toAttrMap) match {
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
      decode(ast).flatMap(toAttrMap) match {
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
  val attrMapToAVSuite                                     = suite("AttrMap to AttributeValue")(
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
  val transformSuite                                       = suite("transform")(avToAttrMapSuite, attrMapToAVSuite)

  val endToEndSuite = suite("AttrMap end to end")(
    test("from AttrMap -> Json string -> AttrMap") {
      import zio.dynamodb.JsonCodec._

      val am = AttrMap.empty +
        ("id"     -> AttributeValue.String("101")) +
        ("nested" -> obj("bar")) +
        ("count"  -> AttributeValue.Number(BigDecimal(101)))

      val jsonString = am.toJsonString

      parse(jsonString) match {
        case Right(am2) =>
          assertTrue(am2 == am)
        case _          => assertTrue(false)
      }
    }
  )

  def obj(value: String) = AttributeValue.Map.empty + ("foo" -> AttributeValue.String(value))

}

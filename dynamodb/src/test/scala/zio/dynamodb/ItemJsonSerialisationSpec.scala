package zio.dynamodb

import zio.json._
import zio.json.ast.Json
import zio.Chunk
import zio.test.ZIOSpecDefault
import zio.Scope
import zio.test.Spec
import zio.test.TestEnvironment
import zio.test.{ assert, assertTrue }
import zio.test.Assertion.equalTo
import scala.util.Try
//import zio.prelude._

object ItemJsonSerialisationSpec extends ZIOSpecDefault {

  val s1 = """{
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

  def createMap(fields: Chunk[(String, Json)], map: AttributeValue.Map): Either[String, AttributeValue.Map] =
    fields.toList match {
      case Nil            =>
        Right(map)
      case (k, json) :: _ =>
        decode(json) match {
          case Right(av) => createMap(fields.tail, map + (k -> av))
          case Left(err) => Left(err)
        }
    }

  def decodeSS(xs: List[Json], acc: AttributeValue.StringSet): Either[String, AttributeValue.StringSet] =
    xs match {
      case Nil       => Right(acc)
      case json :: _ =>
        json match {
          case Json.Str(s) =>
            val x: AttributeValue.StringSet = acc + s
            decodeSS(xs.tail, x)
          case x           => Left(s"Invalid SS $x")
        }
    }

  def decodeL(xs: List[Json], acc: AttributeValue.List): Either[String, AttributeValue.List] =
    xs match {
      case Nil       => Right(acc)
      case json :: _ =>
        decode(json) match {
          case Right(av) => decodeL(xs.tail, acc + av)
          case Left(err) => Left(err)
        }
    }

  def decode(json: Json): Either[String, AttributeValue] =
    json match {
      case Json.Obj(Chunk("N" -> Json.Str(d)))      =>
        Try(BigDecimal(d)).fold(
          _ => Left(s"Invalid Number $d"),
          n => Right(AttributeValue.Number(n))
        )
      case Json.Obj(Chunk("S" -> Json.Str(s)))      => Right(AttributeValue.String(s))
      case Json.Obj(Chunk("B" -> Json.Bool(b)))     => Right(AttributeValue.Bool(b))
      case Json.Obj(Chunk("L" -> Json.Arr(a)))      => decodeL(a.toList, AttributeValue.List.empty)
      case Json.Obj(Chunk("SS" -> Json.Arr(chunk))) => decodeSS(chunk.toList, AttributeValue.StringSet.empty)
//      case Json.Obj(Chunk(_ -> a))              => Left(s"TODO ${a.getClass.getName} $a")

      case Json.Obj(fields) if fields.isEmpty       => Left("empty AttributeValue Map found")
      case Json.Obj(fields)                         => // returns an  AttributeValue.Map
        createMap(fields, AttributeValue.Map.empty)
      // TODO:
      case Json.Str(s)                              => Right(AttributeValue.String(s))
      case a                                        => Left(s"Only top level objects are supported, found $a")
    }

  def translate(fields: List[(AttributeValue.String, AttributeValue)], acc: AttrMap): AttrMap =
    fields match {
      case Nil                                  =>
        acc
      case (s, av @ AttributeValue.Map(m)) :: _ =>
        translate(
          m.toList.tail,
          AttrMap.empty + (s.value -> av)
        ) // AttrMap value posn is an AV and we already have one in hand
      case (s, av) :: _                         =>
        translate(fields.tail, acc + (s.value -> av))

    }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ItemJsonSerialisationSpec")(
      test("decode top level map") {
        val s   =
          """{
              "id": {
                  "S": "101"
              },
              "count": {
                  "N": "42"
              }
          }"""
        val ast = s.fromJson[Json].getOrElse(Json.Null)
        assert(decode(ast))(
          equalTo(
            Right(
              AttributeValue.Map.empty +
                ("id"    -> AttributeValue.String("101")) +
                ("count" -> AttributeValue.Number(BigDecimal(42)))
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
        val s                  =
          """{
              "id": {
                  "S": "101"
              },
              "array": {
                  "L": [{"foo": {"S": "bar"}},  {"foo": {"S": "baz"}}]
              }
          }"""
        val ast                = s.fromJson[Json].getOrElse(Json.Null)
        def obj(value: String) = AttributeValue.Map.empty + ("foo" -> AttributeValue.String(value))

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
            Left("Only top level objects are supported, found []")
          )
        )
      },
      test("translate top level map") {
        val s   =
          """{
              "id": {
                  "S": "101"
              },
              "count": {
                  "N": "42"
              }
          }"""
        val ast = s.fromJson[Json].getOrElse(Json.Null)
        val x   = decode(ast) //.map(m => translate2(m.value.toList, AttrMap.empty))
        x match {
          case Right(AttributeValue.Map(map)) =>
            assertTrue(
              translate(map.toList, AttrMap.empty) == AttrMap.empty + ("id" -> AttributeValue.String(
                "101"
              )) + ("count"                                                 -> AttributeValue.Number(BigDecimal(42)))
            )
          case _                              => assertTrue(false)
        }

      },
      test("translate nested map") {
        val s   =
          """{
              "foo": {
                    "name": {
                       "S": "Avi"
                     }                    
              }
          }"""
        val ast = s.fromJson[Json].getOrElse(Json.Null)
        val x   = decode(ast) //.map(m => translate2(m.value.toList, AttrMap.empty))
        x match {
          case Right(AttributeValue.Map(map)) =>
            val translated = translate(map.toList, AttrMap.empty)
            val nestedAv   = AttributeValue.Map.empty + ("name" -> AttributeValue.String("Avi"))
            val expected   = AttrMap.empty + ("foo"             -> nestedAv)
            assertTrue(translated == expected)
          case _                              => assertTrue(false)
        }
        assertTrue(true)
      }
    )

}

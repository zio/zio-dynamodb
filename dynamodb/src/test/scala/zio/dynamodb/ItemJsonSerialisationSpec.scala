package zio.dynamodb

import zio.json._
import zio.json.ast.Json
import zio.Chunk

object ItemJsonSerialisationSpec {
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

  val s2 =
    """{
    "id": {
        "N": "101"
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

  val x = s2.fromJson[Json]

  val out: Any = x match {
    case Left(err)   => s"Error: $err"
    case Right(json) => decode(json)
  }

  def decode(json: Json): Any =
    json match {
      case Json.Obj(fields) => fields.foreach { case (k, v) => Map(k -> decode(v)) }
      case Json.Arr(fields) => fields.foreach { case json => decode(json) }
      case Json.Bool(b)     => b
      case Json.Null        => "Null"
      case Json.Num(d)      => d
      case Json.Str(s)      => s
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
   */

  /*
   {
     "id":
        { "N": "101" } // => AttributeValue.Number
   }
   - JSON has top level fields - DDB does not
   */

  def isPrimitive(fields: Chunk[(String, Json)]) = ???

  def primitive(k: String, json: Json): AttributeValue = ???

  def decode2(json: Json, map: AttributeValue.Map): AttributeValue =
    json match {
      case Json.Obj(fields) if isPrimitive(fields) =>
        primitive(fields.head._1, fields.head._2)
      case Json.Obj(fields)                        =>
        fields.foldRight(map) {
          case ((k, json), m) => map + (k -> decode2(json, m))
        } //fields.foreach { case (k, v) => map + (k -> decode2(v, map)) }
      case Json.Arr(_ /*fields*/ )                 => AttributeValue.Null //fields.foreach { case json => decode(json) }
      case Json.Bool(b)                            => AttributeValue.Bool(b)
      case Json.Null                               => AttributeValue.Null
      case Json.Num(d)                             => AttributeValue.Number(d)
      case Json.Str(s)                             => AttributeValue.String(s)
    }

  println(out)

}

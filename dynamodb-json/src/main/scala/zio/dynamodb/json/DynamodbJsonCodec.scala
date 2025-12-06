package zio.dynamodb.json

import zio.Chunk
import zio.dynamodb._
import zio.json._
import zio.json.ast.Json

import scala.util.Try

private[dynamodb] object DynamodbJsonCodec {

  object Encoder {
    def encode(av: AttributeValue): Json =
      av match {
        // Note a Number AttributeValue is represented as a Json.Str
        case AttributeValue.String(s)     => Json.Obj(Chunk("S" -> Json.Str(s)))
        case AttributeValue.Number(n)     => Json.Obj(Chunk("N" -> Json.Str(n.toString)))
        case AttributeValue.Bool(b)       => Json.Obj(Chunk("BOOL" -> Json.Bool(b)))
        case AttributeValue.Null          => Json.Obj(Chunk("NULL" -> Json.Null))
        case AttributeValue.List(xs)      =>
          Json.Obj(Chunk("L" -> Json.Arr(xs.map(encode).toList: _*)))
        case AttributeValue.StringSet(xs) => Json.Obj(Chunk("SS" -> Json.Arr(xs.map(Json.Str(_)).toList: _*)))
        case AttributeValue.NumberSet(xs) =>
          Json.Obj(Chunk("NS" -> Json.Arr(xs.map(n => Json.Str(n.toString)).toList: _*)))
        case AttributeValue.Map(map)      =>
          val xs: List[(AttributeValue.String, Json)] = map.map { case (k, v) => k -> encode(v) }.toList
          Json.Obj(Chunk(xs.map { case (k, v) => k.value -> v }: _*))
        case AttributeValue.Binary(_)     => ???
        case AttributeValue.BinarySet(_)  => ???
      }

    def attributeValueToJsonString(av: AttributeValue): String = encode(av).toJson

    def attributeValueToJsonStringPretty(av: AttributeValue): String = encode(av).toJsonPretty
  }
  object Decoder {
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
              val ss: AttributeValue.StringSet = acc + s
              decodeSS(xs.tail, ss)
            case json        => Left(s"Invalid SS value $json, expected a string value")
          }
      }

    def decodeNS(xs: List[Json], acc: AttributeValue.NumberSet): Either[String, AttributeValue.NumberSet] =
      xs match {
        case Nil       => Right(acc)
        case json :: _ =>
          json match {
            case Json.Str(s) =>
              (acc + s).flatMap(decodeNS(xs.tail, _))
            case json        => Left(s"Invalid NS value $json, expected a string number")
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
        // Note a Number AttributeValue is represented as a Json.Str
        case Json.Obj(Chunk(("N", Json.Str(d))))       =>
          Try(BigDecimal(d)).fold(
            _ => Left(s"Invalid Number $d"),
            n => Right(AttributeValue.Number(n))
          )
        case Json.Obj(Chunk(("S", Json.Str(s))))       => Right(AttributeValue.String(s))
        case Json.Obj(Chunk((("BOOL", Json.Bool(b))))) => Right(AttributeValue.Bool(b))
        case Json.Obj(Chunk(("NULL", Json.Null)))      => Right(AttributeValue.Null)
        case Json.Obj(Chunk(("L", Json.Arr(a))))       => decodeL(a.toList, AttributeValue.List.empty)
        case Json.Obj(Chunk(("SS", Json.Arr(chunk))))  => decodeSS(chunk.toList, AttributeValue.StringSet.empty)
        case Json.Obj(Chunk(("NS", Json.Arr(chunk))))  => decodeNS(chunk.toList, AttributeValue.NumberSet.empty)
        case Json.Obj(Chunk(("M", Json.Obj(fields))))  => createMap(fields, AttributeValue.Map.empty)
        case b @ Json.Obj(Chunk(("B", _)))             => Left(s"The Binary type is not supported yet, found: $b")
        case bs @ Json.Obj(Chunk(("BS", _)))           => Left(s"The Binary Set type is not supported yet, found: $bs")
        case Json.Obj(fields) if fields.isEmpty        => Left("empty AttributeValue Map found")
        case Json.Obj(fields)                          =>
          createMap(fields, AttributeValue.Map.empty)
        // for collections
        case Json.Str(s)                               => Right(AttributeValue.String(s))
        case Json.Bool(b)                              => Right(AttributeValue.Bool(b))
        case Json.Null                                 => Right(AttributeValue.Null)
        // Note Json.Num is handled via Json.Str
        case n @ Json.Num(_)                           => Left(s"Unexpected Num $n")

        case a @ Json.Arr(_) => Left(s"top level arrays are not supported, found $a")
      }

    def jsonStringToAttributeValue(json: String): Either[String, AttributeValue] =
      json.fromJson[Json] match {
        case Left(err)   => Left(err)
        case Right(json) => Decoder.decode(json)
      }
  }

}

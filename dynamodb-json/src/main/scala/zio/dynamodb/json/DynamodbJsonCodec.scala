package zio.dynamodb.json

import zio.Chunk
import zio.dynamodb._
import zio.json._
import zio.json.ast.Json

import scala.util.Try

private[dynamodb] object DynamodbJsonCodec {
  private val ddbTags: Set[String] =
    Set("S", "N", "BOOL", "NULL", "L", "SS", "NS", "M", "B", "BS")

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

        // 🔑 Centralised envelope handling
        case Json.Obj(fields) if isDdbEnvelope(fields) =>
          decodeEnvelope(fields)

        // Plain map fallback
        case Json.Obj(fields)                          =>
          createMap(fields, AttributeValue.Map.empty)

        // Scalars
        case Json.Str(s)                               => Right(AttributeValue.String(s))
        case Json.Bool(b)                              => Right(AttributeValue.Bool(b))
        case Json.Null                                 => Right(AttributeValue.Null)

        case n @ Json.Num(_) =>
          Left(s"Unexpected Num $n")

        case a @ Json.Arr(_) =>
          Left(s"top level arrays are not supported, found $a")
      }

    private def decodeEnvelope(
      fields: Chunk[(String, Json)]
    ): Either[String, AttributeValue]                                 =
      fields.head match {
        case ("S", Json.Str(s))       =>
          Right(AttributeValue.String(s))

        case ("N", Json.Str(d))       =>
          Try(BigDecimal(d)).toEither.left
            .map(_ => s"Invalid Number $d")
            .map(AttributeValue.Number(_))

        case ("BOOL", Json.Bool(b))   =>
          Right(AttributeValue.Bool(b))

        case ("NULL", Json.Null)      =>
          Right(AttributeValue.Null)

        case ("L", Json.Arr(values))  =>
          decodeL(values.toList, AttributeValue.List.empty)

        case ("SS", Json.Arr(values)) =>
          decodeSS(values.toList, AttributeValue.StringSet.empty)

        case ("NS", Json.Arr(values)) =>
          decodeNS(values.toList, AttributeValue.NumberSet.empty)

        case ("M", Json.Obj(fields))  =>
          createMap(fields, AttributeValue.Map.empty)

        case ("B", _)                 =>
          Left("Binary type not supported")

        case ("BS", _)                =>
          Left("Binary Set type not supported")

        case (k, v)                   =>
          Left(s"Invalid DDB envelope $k -> $v")
      }
    private def isDdbEnvelope(fields: Chunk[(String, Json)]): Boolean =
      fields.length == 1 &&
        fields.headOption.exists {
          case (k, v) =>
            ddbTags.contains(k) && isValidDdbValue(k, v)
        }
    private def isValidDdbValue(tag: String, json: Json): Boolean     =
      (tag, json) match {
        case ("S", Json.Str(_))     => true
        case ("N", Json.Str(_))     => true
        case ("BOOL", Json.Bool(_)) => true
        case ("NULL", Json.Null)    => true

        case ("L", Json.Arr(values))  =>
          values.forall(looksLikeAttributeValue)

        case ("SS", Json.Arr(values)) =>
          values.forall(_.isInstanceOf[Json.Str])

        case ("NS", Json.Arr(values)) =>
          values.forall(_.isInstanceOf[Json.Str])

        case ("M", Json.Obj(fields))  =>
          fields.forall { case (_, v) => looksLikeAttributeValue(v) }

        // unsupported but still valid envelope
        case ("B" | "BS", _)          =>
          true

        case _                        => false
      }

    private def looksLikeAttributeValue(json: Json): Boolean =
      json match {
        case Json.Obj(Chunk((k, _))) => ddbTags.contains(k)
        case _                       => false
      }

    def jsonStringToAttributeValue(json: String): Either[String, AttributeValue] =
      json.fromJson[Json] match {
        case Left(err)   => Left(err)
        case Right(json) => Decoder.decode(json)
      }
  }

}

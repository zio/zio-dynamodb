package zio.dynamodb

import zio.json.ast.Json
import zio.Chunk
import scala.util.Try

// TODO: note sure where to put this yet
object JsonCodec {
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
              val x: AttributeValue.StringSet = acc + s
              decodeSS(xs.tail, x)
            case x           => Left(s"Invalid SS $x")
          }
      }

    def decodeNS(xs: List[Json], acc: AttributeValue.NumberSet): Either[String, AttributeValue.NumberSet] =
      xs match {
        case Nil       => Right(acc)
        case json :: _ =>
          json match {
            case Json.Str(s) =>
              (acc + s).flatMap(decodeNS(xs.tail, _))
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
        // Note Json.Num is handles via Json.Str
        case Json.Obj(Chunk("BOOL" -> Json.Bool(b)))  => Right(AttributeValue.Bool(b))
        case Json.Obj(Chunk("L" -> Json.Arr(a)))      => decodeL(a.toList, AttributeValue.List.empty)
        case Json.Obj(Chunk("SS" -> Json.Arr(chunk))) => decodeSS(chunk.toList, AttributeValue.StringSet.empty)
        case Json.Obj(Chunk("NS" -> Json.Arr(chunk))) => decodeNS(chunk.toList, AttributeValue.NumberSet.empty)
        case Json.Obj(Chunk("M" -> Json.Obj(fields))) => createMap(fields, AttributeValue.Map.empty)
//      case Json.Obj(Chunk(_ -> a))              => Left(s"TODO ${a.getClass.getName} $a")

        case Json.Obj(fields) if fields.isEmpty       => Left("empty AttributeValue Map found")
        case Json.Obj(fields)                         =>
          createMap(fields, AttributeValue.Map.empty)
        // for collections
        case Json.Str(s)                              => Right(AttributeValue.String(s))
        case Json.Bool(b)                             => Right(AttributeValue.Bool(b))
        // Note Json.Num is handled via Json.Str
        case a @ Json.Arr(_)                          => Left(s"top level arrays are not supported, found $a")
        case x                                        => Left(s"$x is not supported") // TODO
      }

    def toAttrMap(fields: List[(AttributeValue.String, AttributeValue)]): AttrMap =
      fields.map { case (avStr -> av) => avStr.value -> av }.foldLeft(AttrMap.empty)(_ + _)

  }
}

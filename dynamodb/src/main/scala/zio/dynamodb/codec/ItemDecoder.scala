package zio.dynamodb.codec

import zio.Chunk
import zio.dynamodb.{ AttributeValue, FromAttributeValue, Item, ToAttributeValue }
import zio.schema.Schema.{ Optional, Primitive }
import zio.schema.{ Schema, StandardType }

import java.time.{ DayOfWeek, Duration, Instant, LocalDate, LocalDateTime, LocalTime }
import scala.util.Try

object ItemDecoder {

  type Decoder[+A] = AttributeValue => Either[String, A]

  def fromItem[A](item: Item)(implicit schema: Schema[A]): Either[String, A] = {
    val av = ToAttributeValue.attrMapToAttributeValue.toAttributeValue(item)
    decoder(schema)(av)
  }

  def decoder[A](schema: Schema[A]): Decoder[A] =
    schema match {
      case ProductDecoder(decoder)       =>
        decoder
      case s: Optional[a]                =>
        optionalDecoder[a](decoder(s.codec))
      case Schema.Tuple(l, r)            =>
        tupleDecoder(decoder(l), decoder(r))
      case Schema.Transform(codec, f, _) =>
        transformDecoder(codec, f)
      case s: Schema.Sequence[col, a]    =>
        sequenceDecoder[col, a](decoder(s.schemaA), s.fromChunk)
      case Schema.EitherSchema(l, r)     =>
        eitherDecoder(decoder(l), decoder(r))
      case Primitive(standardType)       =>
        primitiveDecoder(standardType)
      case l @ Schema.Lazy(_)            =>
        decoder(l.schema)
      case Schema.Enum1(c)               =>
        enumDecoder(c)
      case Schema.Enum2(c1, c2)          =>
        enumDecoder(c1, c2)
      case Schema.Enum3(c1, c2, c3)      =>
        enumDecoder(c1, c2, c3)
      case _                             =>
        throw new UnsupportedOperationException(s"schema $schema not yet supported")
    }

  object ProductDecoder {
    def unapply[A](schema: Schema[A]): Option[Decoder[A]] =
      schema match {
        case s @ Schema.CaseClass1(_, _, _, _)             =>
          Some(caseClass1Decoder(s))
        case s @ Schema.CaseClass2(_, _, _, _, _, _)       =>
          Some(caseClass2Decoder(s))
        case s @ Schema.CaseClass3(_, _, _, _, _, _, _, _) =>
          Some(caseClass3Decoder(s))
        case _                                             =>
          None
      }
  }

  private def caseClass1Decoder[A1, Z](schema: Schema.CaseClass1[A1, Z]): Decoder[Z] = { (av: AttributeValue) =>
    decodeFields(av, schema.field).map { xs =>
      schema.construct(xs(0).asInstanceOf[A1])
    }
  }

  private def caseClass2Decoder[A1, A2, Z](schema: Schema.CaseClass2[A1, A2, Z]): Decoder[Z] = { (av: AttributeValue) =>
    decodeFields(av, schema.field1, schema.field2).map { xs =>
      schema.construct(xs(0).asInstanceOf[A1], xs(1).asInstanceOf[A2])
    }
  }

  private def caseClass3Decoder[A1, A2, A3, Z](schema: Schema.CaseClass3[A1, A2, A3, Z]): Decoder[Z] = {
    (av: AttributeValue) =>
      decodeFields(av, schema.field1, schema.field2, schema.field3).map { xs =>
        schema.construct(xs(0).asInstanceOf[A1], xs(1).asInstanceOf[A2], xs(2).asInstanceOf[A3])
      }
  }

  private def decodeFields(av: AttributeValue, fields: Schema.Field[_]*): Either[String, List[Any]] =
    av match {
      case AttributeValue.Map(map) =>
        zio.dynamodb
          .foreach(fields) {
            case Schema.Field(key, schema, _) =>
              val dec        = decoder(schema)
              val maybeValue = map.get(AttributeValue.String(key))
              maybeValue.map(dec).toRight(s"field '$key' not found in $av").flatten
          }
          .map(_.toList)
      case _                       =>
        Left(s"$av is not an AttributeValue.Map")
    }

  private def primitiveDecoder[A](standardType: StandardType[A]): Decoder[A] =
    standardType match {
      case StandardType.BoolType                 =>
        (av: AttributeValue) =>
          FromAttributeValue.booleanFromAttributeValue.fromAttributeValue(av).toRight("error getting boolean")
      case StandardType.StringType               =>
        (av: AttributeValue) =>
          FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).toRight("error getting string")
      case StandardType.ShortType                =>
        (av: AttributeValue) =>
          FromAttributeValue.shortFromAttributeValue.fromAttributeValue(av).toRight("error getting short")
      case StandardType.IntType                  =>
        (av: AttributeValue) =>
          FromAttributeValue.intFromAttributeValue.fromAttributeValue(av).toRight("error getting int")
      case StandardType.LongType                 =>
        (av: AttributeValue) =>
          FromAttributeValue.longFromAttributeValue.fromAttributeValue(av).toRight("error getting long")
      case StandardType.FloatType                =>
        (av: AttributeValue) =>
          FromAttributeValue.floatFromAttributeValue.fromAttributeValue(av).toRight("error getting float")
      case StandardType.DoubleType               =>
        (av: AttributeValue) =>
          FromAttributeValue.doubleFromAttributeValue.fromAttributeValue(av).toRight("error getting double")
      case StandardType.BigDecimalType           =>
        (av: AttributeValue) =>
          FromAttributeValue.bigDecimalFromAttributeValue
            .fromAttributeValue(av)
            .toRight("error getting big decimal")
            .map(_.bigDecimal)
      case StandardType.BigIntegerType           =>
        (av: AttributeValue) =>
          FromAttributeValue.bigDecimalFromAttributeValue
            .fromAttributeValue(av)
            .toRight("error getting big integer")
            .map(_.toBigInt.bigInteger)
      case StandardType.BinaryType               =>
        (av: AttributeValue) =>
          FromAttributeValue.binaryFromAttributeValue
            .fromAttributeValue(av)
            .toRight("error getting iterable of byte")
            .map(Chunk.fromIterable(_))
      case StandardType.CharType                 =>
        (av: AttributeValue) =>
          FromAttributeValue.stringFromAttributeValue
            .fromAttributeValue(av)
            .toRight("error getting iterable of byte")
            .map { s =>
              val array = s.toCharArray
              array(0)
            }
      case StandardType.DayOfWeekType            =>
        (av: AttributeValue) => avStringParser(av)(DayOfWeek.valueOf(_))
      case StandardType.Duration(_)              =>
        (av: AttributeValue) => avStringParser(av)(Duration.parse(_))
      case StandardType.Instant(formatter)       =>
        (av: AttributeValue) => avStringParser(av)(formatter.parse(_, Instant.from(_)))
      case StandardType.LocalDate(formatter)     =>
        (av: AttributeValue) => avStringParser(av)(formatter.parse(_, LocalDate.from(_)))
      case StandardType.LocalDateTime(formatter) =>
        (av: AttributeValue) => avStringParser(av)(formatter.parse(_, LocalDateTime.from(_)))
      case StandardType.LocalTime(formatter)     =>
        (av: AttributeValue) => avStringParser(av)(formatter.parse(_, LocalTime.from(_)))
      case StandardType.UnitType                 => _ => Right(())
      case _                                     => // TODO: remove after full implementation
        throw new UnsupportedOperationException(s"standardType $standardType not yet supported")

    }

  private def avStringParser[A](av: AttributeValue)(unsafeParse: String => A): Either[String, A] =
    FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).toRight("error getting string").flatMap { s =>
      Try(unsafeParse(s)).toEither.left
        .map(e => s"error parsing string '$s': ${e.getMessage}")
    }

  private def transformDecoder[A, B](codec: Schema[A], f: A => Either[String, B]): Decoder[B] = {
    val dec = decoder(codec)
    (a: AttributeValue) => dec(a).flatMap(f)
  }

  private def optionalDecoder[A](decoder: Decoder[A]): Decoder[Option[A]] = {
    case AttributeValue.Null => Right(None)
    case av                  => decoder(av).map(Some(_))
  }

  private def eitherDecoder[A, B](decL: Decoder[A], decR: Decoder[B]): Decoder[Either[A, B]] = {
    case AttributeValue.Map(map) =>
      map.toList match {
        case (AttributeValue.String("Left"), a) :: Nil  =>
          decL(a).map(Left(_))
        case (AttributeValue.String("Right"), b) :: Nil =>
          decR(b).map(Right(_))
        case av                                         =>
          Left(s"AttributeValue.Map map element $av not expected.")
      }
    case av                      => Left(s"Expected AttributeValue.Map but found $av")
  }

  private def tupleDecoder[A, B](decL: Decoder[A], decR: Decoder[B]): Decoder[(A, B)] = {

    case AttributeValue.List(avA :: avB :: Nil) =>
      for {
        a <- decL(avA)
        b <- decR(avB)
      } yield (a, b)
    case av                                     =>
      Left(s"Expected an AttributeValue.List of two elements but found $av")
  }

  private def sequenceDecoder[Col[_], A](decoder: Decoder[A], to: Chunk[A] => Col[A]): Decoder[Col[A]] = {
    case AttributeValue.List(list) =>
      zio.dynamodb.foreach(list)(decoder(_)).map(xs => to(Chunk.fromIterable(xs)))
    case av                        => Left(s"unable to decode $av as a list")
  }

  private def enumDecoder[A](cases: Schema.Case[_, A]*): Decoder[A] =
    (av: AttributeValue) =>
      av match {
        case AttributeValue.Map(map) => // TODO: assume Map is ListMap for now
          map.toList.headOption.fold[Either[String, A]](Left(s"map $av is empty")) {
            case (AttributeValue.String(subtype), av) =>
              cases.find(_.id == subtype) match {
                case Some(c) =>
                  decoder(c.codec)(av).map(_.asInstanceOf[A])
                case None    =>
                  Left(s"subtype $subtype not found")
              }
          }
        case _                       =>
          Left(s"invalid AttributeValue $av")
      }

}

package zio.dynamodb

import zio.schema.Schema.{ Optional, Primitive }
import zio.schema.{ Schema, SchemaAst, StandardType }
import zio.{ schema, Chunk }

import java.time.{ ZoneId, _ }
import scala.util.Try

private[dynamodb] object Decoder {

  def apply[A](schema: Schema[A]): Decoder[A] = decoder(schema)

  private def decoder[A](schema: Schema[A]): Decoder[A] =
    schema match {
      case ProductDecoder(decoder)         => decoder
      case s: Optional[a]                  => optionalDecoder[a](decoder(s.codec))
      case Schema.Fail(s)                  => _ => Left(s)
      case Schema.GenericRecord(structure) => genericRecordDecoder(structure).asInstanceOf[Decoder[A]]
      case Schema.Enumeration(structure)   => enumerationDecoder(structure)
      case Schema.Tuple(l, r)              => tupleDecoder(decoder(l), decoder(r))
      case Schema.Transform(codec, f, _)   => transformDecoder(codec, f)
      case s: Schema.Sequence[col, a]      =>
        sequenceDecoder[col, a](decoder(s.schemaA), s.fromChunk)
      case Schema.EitherSchema(l, r)       =>
        eitherDecoder(decoder(l), decoder(r))
      case Primitive(standardType)         =>
        primitiveDecoder(standardType)
      case l @ Schema.Lazy(_)              =>
        decoder(l.schema)
      case Schema.Meta(_)                  => astDecoder
      case Schema.Enum1(c)                 =>
        enumDecoder(c)
      case Schema.Enum2(c1, c2)            =>
        enumDecoder(c1, c2)
      case Schema.Enum3(c1, c2, c3)        =>
        enumDecoder(c1, c2, c3)
      case Schema.EnumN(cs)                => enumDecoder(cs: _*)
    }

  private val astDecoder: Decoder[Schema[_]] =
    (av: AttributeValue) => decoder(Schema[SchemaAst])(av).map(_.toSchema)

  private def enumerationDecoder(structure: Map[String, Schema[_]]): Decoder[(String, Any)] = {
    case AttributeValue.List(AttributeValue.String(s) :: av :: Nil) =>
      decoder(structure(s))(av).map((s, _))
    case av                                                         => Left(s"Unexpected AttributeValue type $av")
  }

  private def genericRecordDecoder(structure: Chunk[schema.Schema.Field[_]]): Decoder[Any] =
    (av: AttributeValue) =>
      av match {
        case AttributeValue.Map(map) =>
          zio.dynamodb
            .foreach[schema.Schema.Field[_], (String, Any)](structure) {
              case Schema.Field(key, schema: Schema[a], _) =>
                val av  = map(AttributeValue.String(key))
                val dec = decoder(schema)
                dec(av) match {
                  case Right(value) => Right(key -> value)
                  case Left(s)      => Left(s)
                }
            }
            .map(_.toMap)
        case av                      => Left(s"Expected AttributeValue.Map but found $av")
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
      case StandardType.UnitType                  => _ => Right(())
      case StandardType.StringType                =>
        (av: AttributeValue) => FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av)
      case StandardType.BoolType                  =>
        (av: AttributeValue) => FromAttributeValue.booleanFromAttributeValue.fromAttributeValue(av)
      case StandardType.ShortType                 =>
        (av: AttributeValue) => FromAttributeValue.shortFromAttributeValue.fromAttributeValue(av)
      case StandardType.IntType                   =>
        (av: AttributeValue) => FromAttributeValue.intFromAttributeValue.fromAttributeValue(av)
      case StandardType.LongType                  =>
        (av: AttributeValue) => FromAttributeValue.longFromAttributeValue.fromAttributeValue(av)
      case StandardType.FloatType                 =>
        (av: AttributeValue) => FromAttributeValue.floatFromAttributeValue.fromAttributeValue(av)
      case StandardType.DoubleType                =>
        (av: AttributeValue) => FromAttributeValue.doubleFromAttributeValue.fromAttributeValue(av)
      case StandardType.BigDecimalType            =>
        (av: AttributeValue) =>
          FromAttributeValue.bigDecimalFromAttributeValue
            .fromAttributeValue(av)
            .map(_.bigDecimal)
      case StandardType.BigIntegerType            =>
        (av: AttributeValue) =>
          FromAttributeValue.bigDecimalFromAttributeValue
            .fromAttributeValue(av)
            .map(_.toBigInt.bigInteger)
      case StandardType.BinaryType                =>
        (av: AttributeValue) =>
          FromAttributeValue.binaryFromAttributeValue
            .fromAttributeValue(av)
            .map(Chunk.fromIterable(_))
      case StandardType.CharType                  =>
        (av: AttributeValue) =>
          FromAttributeValue.stringFromAttributeValue
            .fromAttributeValue(av)
            .map { s =>
              val array = s.toCharArray
              array(0)
            }
      case StandardType.DayOfWeekType             =>
        (av: AttributeValue) => javaTimeStringParser(av)(DayOfWeek.valueOf(_))
      case StandardType.Duration(_)               =>
        (av: AttributeValue) => javaTimeStringParser(av)(Duration.parse(_))
      case StandardType.Instant(formatter)        =>
        (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, Instant.from(_)))
      case StandardType.LocalDate(formatter)      =>
        (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, LocalDate.from(_)))
      case StandardType.LocalDateTime(formatter)  =>
        (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, LocalDateTime.from(_)))
      case StandardType.LocalTime(formatter)      =>
        (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, LocalTime.from(_)))
      case StandardType.Month                     =>
        (av: AttributeValue) => javaTimeStringParser(av)(Month.valueOf(_))
      case StandardType.MonthDay                  =>
        (av: AttributeValue) => javaTimeStringParser(av)(MonthDay.parse(_))
      case StandardType.OffsetDateTime(formatter) =>
        (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, OffsetDateTime.from(_)))
      case StandardType.OffsetTime(formatter)     =>
        (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, OffsetTime.from(_)))
      case StandardType.Period                    =>
        (av: AttributeValue) => javaTimeStringParser(av)(Period.parse(_))
      case StandardType.Year                      =>
        (av: AttributeValue) => javaTimeStringParser(av)(Year.parse(_))
      case StandardType.YearMonth                 =>
        (av: AttributeValue) => javaTimeStringParser(av)(YearMonth.parse(_))
      case StandardType.ZonedDateTime(formatter)  =>
        (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, ZonedDateTime.from(_)))
      case StandardType.ZoneId                    =>
        (av: AttributeValue) => javaTimeStringParser(av)(ZoneId.of(_))
      case StandardType.ZoneOffset                =>
        (av: AttributeValue) => javaTimeStringParser(av)(ZoneOffset.of(_))
    }

  private def javaTimeStringParser[A](av: AttributeValue)(unsafeParse: String => A): Either[String, A] =
    FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
      val stringOrA = Try(unsafeParse(s)).toEither.left
        .map(e => s"error parsing string '$s': ${e.getMessage}")
      if (stringOrA.isLeft)
        println(stringOrA)
      stringOrA
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

  private def tupleDecoder[A, B](decL: Decoder[A], decR: Decoder[B]): Decoder[(A, B)] =
    (av: AttributeValue) =>
      av match {
        case AttributeValue.List(list: Seq[AttributeValue]) if list.size == 2 =>
          val avA = list(0)
          val avB = list(1)
          for {
            a <- decL(avA)
            b <- decR(avB)
          } yield (a, b)
        case av                                                               =>
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

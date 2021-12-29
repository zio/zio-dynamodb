package zio.dynamodb

import zio.schema.Schema.{ Optional, Primitive }
import zio.schema.ast.SchemaAst
import zio.schema.{ FieldSet, Schema, StandardType }
import zio.{ schema, Chunk }

import java.time._
import java.util.UUID
import scala.util.Try

private[dynamodb] object Decoder extends GeneratedCaseClassDecoders {

  def apply[A](schema: Schema[A]): Decoder[A] = decoder(schema)

  //scalafmt: { maxColumn = 400, optIn.configStyleArguments = false }
  private[dynamodb] def decoder[A](schema: Schema[A]): Decoder[A] =
    schema match {
      case s: Optional[a]                                                                                                                                                   => optionalDecoder[a](decoder(s.codec))
      case Schema.Fail(s, _)                                                                                                                                                => _ => Left(s)
      case Schema.GenericRecord(structure, _)                                                                                                                               => genericRecordDecoder(structure).asInstanceOf[Decoder[A]]
      case Schema.Tuple(l, r, _)                                                                                                                                            => tupleDecoder(decoder(l), decoder(r))
      case Schema.Transform(codec, f, _, _)                                                                                                                                 => transformDecoder(codec, f)
      case s: Schema.Sequence[col, a]                                                                                                                                       => sequenceDecoder[col, a](decoder(s.schemaA), s.fromChunk)
      case Schema.EitherSchema(l, r, _)                                                                                                                                     => eitherDecoder(decoder(l), decoder(r))
      case Primitive(standardType, _)                                                                                                                                       => primitiveDecoder(standardType)
      case l @ Schema.Lazy(_)                                                                                                                                               =>
        lazy val dec = decoder(l.schema)
        (av: AttributeValue) => dec(av)
      case Schema.Meta(_, _)                                                                                                                                                => astDecoder
      case Schema.MapSchema(ks, vs, _)                                                                                                                                      =>
        mapDecoder(ks, vs).asInstanceOf[Decoder[A]]
      case s @ Schema.CaseClass1(_, _, _, _)                                                                                                                                => caseClass1Decoder(s)
      case s @ Schema.CaseClass2(_, _, _, _, _, _)                                                                                                                          => caseClass2Decoder(s)
      case s @ Schema.CaseClass3(_, _, _, _, _, _, _, _)                                                                                                                    => caseClass3Decoder(s)
      case s @ Schema.CaseClass4(_, _, _, _, _, _, _, _, _, _)                                                                                                              => caseClass4Decoder(s)
      case s @ Schema.CaseClass5(_, _, _, _, _, _, _, _, _, _, _, _)                                                                                                        => caseClass5Decoder(s)
      case s @ Schema.CaseClass6(_, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                                                  => caseClass6Decoder(s)
      case s @ Schema.CaseClass7(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                                            => caseClass7Decoder(s)
      case s @ Schema.CaseClass8(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                                      => caseClass8Decoder(s)
      case s @ Schema.CaseClass9(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                                => caseClass9Decoder(s)
      case s @ Schema.CaseClass10(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                         => caseClass10Decoder(s)
      case s @ Schema.CaseClass11(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                   =>
        caseClass11Decoder(s)
      case s @ Schema.CaseClass12(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                             =>
        caseClass12Decoder(s)
      case s @ Schema.CaseClass13(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                       =>
        caseClass13Decoder(s)
      case s @ Schema
            .CaseClass14(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
        caseClass14Decoder(s)
      case s @ Schema
            .CaseClass15(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
        caseClass15Decoder(s)
      case s @ Schema.CaseClass16(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                     =>
        caseClass16Decoder(s)
      case s @ Schema.CaseClass17(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                               =>
        caseClass17Decoder(s)
      case s @ Schema.CaseClass18(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                         =>
        caseClass18Decoder(s)
      case s @ Schema.CaseClass19(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                   =>
        caseClass19Decoder(s)
      case s @ Schema.CaseClass20(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)             =>
        caseClass20Decoder(s)
      case s @ Schema.CaseClass21(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)       =>
        caseClass21Decoder(s)
      case s @ Schema.CaseClass22(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
        caseClass22Decoder(s)
      case Schema.Enum1(c, _)                                                                                                                                               => enumDecoder(c)
      case Schema.Enum2(c1, c2, _)                                                                                                                                          => enumDecoder(c1, c2)
      case Schema.Enum3(c1, c2, c3, _)                                                                                                                                      => enumDecoder(c1, c2, c3)
      case Schema.Enum4(c1, c2, c3, c4, _)                                                                                                                                  => enumDecoder(c1, c2, c3, c4)
      case Schema.Enum5(c1, c2, c3, c4, c5, _)                                                                                                                              => enumDecoder(c1, c2, c3, c4, c5)
      case Schema.Enum6(c1, c2, c3, c4, c5, c6, _)                                                                                                                          => enumDecoder(c1, c2, c3, c4, c5, c6)
      case Schema.Enum7(c1, c2, c3, c4, c5, c6, c7, _)                                                                                                                      => enumDecoder(c1, c2, c3, c4, c5, c6, c7)
      case Schema.Enum8(c1, c2, c3, c4, c5, c6, c7, c8, _)                                                                                                                  => enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8)
      case Schema.Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, _)                                                                                                              => enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9)
      case Schema.Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, _)                                                                                                        =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
      case Schema.Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, _)                                                                                                   =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
      case Schema.Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, _)                                                                                              =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
      case Schema.Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, _)                                                                                         =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
      case Schema.Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, _)                                                                                    =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
      case Schema.Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, _)                                                                               =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
      case Schema.Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, _)                                                                          =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
      case Schema.Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, _)                                                                     =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
      case Schema.Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, _)                                                                =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)
      case Schema.Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, _)                                                           =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)
      case Schema
            .Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, _) =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
      case Schema.Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, _)                                                 =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)
      case Schema.Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, _)                                            =>
        enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
      case Schema.EnumN(cs, _)                                                                                                                                              => enumDecoder(cs.toSeq: _*)

    }
  //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }

  private val astDecoder: Decoder[Schema[_]] =
    (av: AttributeValue) => decoder(Schema[SchemaAst])(av).map(_.toSchema)

  private def genericRecordDecoder(structure: FieldSet): Decoder[Any] =
    (av: AttributeValue) =>
      av match {
        case AttributeValue.Map(map) =>
          EitherUtil
            .forEach[schema.Schema.Field[_], (String, Any)](structure.toChunk) {
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
      case StandardType.UUIDType                  =>
        (av: AttributeValue) =>
          FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
            Try(UUID.fromString(s)).toEither.left.map(iae => s"Invalid UUID: ${iae.getMessage}")
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

  private def sequenceDecoder[Col, A](decoder: Decoder[A], to: Chunk[A] => Col): Decoder[Col] = {
    case AttributeValue.List(list) =>
      EitherUtil.forEach(list)(decoder(_)).map(xs => to(Chunk.fromIterable(xs)))
    case av                        => Left(s"unable to decode $av as a list")
  }

  private def mapDecoder[K, V](ks: Schema[K], vs: Schema[V]) =
    (ks match {
      case Schema.Primitive(standardType, _) =>
        if (isString(standardType))
          nativeMapDecoder(decoder(vs))
        else
          nonNativeMapDecoder(decoder(ks), decoder(vs))
      case _                                 =>
        nonNativeMapDecoder(decoder(ks), decoder(vs))
    })

  private def nativeMapDecoder[V](dec: Decoder[V]): Decoder[Map[String, V]] =
    (av: AttributeValue) => {
      av match {
        case AttributeValue.Map(map) =>
          val xs: Iterable[Either[String, (String, V)]] = map.map {
            case (k, v) =>
              dec(v) match {
                case Right(decV) => Right((k.value, decV))
                case Left(s)     => Left(s)
              }
          }
          EitherUtil.collectAll(xs).map(elems => Map.newBuilder[String, V].++=(elems).result())
        case av                      => Left(s"Error: expected AttributeValue.Map but found $av")
      }
    }

  def nonNativeMapDecoder[A, B](decA: Decoder[A], decB: Decoder[B]): Decoder[Map[A, B]] =
    (av: AttributeValue) => {
      av match {
        case AttributeValue.List(listOfAv) =>
          val x: Either[String, Iterable[(A, B)]] = EitherUtil.forEach(listOfAv) {
            case avList @ AttributeValue.List(_) =>
              tupleDecoder(decA, decB)(avList)
            case av                              =>
              Left(s"Error: expected AttributeValue.List but found $av")
          }
          x.map(elems => Map.newBuilder[A, B].++=(elems).result())
        case av                            => Left(s"Error: expected AttributeValue.List but found $av")
      }
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

  private def isString[A](standardType: StandardType[A]): Boolean =
    standardType match {
      case StandardType.StringType => true
      case _                       => false
    }
}

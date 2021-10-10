package zio.dynamodb

import zio.schema.{ Schema, SchemaAst, StandardType }
import zio.{ schema, Chunk }

import java.time.Year
import java.time.format.{ DateTimeFormatterBuilder, SignStyle }
import java.time.temporal.ChronoField.YEAR
import scala.annotation.tailrec
import scala.collection.immutable.ListMap

private[dynamodb] object Encoder {

  private val yearFormatter =
    new DateTimeFormatterBuilder().appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD).toFormatter

  def encoder[A](schema: Schema[A]): Encoder[A] =
    schema match {
      case ProductEncoder(encoder)         => encoder
      case s: Schema.Optional[a]           => optionalEncoder[a](encoder(s.codec))
      case Schema.Fail(_)                  => _ => AttributeValue.Null
      case Schema.Tuple(l, r)              => tupleEncoder(encoder(l), encoder(r))
      case s: Schema.Sequence[col, a]      => sequenceEncoder[col, a](encoder(s.schemaA), s.toChunk)
      case Schema.Transform(c, _, g)       => transformEncoder(c, g)
      case Schema.Primitive(standardType)  => primitiveEncoder(standardType)
      case Schema.GenericRecord(structure) => genericRecordEncoder(structure)
      case Schema.Enumeration(structure)   => enumerationEncoder(structure)
      case Schema.EitherSchema(l, r)       => eitherEncoder(encoder(l), encoder(r))
      case l @ Schema.Lazy(_)              => encoder(l.schema)
      case Schema.Meta(_)                  => astEncoder
      case Schema.Enum1(c)                 => enumEncoder(c)
      case Schema.Enum2(c1, c2)            => enumEncoder(c1, c2)
      case Schema.Enum3(c1, c2, c3)        => enumEncoder(c1, c2, c3)
      case Schema.EnumN(cs)                => enumEncoder(cs: _*)
    }

  private val astEncoder: Encoder[Schema[_]] =
    (schema: Schema[_]) => encoder(Schema[SchemaAst])(SchemaAst.fromSchema(schema))

  private def enumerationEncoder(structure: Map[String, Schema[_]]): Encoder[(String, _)] =
    (value: (String, _)) => {
      val (k, v)                  = value
      val s                       = structure(k).asInstanceOf[Schema[Any]]
      val enc                     = encoder(s)
      val encoded: AttributeValue = enc(v)
      AttributeValue.List(List(AttributeValue.String(k), encoded))
    }

  private def genericRecordEncoder(structure: Chunk[schema.Schema.Field[_]]): Encoder[ListMap[String, _]] =
    (valuesMap: ListMap[String, _]) => {
      structure.foldRight(AttributeValue.Map(Map.empty)) {
        case (Schema.Field(key, schema: Schema[a], _), avMap) =>
          val value              = valuesMap(key)
          val enc                = encoder[a](schema)
          val av: AttributeValue = enc(value.asInstanceOf[a])
          AttributeValue.Map(avMap.value + (AttributeValue.String(key) -> av))
      }
    }

  private object ProductEncoder {
    def unapply[A](schema: Schema[A]): Option[Encoder[A]] =
      schema match {
        case Schema.CaseClass1(_, field1, _, extractField1)                                               =>
          Some(caseClassEncoder(field1 -> extractField1))
        case Schema.CaseClass2(_, field1, field2, _, extractField1, extractField2)                        =>
          Some(caseClassEncoder(field1 -> extractField1, field2 -> extractField2))
        case Schema.CaseClass3(_, field1, field2, field3, _, extractField1, extractField2, extractField3) =>
          Some(caseClassEncoder(field1 -> extractField1, field2 -> extractField2, field3 -> extractField3))
        case _                                                                                            =>
          None
      }
  }

  private def caseClassEncoder[A](fields: (Schema.Field[_], A => Any)*): Encoder[A] =
    (a: A) => {
      val avMap: AttributeValue.Map = fields.foldRight[AttributeValue.Map](AttributeValue.Map(Map.empty)) {
        case ((Schema.Field(key, schema, _), ext), acc) =>
          val enc                 = encoder(schema)
          val extractedFieldValue = ext(a)
          val av                  = enc(extractedFieldValue)

          @tailrec
          def appendToMap[B](schema: Schema[B]): AttributeValue.Map =
            schema match {
              case l @ Schema.Lazy(_) =>
                appendToMap(l.schema)
              case _                  =>
                AttributeValue.Map(acc.value + (AttributeValue.String(key) -> av))
            }

          appendToMap(schema)
      }
      avMap
    }

  private def primitiveEncoder[A](standardType: StandardType[A]): Encoder[A] =
    standardType match {
      case StandardType.UnitType                  => _ => AttributeValue.Null
      case StandardType.CharType                  => (a: A) => AttributeValue.String(Character.toString(a))
      case StandardType.StringType                => (a: A) => AttributeValue.String(a.toString)
      case StandardType.BoolType                  => (a: A) => AttributeValue.Bool(a.asInstanceOf[Boolean])
      case StandardType.BinaryType                => (a: A) => AttributeValue.Binary(a)
      case StandardType.ShortType                 => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case StandardType.IntType                   => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case StandardType.LongType                  => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case StandardType.FloatType                 => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case StandardType.DoubleType                => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case StandardType.BigDecimalType            => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case StandardType.BigIntegerType            => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case StandardType.DayOfWeekType             => (a: A) => AttributeValue.String(a.toString)
      case StandardType.Duration(_)               => (a: A) => AttributeValue.String(a.toString)
      case StandardType.Instant(formatter)        => (a: A) => AttributeValue.String(formatter.format(a))
      case StandardType.LocalDate(formatter)      => (a: A) => AttributeValue.String(formatter.format(a))
      case StandardType.LocalDateTime(formatter)  => (a: A) => AttributeValue.String(formatter.format(a))
      case StandardType.LocalTime(formatter)      => (a: A) => AttributeValue.String(formatter.format(a))
      case StandardType.Month                     => (a: A) => AttributeValue.String(a.toString)
      case StandardType.MonthDay                  => (a: A) => AttributeValue.String(a.toString)
      case StandardType.OffsetDateTime(formatter) => (a: A) => AttributeValue.String(formatter.format(a))
      case StandardType.OffsetTime(formatter)     => (a: A) => AttributeValue.String(formatter.format(a))
      case StandardType.Period                    => (a: A) => AttributeValue.String(a.toString)
      case StandardType.Year                      => yearEncoder
      case StandardType.YearMonth                 => (a: A) => AttributeValue.String(a.toString)
      case StandardType.ZonedDateTime(formatter)  => (a: A) => AttributeValue.String(formatter.format(a))
      case StandardType.ZoneId                    => (a: A) => AttributeValue.String(a.toString)
      case StandardType.ZoneOffset                => (a: A) => AttributeValue.String(a.toString)
    }

  private def yearEncoder[A]: Encoder[A] =
    (a: A) => {
      val year      = a.asInstanceOf[Year]
      val formatted = year.format(yearFormatter)
      AttributeValue.String(formatted)
    }

  private def transformEncoder[A, B](schema: Schema[A], g: B => Either[String, A]): Encoder[B] = { (b: B) =>
    g(b) match {
      case Right(a) => encoder(schema)(a)
      case _        => AttributeValue.Null
    }
  }

  private def optionalEncoder[A](encoder: Encoder[A]): Encoder[Option[A]] = {
    case None        => AttributeValue.Null
    case Some(value) => encoder(value)
  }

  private def eitherEncoder[A, B](encL: Encoder[A], encR: Encoder[B]): Encoder[Either[A, B]] = {
    case Left(a)  => AttributeValue.Map(Map.empty + (AttributeValue.String("Left") -> encL(a)))
    case Right(b) => AttributeValue.Map(Map.empty + (AttributeValue.String("Right") -> encR(b)))
  }

  private def tupleEncoder[A, B](encL: Encoder[A], encR: Encoder[B]): Encoder[(A, B)] = {
    case (a, b) =>
      AttributeValue.List(Chunk(encL(a), encR(b)))
  }

  private def sequenceEncoder[Col[_], A](encoder: Encoder[A], from: Col[A] => Chunk[A]): Encoder[Col[A]] =
    (col: Col[A]) => AttributeValue.List(from(col).map(encoder))

  private def enumEncoder[A](cases: Schema.Case[_, A]*): Encoder[A] =
    (a: A) => {
      val fieldIndex = cases.indexWhere(c => c.deconstruct(a).isDefined)
      if (fieldIndex > -1) {
        val case_ = cases(fieldIndex)
        val enc   = encoder(case_.codec.asInstanceOf[Schema[Any]])
        val av    = enc(a)
        AttributeValue.Map(Map.empty + (AttributeValue.String(case_.id) -> av))
      } else
        AttributeValue.Null
    }
}

package zio.dynamodb

import zio.Chunk
import zio.schema.ast.SchemaAst
import zio.schema.{ FieldSet, Schema, StandardType }

import java.time.Year
import java.time.format.{ DateTimeFormatterBuilder, SignStyle }
import java.time.temporal.ChronoField.YEAR
import scala.annotation.tailrec
import scala.collection.immutable.ListMap

private[dynamodb] object Encoder {

  private val yearFormatter =
    new DateTimeFormatterBuilder().appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD).toFormatter

  def apply[A](schema: Schema[A]): Encoder[A] = encoder(schema)

  //scalafmt: { maxColumn = 400, optIn.configStyleArguments = false }
  private def encoder[A](schema: Schema[A]): Encoder[A] =
    schema match {
      case s: Schema.Optional[a]                                                                                                                                                                                                                                                          => optionalEncoder[a](encoder(s.codec))
      case Schema.Fail(_, _)                                                                                                                                                                                                                                                              => _ => AttributeValue.Null
      case Schema.Tuple(l, r, _)                                                                                                                                                                                                                                                          => tupleEncoder(encoder(l), encoder(r))
      case s: Schema.Sequence[col, a]                                                                                                                                                                                                                                                     => sequenceEncoder[col, a](encoder(s.schemaA), s.toChunk)
      case Schema.MapSchema(ks, vs, _)                                                                                                                                                                                                                                                    => println(s"$ks $vs"); ??? // TODO: implement
      case Schema.Transform(c, _, g, _)                                                                                                                                                                                                                                                   => transformEncoder(c, g)
      case Schema.Primitive(standardType, _)                                                                                                                                                                                                                                              => primitiveEncoder(standardType)
      case Schema.GenericRecord(structure, _)                                                                                                                                                                                                                                             => genericRecordEncoder(structure)
      case Schema.EitherSchema(l, r, _)                                                                                                                                                                                                                                                   => eitherEncoder(encoder(l), encoder(r))
      case l @ Schema.Lazy(_)                                                                                                                                                                                                                                                             =>
        lazy val enc = encoder(l.schema)
        (a: A) => enc(a)
      case Schema.Meta(_, _)                                                                                                                                                                                                                                                              => astEncoder
      case Schema.CaseClass1(_, f, _, ext)                                                                                                                                                                                                                                                => caseClassEncoder(f -> ext)
      case Schema.CaseClass2(_, f1, f2, _, ext1, ext2)                                                                                                                                                                                                                                    => caseClassEncoder(f1 -> ext1, f2 -> ext2)
      case Schema.CaseClass3(_, f1, f2, f3, _, ext1, ext2, ext3)                                                                                                                                                                                                                          => caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3)
      case Schema.CaseClass4(_, f1, f2, f3, f4, _, ext1, ext2, ext3, ext4)                                                                                                                                                                                                                =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4)
      case Schema.CaseClass5(_, f1, f2, f3, f4, f5, _, ext1, ext2, ext3, ext4, ext5)                                                                                                                                                                                                      =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5)
      case Schema.CaseClass6(_, f1, f2, f3, f4, f5, f6, _, ext1, ext2, ext3, ext4, ext5, ext6)                                                                                                                                                                                            =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6)
      case Schema.CaseClass7(_, f1, f2, f3, f4, f5, f6, f7, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7)                                                                                                                                                                                  =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7)
      case Schema.CaseClass8(_, f1, f2, f3, f4, f5, f6, f7, f8, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8)                                                                                                                                                                        =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8)
      case Schema
            .CaseClass9(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9) =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9)
      case Schema.CaseClass10(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10)                                                                                                                                                 =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10)
      case Schema.CaseClass11(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11)                                                                                                                                     =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11)
      case Schema.CaseClass12(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12)                                                                                                                         =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12)
      case Schema.CaseClass13(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13)                                                                                                             =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13)
      case Schema.CaseClass14(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14)                                                                                                 =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14)
      case Schema.CaseClass15(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15)                                                                                     =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15)
      case Schema.CaseClass16(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16)                                                                         =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16)
      case Schema.CaseClass17(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17)                                                             =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17)
      case Schema.CaseClass18(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18)                                                 =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18)
      case Schema.CaseClass19(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19)                                     =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19)
      case Schema.CaseClass20(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20)                         =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20)
      case Schema.CaseClass21(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21)             =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21)
      case Schema.CaseClass22(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, ext22) =>
        caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21, f22 -> ext22)
      case Schema.Enum1(c, _)                                                                                                                                                                                                                                                             => enumEncoder(c)
      case Schema.Enum2(c1, c2, _)                                                                                                                                                                                                                                                        => enumEncoder(c1, c2)
      case Schema.Enum3(c1, c2, c3, _)                                                                                                                                                                                                                                                    => enumEncoder(c1, c2, c3)
      case Schema.Enum4(c1, c2, c3, c4, _)                                                                                                                                                                                                                                                => enumEncoder(c1, c2, c3, c4)
      case Schema.Enum5(c1, c2, c3, c4, c5, _)                                                                                                                                                                                                                                            => enumEncoder(c1, c2, c3, c4, c5)
      case Schema.Enum6(c1, c2, c3, c4, c5, c6, _)                                                                                                                                                                                                                                        => enumEncoder(c1, c2, c3, c4, c5, c6)
      case Schema.Enum7(c1, c2, c3, c4, c5, c6, c7, _)                                                                                                                                                                                                                                    => enumEncoder(c1, c2, c3, c4, c5, c6, c7)
      case Schema.Enum8(c1, c2, c3, c4, c5, c6, c7, c8, _)                                                                                                                                                                                                                                => enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8)
      case Schema.Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, _)                                                                                                                                                                                                                            => enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9)
      case Schema.Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, _)                                                                                                                                                                                                                      =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
      case Schema.Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, _)                                                                                                                                                                                                                 =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
      case Schema.Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, _)                                                                                                                                                                                                            =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
      case Schema.Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, _)                                                                                                                                                                                                       =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
      case Schema.Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, _)                                                                                                                                                                                                  =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
      case Schema.Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, _)                                                                                                                                                                                             =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
      case Schema.Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, _)                                                                                                                                                                                        =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
      case Schema.Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, _)                                                                                                                                                                                   =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
      case Schema.Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, _)                                                                                                                                                                              =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)
      case Schema.Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, _)                                                                                                                                                                         =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)
      case Schema
            .Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, _) =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
      case Schema
            .Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, _) =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)
      case Schema.Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, _)                                                                                                                                                          =>
        enumEncoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
      case Schema.EnumN(cs, _)                                                                                                                                                                                                                                                            =>
        enumEncoder(cs.toSeq: _*)
    }
  //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }

  private val astEncoder: Encoder[Schema[_]] =
    (schema: Schema[_]) => encoder(Schema[SchemaAst])(SchemaAst.fromSchema(schema))

  private def genericRecordEncoder(structure: FieldSet): Encoder[ListMap[String, _]] =
    (valuesMap: ListMap[String, _]) => {
      structure.toChunk.foldRight(AttributeValue.Map(Map.empty)) {
        case (Schema.Field(key, schema: Schema[a], _), avMap) =>
          val value              = valuesMap(key)
          val enc                = encoder[a](schema)
          val av: AttributeValue = enc(value.asInstanceOf[a])
          AttributeValue.Map(avMap.value + (AttributeValue.String(key) -> av))
      }
    }

  private def caseClassEncoder[A](fields: (Schema.Field[_], A => Any)*): Encoder[A] =
    (a: A) =>
      fields.foldRight[AttributeValue.Map](AttributeValue.Map(Map.empty)) {
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
      case StandardType.UUIDType                  => (a: A) => AttributeValue.String(a.toString)
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

  private def sequenceEncoder[Col, A](encoder: Encoder[A], from: Col => Chunk[A]): Encoder[Col] =
    (col: Col) => AttributeValue.List(from(col).map(encoder))

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

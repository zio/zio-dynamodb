package zio.dynamodb

import zio.dynamodb.Annotations.{ discriminator, enumOfCaseObjects, id }
import zio.schema.Schema.{ Optional, Primitive, Transform }
import zio.schema.ast.SchemaAst
import zio.schema.{ FieldSet, Schema, StandardType }
import zio.{ schema, Chunk }

import java.time._
import java.time.format.{ DateTimeFormatterBuilder, SignStyle }
import java.time.temporal.ChronoField.YEAR
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.util.Try

private[dynamodb] object Codec {

  def encoder[A](schema: Schema[A]): Encoder[A] = Encoder(schema)

  def decoder[A](schema: Schema[A]): Decoder[A] = Decoder(schema)

  private[dynamodb] object Encoder {

    private val stringEncoder = encoder(Schema[String])
    private val yearFormatter =
      new DateTimeFormatterBuilder().appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD).toFormatter

    def apply[A](schema: Schema[A]): Encoder[A] = encoder(schema)

    //scalafmt: { maxColumn = 400, optIn.configStyleArguments = false }
    private def encoder[A](schema: Schema[A]): Encoder[A] =
      schema match {
        case s: Schema.Optional[a]                                                                                                                                                                                                                                                          =>
          optionalEncoder[a](encoder(s.codec))
        case Schema.Fail(_, _)                                                                                                                                                                                                                                                              =>
          _ => AttributeValue.Null
        case Schema.Tuple(l, r, _)                                                                                                                                                                                                                                                          =>
          tupleEncoder(encoder(l), encoder(r))
        case s: Schema.Sequence[col, a]                                                                                                                                                                                                                                                     =>
          sequenceEncoder[col, a](encoder(s.schemaA), s.toChunk)
        case Schema.SetSchema(s, _)                                                                                                                                                                                                                                                         =>
          setEncoder(s)
        case Schema.MapSchema(ks, vs, _)                                                                                                                                                                                                                                                    =>
          mapEncoder(ks, vs)
        case Schema.Transform(c, _, g, _)                                                                                                                                                                                                                                                   =>
          transformEncoder(c, g)
        case Schema.Primitive(standardType, _)                                                                                                                                                                                                                                              =>
          primitiveEncoder(standardType)
        case Schema.GenericRecord(structure, _)                                                                                                                                                                                                                                             =>
          genericRecordEncoder(structure)
        case Schema.EitherSchema(l, r, _)                                                                                                                                                                                                                                                   =>
          eitherEncoder(encoder(l), encoder(r))
        case l @ Schema.Lazy(_)                                                                                                                                                                                                                                                             =>
          lazy val enc = encoder(l.schema)
          (a: A) => enc(a)
        case Schema.Meta(_, _)                                                                                                                                                                                                                                                              =>
          astEncoder
        case Schema.CaseClass1(f, _, ext, _)                                                                                                                                                                                                                                                =>
          caseClassEncoder(f -> ext)
        case Schema.CaseClass2(f1, f2, _, ext1, ext2, _)                                                                                                                                                                                                                                    =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2)
        case Schema.CaseClass3(f1, f2, f3, _, ext1, ext2, ext3, _)                                                                                                                                                                                                                          =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3)
        case Schema.CaseClass4(f1, f2, f3, f4, _, ext1, ext2, ext3, ext4, _)                                                                                                                                                                                                                =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4)
        case Schema.CaseClass5(f1, f2, f3, f4, f5, _, ext1, ext2, ext3, ext4, ext5, _)                                                                                                                                                                                                      =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5)
        case Schema.CaseClass6(f1, f2, f3, f4, f5, f6, _, ext1, ext2, ext3, ext4, ext5, ext6, _)                                                                                                                                                                                            =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6)
        case Schema.CaseClass7(f1, f2, f3, f4, f5, f6, f7, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, _)                                                                                                                                                                                  =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7)
        case Schema.CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, _)                                                                                                                                                                        =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8)
        case Schema
              .CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, _) =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9)
        case Schema.CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, _)                                                                                                                                                 =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10)
        case Schema.CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, _)                                                                                                                                     =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11)
        case Schema.CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, _)                                                                                                                         =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12)
        case Schema.CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, _)                                                                                                             =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13)
        case Schema.CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, _)                                                                                                 =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14)
        case Schema.CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, _)                                                                                     =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15)
        case Schema.CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, _)                                                                         =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16)
        case Schema.CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, _)                                                             =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17)
        case Schema.CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, _)                                                 =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18)
        case Schema.CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, _)                                     =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19)
        case Schema.CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, _)                         =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20)
        case Schema.CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, _)             =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21)
        case Schema.CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, ext22, _) =>
          caseClassEncoder(f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21, f22 -> ext22)
        case Schema.Enum1(c, annotations)                                                                                                                                                                                                                                                   =>
          enumEncoder(annotations, c)
        case Schema.Enum2(c1, c2, annotations)                                                                                                                                                                                                                                              =>
          enumEncoder(annotations, c1, c2)
        case Schema.Enum3(c1, c2, c3, annotations)                                                                                                                                                                                                                                          =>
          enumEncoder(annotations, c1, c2, c3)
        case Schema.Enum4(c1, c2, c3, c4, annotations)                                                                                                                                                                                                                                      =>
          enumEncoder(annotations, c1, c2, c3, c4)
        case Schema.Enum5(c1, c2, c3, c4, c5, annotations)                                                                                                                                                                                                                                  =>
          enumEncoder(annotations, c1, c2, c3, c4, c5)
        case Schema.Enum6(c1, c2, c3, c4, c5, c6, annotations)                                                                                                                                                                                                                              =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6)
        case Schema.Enum7(c1, c2, c3, c4, c5, c6, c7, annotations)                                                                                                                                                                                                                          =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7)
        case Schema.Enum8(c1, c2, c3, c4, c5, c6, c7, c8, annotations)                                                                                                                                                                                                                      =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8)
        case Schema.Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations)                                                                                                                                                                                                                  =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)
        case Schema.Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations)                                                                                                                                                                                                            =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
        case Schema.Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations)                                                                                                                                                                                                       =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
        case Schema.Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations)                                                                                                                                                                                                  =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
        case Schema.Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations)                                                                                                                                                                                             =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
        case Schema.Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations)                                                                                                                                                                                        =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
        case Schema.Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations)                                                                                                                                                                                   =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
        case Schema.Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations)                                                                                                                                                                              =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
        case Schema.Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations)                                                                                                                                                                         =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
        case Schema.Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations)                                                                                                                                                                    =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)
        case Schema.Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations)                                                                                                                                                               =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)
        case Schema
              .Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
        case Schema
              .Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations) =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)
        case Schema.Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations)                                                                                                                                                =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
        case Schema.EnumN(cs, annotations)                                                                                                                                                                                                                                                  =>
          enumEncoder(annotations, cs.toSeq: _*)
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
      (a: A) => {
        fields.foldRight[AttributeValue.Map](AttributeValue.Map(Map.empty)) {
          case ((Schema.Field(key, schema, annotations), ext), acc) =>
            val enc                 = encoder(schema)
            val extractedFieldValue = ext(a)
            val av                  = enc(extractedFieldValue)
            val k                   = maybeId(annotations).getOrElse(key)

            @tailrec
            def appendToMap[B](schema: Schema[B]): AttributeValue.Map =
              schema match {
                case l @ Schema.Lazy(_)                                                 =>
                  appendToMap(l.schema)
                case _: Schema.Optional[_] if av.isInstanceOf[AttributeValue.Null.type] =>
                  AttributeValue.Map(acc.value)
                case _                                                                  =>
                  AttributeValue.Map(acc.value + (AttributeValue.String(k) -> av))
              }

            appendToMap(schema)
        }
      }

    private def primitiveEncoder[A](standardType: StandardType[A]): Encoder[A] =
      standardType match {
        case StandardType.UnitType                      => _ => AttributeValue.Null
        case StandardType.CharType                      => (a: A) => AttributeValue.String(Character.toString(a))
        case StandardType.StringType                    => (a: A) => AttributeValue.String(a.toString)
        case StandardType.BoolType                      => (a: A) => AttributeValue.Bool(a.asInstanceOf[Boolean])
        case StandardType.BinaryType                    => (a: A) => AttributeValue.Binary(a)
        case StandardType.ShortType                     => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.IntType                       => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.LongType                      => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.FloatType                     => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.DoubleType                    => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.BigDecimalType                => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.BigIntegerType                => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.UUIDType                      => (a: A) => AttributeValue.String(a.toString)
        case StandardType.DayOfWeekType                 => (a: A) => AttributeValue.String(a.toString)
        case StandardType.Duration(_)                   => (a: A) => AttributeValue.String(a.toString)
        case StandardType.InstantType(formatter)        => (a: A) => AttributeValue.String(formatter.format(a))
        case StandardType.LocalDateType(formatter)      => (a: A) => AttributeValue.String(formatter.format(a))
        case StandardType.LocalDateTimeType(formatter)  => (a: A) => AttributeValue.String(formatter.format(a))
        case StandardType.LocalTimeType(formatter)      => (a: A) => AttributeValue.String(formatter.format(a))
        case StandardType.MonthType                     => (a: A) => AttributeValue.String(a.toString)
        case StandardType.MonthDayType                  => (a: A) => AttributeValue.String(a.toString)
        case StandardType.OffsetDateTimeType(formatter) => (a: A) => AttributeValue.String(formatter.format(a))
        case StandardType.OffsetTimeType(formatter)     => (a: A) => AttributeValue.String(formatter.format(a))
        case StandardType.PeriodType                    => (a: A) => AttributeValue.String(a.toString)
        case StandardType.YearType                      => yearEncoder
        case StandardType.YearMonthType                 => (a: A) => AttributeValue.String(a.toString)
        case StandardType.ZonedDateTimeType(formatter)  => (a: A) => AttributeValue.String(formatter.format(a))
        case StandardType.ZoneIdType                    => (a: A) => AttributeValue.String(a.toString)
        case StandardType.ZoneOffsetType                => (a: A) => AttributeValue.String(a.toString)
      }

    private def yearEncoder[A]: Encoder[A] =
      (a: A) => {
        val year      = a.asInstanceOf[Year]
        val formatted = year.format(yearFormatter)
        AttributeValue.String(formatted)
      }

    private def transformEncoder[A, B](schema: Schema[A], g: B => Either[String, A]): Encoder[B] = { (b: B) =>
      g(b) match {
        case Right(a) =>
          encoder(schema)(a)
        case _        =>
          AttributeValue.Null
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

    private def enumEncoder[A](annotations: Chunk[Any], cases: Schema.Case[_, A]*): Encoder[A] =
      if (isAlternateEnumCodec(annotations))
        alternateEnumEncoder(discriminator(annotations), cases: _*)
      else
        defaultEnumEncoder(cases: _*)

    private def defaultEnumEncoder[A](cases: Schema.Case[_, A]*): Encoder[A] =
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

    private def alternateEnumEncoder[A](discriminator: String, cases: Schema.Case[_, A]*): Encoder[A] =
      (a: A) => {
        val fieldIndex = cases.indexWhere(c => c.deconstruct(a).isDefined)
        if (fieldIndex > -1) {
          val case_   = cases(fieldIndex)
          val enc     = encoder(case_.codec.asInstanceOf[Schema[Any]])
          lazy val id = maybeId(case_.annotations).getOrElse(case_.id)
          val av      = enc(a)
          av match { // TODO: review all pattern matches inside of a lambda
            case AttributeValue.Map(map) =>
              AttributeValue.Map(
                map + (AttributeValue.String(discriminator) -> AttributeValue.String(id))
              )
            case AttributeValue.Null     =>
              val av2 = AttributeValue.String(id)
              if (allCaseObjects(cases))
                av2
              else
                // these are case objects and are a special case - they need to wrapped in an AttributeValue.Map
                AttributeValue.Map(Map(AttributeValue.String(discriminator) -> av2))
            case av                      => throw new IllegalStateException(s"unexpected state $av")
          }
        } else
          AttributeValue.Null
      }

    private def setEncoder[A](s: Schema[A]): Encoder[Set[A]] =
      s match {
        // AttributeValue.StringSet
        case Schema.Primitive(StandardType.StringType, _)     =>
          (a: Set[A]) => AttributeValue.StringSet(a.asInstanceOf[Set[String]])

        // AttributeValue.NumberSet
        case Schema.Primitive(StandardType.IntType, _)        =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[Int]].map(BigDecimal(_)))
        case Schema.Primitive(StandardType.LongType, _)       =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[Long]].map(BigDecimal(_)))
        case Schema.Primitive(StandardType.ShortType, _)      =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[Short]].map(s => BigDecimal(s.toInt)))
        case Schema.Primitive(StandardType.DoubleType, _)     =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[Double]].map(BigDecimal(_)))
        case Schema.Primitive(StandardType.FloatType, _)      =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[Float]].map(f => BigDecimal(f.toString)))
        case Schema.Primitive(StandardType.BigDecimalType, _) =>
          (a: Set[A]) =>
            AttributeValue.NumberSet(
              a.asInstanceOf[Set[java.math.BigDecimal]].map(bd => BigDecimal(bd.doubleValue))
            )
        // DerivedGen will wrap a java BigDecimal with a Transform for a scala BigDecimal so we need to peek ahead here
        case Schema.Transform(Schema.Primitive(bigDecimal, _), _, _, _)
            if bigDecimal.isInstanceOf[StandardType.BigDecimalType.type] =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[BigDecimal]])
        case Schema.Primitive(StandardType.BigIntegerType, _) =>
          (a: Set[A]) =>
            AttributeValue.NumberSet(a.asInstanceOf[Set[java.math.BigInteger]].map(i => BigDecimal(i.longValue)))
        // DerivedGen will wrap a java BigInteger with a Transform for a scala BigInt so we need to peek ahead here
        case Schema.Transform(Schema.Primitive(bigDecimal, _), _, _, _)
            if bigDecimal.isInstanceOf[StandardType.BigIntegerType.type] =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[BigInt]].map(bi => BigDecimal(bi.bigInteger)))

        // AttributeValue.BinarySet
        case Schema.Primitive(StandardType.BinaryType, _)     =>
          (a: Set[A]) => AttributeValue.BinarySet(a.asInstanceOf[Set[Chunk[Byte]]])

        // Non native set
        case schema                                           =>
          sequenceEncoder[Chunk[A], A](encoder(schema), (c: Iterable[A]) => Chunk.fromIterable(c))
            .asInstanceOf[Encoder[Set[A]]]
      }

    private def mapEncoder[K, V](ks: Schema[K], vs: Schema[V]): Encoder[Map[K, V]] =
      ks match {
        case Schema.Primitive(StandardType.StringType, _) =>
          nativeMapEncoder(encoder(vs))
        case _                                            =>
          nonNativeMapEncoder(encoder(ks), encoder(vs))
      }

    private def nativeMapEncoder[A, V](encoderV: Encoder[V]) =
      (a: A) => {
        val m = a.asInstanceOf[Map[String, V]]
        AttributeValue.Map(m.map {
          case (k, v) =>
            (stringEncoder(k), encoderV(v))
        }.asInstanceOf[Map[AttributeValue.String, AttributeValue]])
      }

    private def nonNativeMapEncoder[A, K, V](encoderK: Encoder[K], encoderV: Encoder[V]): Encoder[A] = {
      val te = tupleEncoder(encoderK, encoderV)
      val se = sequenceEncoder[Chunk[(K, V)], (K, V)](te, (c: Iterable[(K, V)]) => Chunk.fromIterable(c))
      se.asInstanceOf[Encoder[A]]
    }

  } // end Encoder

  private[dynamodb] object Decoder extends GeneratedCaseClassDecoders {

    sealed trait ContainerField
    object ContainerField {
      case object Optional extends ContainerField
      case object List     extends ContainerField
      case object Map      extends ContainerField
      case object Set      extends ContainerField
      case object Scalar   extends ContainerField

      def containerField[B](schema: Schema[B]): ContainerField =
        schema match {
          case l @ Schema.Lazy(_)         =>
            containerField(l.schema)
          case _: Schema.Optional[_]      =>
            Optional
          case _: Schema.MapSchema[_, _]  =>
            Map
          case _: Schema.SetSchema[_]     =>
            Set
          case _: Schema.Collection[_, _] =>
            List
          case _                          =>
            Scalar
        }
    }

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
        case Schema.SetSchema(s, _)                                                                                                                                           =>
          setDecoder(s).asInstanceOf[Decoder[A]]
        case Schema.MapSchema(ks, vs, _)                                                                                                                                      =>
          mapDecoder(ks, vs).asInstanceOf[Decoder[A]]
        case s @ Schema.CaseClass1(_, _, _, _)                                                                                                                                =>
          caseClass1Decoder(s)
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
        case Schema.Enum1(c, annotations)                                                                                                                                     =>
          enumDecoder(annotations, c)
        case Schema.Enum2(c1, c2, annotations)                                                                                                                                =>
          enumDecoder(annotations, c1, c2)
        case Schema.Enum3(c1, c2, c3, annotations)                                                                                                                            =>
          enumDecoder(annotations, c1, c2, c3)
        case Schema.Enum4(c1, c2, c3, c4, annotations)                                                                                                                        =>
          enumDecoder(annotations, c1, c2, c3, c4)
        case Schema.Enum5(c1, c2, c3, c4, c5, annotations)                                                                                                                    =>
          enumDecoder(annotations, c1, c2, c3, c4, c5)
        case Schema.Enum6(c1, c2, c3, c4, c5, c6, annotations)                                                                                                                =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6)
        case Schema.Enum7(c1, c2, c3, c4, c5, c6, c7, annotations)                                                                                                            =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7)
        case Schema.Enum8(c1, c2, c3, c4, c5, c6, c7, c8, annotations)                                                                                                        =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8)
        case Schema.Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations)                                                                                                    =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)
        case Schema.Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations)                                                                                              =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
        case Schema.Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations)                                                                                         =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
        case Schema.Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations)                                                                                    =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
        case Schema.Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations)                                                                               =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
        case Schema.Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations)                                                                          =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
        case Schema.Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations)                                                                     =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
        case Schema.Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations)                                                                =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
        case Schema.Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations)                                                           =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
        case Schema.Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations)                                                      =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)
        case Schema.Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations)                                                 =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)
        case Schema
              .Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
        case Schema.Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations)                                       =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)
        case Schema.Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations)                                  =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
        case Schema.EnumN(cs, annotations)                                                                                                                                    =>
          enumDecoder(annotations, cs.toSeq: _*)

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
        case StandardType.UnitType                      => _ => Right(())
        case StandardType.StringType                    =>
          (av: AttributeValue) => FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av)
        case StandardType.BoolType                      =>
          (av: AttributeValue) => FromAttributeValue.booleanFromAttributeValue.fromAttributeValue(av)
        case StandardType.ShortType                     =>
          (av: AttributeValue) => FromAttributeValue.shortFromAttributeValue.fromAttributeValue(av)
        case StandardType.IntType                       =>
          (av: AttributeValue) => FromAttributeValue.intFromAttributeValue.fromAttributeValue(av)
        case StandardType.LongType                      =>
          (av: AttributeValue) => FromAttributeValue.longFromAttributeValue.fromAttributeValue(av)
        case StandardType.FloatType                     =>
          (av: AttributeValue) => FromAttributeValue.floatFromAttributeValue.fromAttributeValue(av)
        case StandardType.DoubleType                    =>
          (av: AttributeValue) => FromAttributeValue.doubleFromAttributeValue.fromAttributeValue(av)
        case StandardType.BigDecimalType                =>
          (av: AttributeValue) =>
            FromAttributeValue.bigDecimalFromAttributeValue
              .fromAttributeValue(av)
              .map(_.bigDecimal)
        case StandardType.BigIntegerType                =>
          (av: AttributeValue) =>
            FromAttributeValue.bigDecimalFromAttributeValue
              .fromAttributeValue(av)
              .map(_.toBigInt.bigInteger)
        case StandardType.BinaryType                    =>
          (av: AttributeValue) =>
            FromAttributeValue.binaryFromAttributeValue
              .fromAttributeValue(av)
              .map(Chunk.fromIterable(_))
        case StandardType.CharType                      =>
          (av: AttributeValue) =>
            FromAttributeValue.stringFromAttributeValue
              .fromAttributeValue(av)
              .map { s =>
                val array = s.toCharArray
                array(0)
              }
        case StandardType.UUIDType                      =>
          (av: AttributeValue) =>
            FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
              Try(UUID.fromString(s)).toEither.left.map(iae => s"Invalid UUID: ${iae.getMessage}")
            }
        case StandardType.DayOfWeekType                 =>
          (av: AttributeValue) => javaTimeStringParser(av)(DayOfWeek.valueOf(_))
        case StandardType.Duration(_)                   =>
          (av: AttributeValue) => javaTimeStringParser(av)(Duration.parse(_))
        case StandardType.InstantType(formatter)        =>
          (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, Instant.from(_)))
        case StandardType.LocalDateType(formatter)      =>
          (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, LocalDate.from(_)))
        case StandardType.LocalDateTimeType(formatter)  =>
          (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, LocalDateTime.from(_)))
        case StandardType.LocalTimeType(formatter)      =>
          (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, LocalTime.from(_)))
        case StandardType.MonthType                     =>
          (av: AttributeValue) => javaTimeStringParser(av)(Month.valueOf(_))
        case StandardType.MonthDayType                  =>
          (av: AttributeValue) => javaTimeStringParser(av)(MonthDay.parse(_))
        case StandardType.OffsetDateTimeType(formatter) =>
          (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, OffsetDateTime.from(_)))
        case StandardType.OffsetTimeType(formatter)     =>
          (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, OffsetTime.from(_)))
        case StandardType.PeriodType                    =>
          (av: AttributeValue) => javaTimeStringParser(av)(Period.parse(_))
        case StandardType.YearType                      =>
          (av: AttributeValue) => javaTimeStringParser(av)(Year.parse(_))
        case StandardType.YearMonthType                 =>
          (av: AttributeValue) => javaTimeStringParser(av)(YearMonth.parse(_))
        case StandardType.ZonedDateTimeType(formatter)  =>
          (av: AttributeValue) => javaTimeStringParser(av)(formatter.parse(_, ZonedDateTime.from(_)))
        case StandardType.ZoneIdType                    =>
          (av: AttributeValue) => javaTimeStringParser(av)(ZoneId.of(_))
        case StandardType.ZoneOffsetType                =>
          (av: AttributeValue) => javaTimeStringParser(av)(ZoneOffset.of(_))
      }

    private def javaTimeStringParser[A](av: AttributeValue)(unsafeParse: String => A): Either[String, A] =
      FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
        val stringOrA = Try(unsafeParse(s)).toEither.left
          .map(e => s"error parsing string '$s': ${e.getMessage}")
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

    private def setDecoder[A](s: Schema[A]): Decoder[Set[A]] = {
      def nativeStringSetDecoder[A]: Decoder[Set[A]] = {
        case AttributeValue.StringSet(stringSet) =>
          Right(stringSet.asInstanceOf[Set[A]])
        case av                                  =>
          Left(s"Error: expected a string set but found '$av'")
      }

      def nativeNumberSetDecoder[A](f: BigDecimal => A): Decoder[Set[A]] = {
        case AttributeValue.NumberSet(numberSet) =>
          Right(numberSet.map(f))
        case av                                  =>
          Left(s"Error: expected a number set but found '$av'")
      }

      def nativeBinarySetDecoder[A]: Decoder[Set[A]] = {
        case AttributeValue.BinarySet(setOfChunkOfByte) =>
          val set: Set[Chunk[Byte]] = setOfChunkOfByte.toSet.map((xs: Iterable[Byte]) => Chunk.fromIterable(xs))
          Right(set.asInstanceOf[Set[A]])
        case av                                         =>
          Left(s"Error: expected a Set of Chunk of Byte but found '$av'")
      }

      s match {
        // StringSet
        case Schema.Primitive(StandardType.StringType, _)     =>
          nativeStringSetDecoder

        // NumberSet
        case Schema.Primitive(StandardType.IntType, _)        =>
          nativeNumberSetDecoder(_.intValue)
        case Schema.Primitive(StandardType.LongType, _)       =>
          nativeNumberSetDecoder(_.longValue)
        case Schema.Primitive(StandardType.ShortType, _)      =>
          nativeNumberSetDecoder(_.shortValue)
        case Schema.Primitive(StandardType.DoubleType, _)     =>
          nativeNumberSetDecoder(_.doubleValue)
        case Schema.Primitive(StandardType.FloatType, _)      =>
          nativeNumberSetDecoder(_.floatValue)
        case Schema.Primitive(StandardType.BigDecimalType, _) =>
          nativeNumberSetDecoder(_.bigDecimal)
        case Schema.Transform(Schema.Primitive(bigDecimal, _), _, _, _)
            if bigDecimal.isInstanceOf[StandardType.BigDecimalType.type] =>
          nativeNumberSetDecoder[BigDecimal](_.bigDecimal).asInstanceOf[Decoder[Set[A]]]
        case Schema.Primitive(StandardType.BigIntegerType, _) =>
          nativeNumberSetDecoder(bd => bd.toBigInt.bigInteger)
        case Schema.Transform(Schema.Primitive(bigInt, _), _, _, _)
            if bigInt.isInstanceOf[StandardType.BigIntegerType.type] =>
          nativeNumberSetDecoder[BigInt](_.toBigInt).asInstanceOf[Decoder[Set[A]]]

        // BinarySet
        case Schema.Primitive(StandardType.BinaryType, _)     =>
          nativeBinarySetDecoder

        // non native set
        case _                                                =>
          nonNativeSetDecoder(decoder(s))
      }
    }

    private def nonNativeSetDecoder[A](decA: Decoder[A]): Decoder[Set[A]] = { (av: AttributeValue) =>
      av match {
        case AttributeValue.List(listOfAv) =>
          val errorOrList = EitherUtil.forEach(listOfAv) { av =>
            decA(av)
          }
          errorOrList.map(_.toSet)
        case av                            => Left(s"Error: expected AttributeValue.List but found $av")
      }
    }

    private def mapDecoder[K, V](ks: Schema[K], vs: Schema[V]) =
      ks match {
        case Schema.Primitive(StandardType.StringType, _) =>
          nativeMapDecoder(decoder(vs))
        case _                                            =>
          nonNativeMapDecoder(decoder(ks), decoder(vs))
      }

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
            EitherUtil.collectAll(xs).map(_.toMap)
          case av                      => Left(s"Error: expected AttributeValue.Map but found $av")
        }
      }

    def nonNativeMapDecoder[A, B](decA: Decoder[A], decB: Decoder[B]): Decoder[Map[A, B]] =
      (av: AttributeValue) => {
        av match {
          case AttributeValue.List(listOfAv) =>
            val errorOrListOfTuple = EitherUtil.forEach(listOfAv) {
              case avList @ AttributeValue.List(_) =>
                tupleDecoder(decA, decB)(avList)
              case av                              =>
                Left(s"Error: expected AttributeValue.List but found $av")
            }
            errorOrListOfTuple.map(_.toMap)
          case av                            => Left(s"Error: expected AttributeValue.List but found $av")
        }
      }

    private def enumDecoder[A](annotations: Chunk[Any], cases: Schema.Case[_, A]*): Decoder[A] =
      if (isAlternateEnumCodec(annotations))
        alternateEnumDecoder(discriminator(annotations), cases: _*)
      else
        defaultEnumDecoder(cases: _*)

    private def defaultEnumDecoder[A](cases: Schema.Case[_, A]*): Decoder[A] =
      (av: AttributeValue) =>
        av match {
          case AttributeValue.Map(map) =>
            // default enum encoding uses a Map with a single entry that denotes the type
            // TODO: think about being stricter and rejecting Maps with > 1 entry ???
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

    private def alternateEnumDecoder[A](discriminator: String, cases: Schema.Case[_, A]*): Decoder[A] = {
      (av: AttributeValue) =>
        def findCase(value: String): Either[String, Schema.Case[_, A]] =
          cases.find {
            case Schema.Case(_, _, _, Chunk(id(const))) => const == value
            case Schema.Case(id, _, _, _)               => id == value
          }.toRight(s"type name '$value' not found in schema cases")

        def decode(id: String): Either[String, A] =
          findCase(id).flatMap { c =>
            val dec = decoder(c.codec)
            dec(av).map(_.asInstanceOf[A])
          }

        av match {
          case AttributeValue.String(id) =>
            if (allCaseObjects(cases))
              decode(id)
            else
              Left(s"Error: not all enumeration elements are case objects. Found $cases")
          case AttributeValue.Map(map)   =>
            map
              .get(AttributeValue.String(discriminator))
              .fold[Either[String, A]](Left(s"map $av does not contain discriminator field '$discriminator'")) {
                case AttributeValue.String(typeName) =>
                  decode(typeName)
                case av                              => Left(s"expected string type but found $av")
              }
          case _                         => Left(s"unexpected AttributeValue type $av")
        }
    }

    private[dynamodb] def decodeFields(av: AttributeValue, fields: Schema.Field[_]*): Either[String, List[Any]] =
      av match {
        case AttributeValue.Map(map) =>
          EitherUtil
            .forEach(fields) {
              case Schema.Field(key, schema, annotations) =>
                val dec                         = decoder(schema)
                val k                           = maybeId(annotations).getOrElse(key)
                val maybeValue                  = map.get(AttributeValue.String(k))
                val maybeDecoder                = maybeValue.map(dec).toRight(s"field '$k' not found in $av")
                val either: Either[String, Any] = for {
                  decoder <- maybeDecoder
                  decoded <- decoder
                } yield decoded

                if (maybeValue.isEmpty)
                  ContainerField.containerField(schema) match {
                    case ContainerField.Optional =>
                      Right(None)
                    case ContainerField.List     =>
                      Right(List.empty)
                    case ContainerField.Map      =>
                      Right(Map.empty)
                    case ContainerField.Set      =>
                      Right(Set.empty)
                    case ContainerField.Scalar   =>
                      either
                  }
                else
                  either
            }
            .map(_.toList)
        case _                       =>
          Left(s"$av is not an AttributeValue.Map")
      }

  } // end Decoder

  private def allCaseObjects[A](cases: Seq[Schema.Case[_, A]]): Boolean =
    cases.forall {
      case Schema.Case(_, Transform(Primitive(standardType, _), _, _, _), _, _)
          if standardType == StandardType.UnitType =>
        true
      case _ =>
        false
    }

  private def discriminator(annotations: Chunk[Any]): String =
    annotations.toList match {
      case discriminator(name) :: _      =>
        name
      case _ :: discriminator(name) :: _ =>
        name
      case _                             =>
        "discriminator"
    }

  private def maybeId(annotations: Chunk[Any]): Option[String] =
    annotations.toList match {
      case id(name) :: _ =>
        Some(name)
      case _             =>
        None
    }

  private def isAlternateEnumCodec(annotations: Chunk[Any]): Boolean =
    annotations.exists {
      case discriminator(_) | enumOfCaseObjects() => true
      case _                                      => false
    }

} // end Codec

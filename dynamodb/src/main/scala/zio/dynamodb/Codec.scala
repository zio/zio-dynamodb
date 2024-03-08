package zio.dynamodb

import zio.dynamodb.Annotations._
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.DynamoDBError.ItemError
import zio.prelude.{ FlipOps, ForEachOps }
import zio.schema.Schema.{ Optional, Primitive }
import zio.schema.annotation.caseName
import zio.schema.{ FieldSet, Schema, StandardType }
import zio.Chunk

import java.math.BigInteger
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
        case s: Schema.Optional[a]                                                                                                              =>
          optionalEncoder[a](encoder(s.schema))
        case Schema.Fail(_, _)                                                                                                                  =>
          _ => AttributeValue.Null
        case Schema.Tuple2(l, r, _)                                                                                                             =>
          tupleEncoder(encoder(l), encoder(r))
        case s: Schema.Sequence[col, a, _]                                                                                                      =>
          sequenceEncoder[col, a](encoder(s.elementSchema), s.toChunk)
        case Schema.Set(s, _)                                                                                                                   =>
          setEncoder(s)
        case Schema.Map(ks, vs, _)                                                                                                              =>
          mapEncoder(ks, vs)
        case Schema.Transform(c, _, g, _, _)                                                                                                    =>
          transformEncoder(c, g)
        case Schema.Primitive(standardType, _)                                                                                                  =>
          primitiveEncoder(standardType)
        case Schema.GenericRecord(_, structure, _)                                                                                              =>
          genericRecordEncoder(structure)
        case Schema.Either(l, r, _)                                                                                                             =>
          eitherEncoder(encoder(l), encoder(r))
        case l @ Schema.Lazy(_)                                                                                                                 =>
          lazy val enc = encoder(l.schema)
          (a: A) => enc(a)
        case Schema.Dynamic(_)                                                                                                                  =>
          dynamicEncoder
        case Schema.CaseClass0(_, _, _)                                                                                                         =>
          caseClassEncoder0
        case Schema.CaseClass1(_, f, _, _)                                                                                                      =>
          caseClassEncoder(f)
        case Schema.CaseClass2(_, f1, f2, _, _)                                                                                                 =>
          caseClassEncoder(f1, f2)
        case Schema.CaseClass3(_, f1, f2, f3, _, _)                                                                                             =>
          caseClassEncoder(f1, f2, f3)
        case Schema.CaseClass4(_, f1, f2, f3, f4, _, _)                                                                                         =>
          caseClassEncoder(f1, f2, f3, f4)
        case Schema.CaseClass5(_, f1, f2, f3, f4, f5, _, _)                                                                                     =>
          caseClassEncoder(f1, f2, f3, f4, f5)
        case Schema.CaseClass6(_, f1, f2, f3, f4, f5, f6, _, _)                                                                                 =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6)
        case Schema.CaseClass7(_, f1, f2, f3, f4, f5, f6, f7, _, _)                                                                             =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7)
        case Schema.CaseClass8(_, f1, f2, f3, f4, f5, f6, f7, f8, _, _)                                                                         =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8)
        case Schema
              .CaseClass9(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, _, _) =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9)
        case Schema.CaseClass10(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, _)                                                               =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10)
        case Schema.CaseClass11(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, _)                                                          =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11)
        case Schema.CaseClass12(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, _)                                                     =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12)
        case Schema.CaseClass13(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, _)                                                =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13)
        case Schema.CaseClass14(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, _)                                           =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14)
        case Schema.CaseClass15(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, _)                                      =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15)
        case Schema.CaseClass16(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, _)                                 =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16)
        case Schema.CaseClass17(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, _)                            =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17)
        case Schema.CaseClass18(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, _)                       =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18)
        case Schema.CaseClass19(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, _)                  =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19)
        case Schema.CaseClass20(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, _)                =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20)
        case Schema.CaseClass21(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, tail)             =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, tail._1)
        case Schema.CaseClass22(_, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, tail)             =>
          caseClassEncoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, tail._1, tail._2)
        case Schema.Enum1(_, c, annotations)                                                                                                    =>
          enumEncoder(annotations, c)
        case Schema.Enum2(_, c1, c2, annotations)                                                                                               =>
          enumEncoder(annotations, c1, c2)
        case Schema.Enum3(_, c1, c2, c3, annotations)                                                                                           =>
          enumEncoder(annotations, c1, c2, c3)
        case Schema.Enum4(_, c1, c2, c3, c4, annotations)                                                                                       =>
          enumEncoder(annotations, c1, c2, c3, c4)
        case Schema.Enum5(_, c1, c2, c3, c4, c5, annotations)                                                                                   =>
          enumEncoder(annotations, c1, c2, c3, c4, c5)
        case Schema.Enum6(_, c1, c2, c3, c4, c5, c6, annotations)                                                                               =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6)
        case Schema.Enum7(_, c1, c2, c3, c4, c5, c6, c7, annotations)                                                                           =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7)
        case Schema.Enum8(_, c1, c2, c3, c4, c5, c6, c7, c8, annotations)                                                                       =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8)
        case Schema.Enum9(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations)                                                                   =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)
        case Schema.Enum10(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations)                                                             =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
        case Schema.Enum11(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations)                                                        =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
        case Schema.Enum12(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations)                                                   =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
        case Schema.Enum13(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations)                                              =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
        case Schema.Enum14(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations)                                         =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
        case Schema.Enum15(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations)                                    =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
        case Schema.Enum16(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations)                               =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
        case Schema.Enum17(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations)                          =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
        case Schema.Enum18(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations)                     =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)
        case Schema.Enum19(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations)                =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)
        case Schema
              .Enum20(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
        case Schema
              .Enum21(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations) =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)
        case Schema.Enum22(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations) =>
          enumEncoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
        case Schema.EnumN(_, cs, annotations)                                                                                                   =>
          enumEncoder(annotations, cs.toSeq: _*)
        case _                                                                                                                                  => throw new Exception("Match was non-exhaustive")
      }
    //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }

    private def genericRecordEncoder(structure: FieldSet): Encoder[ListMap[String, _]] =
      (valuesMap: ListMap[String, _]) => {
        structure.toChunk.foldRight(AttributeValue.Map(ListMap.empty)) {
          case (Schema.Field(key, schema: Schema[a], _, _, _, _), avMap) =>
            val value              = valuesMap(key)
            val enc                = encoder[a](schema)
            val av: AttributeValue = enc(value.asInstanceOf[a])
            AttributeValue.Map(avMap.value + (AttributeValue.String(key) -> av))
        }
      }

    private def dynamicEncoder[A]: Encoder[A] =
      encoder(Schema.dynamicValue).asInstanceOf[Encoder[A]]

    private def caseClassEncoder0[Z]: Encoder[Z] = _ => AttributeValue.Null

    private def caseClassEncoder[Z](fields: Schema.Field[Z, _]*): Encoder[Z] = { (a: Z) =>
      fields.foldRight[AttributeValue.Map](AttributeValue.Map(Map.empty)) {
        case s: (Schema.Field[Z, _], AttributeValue.Map) =>
          val enc                 = encoder(s._1.schema)
          val extractedFieldValue = s._1.get(a)
          val av                  = enc(extractedFieldValue)
          val k                   = s._1.name

          @tailrec
          def appendToMap[B](schema: Schema[B]): AttributeValue.Map =
            schema match {
              case l @ Schema.Lazy(_)                                                 =>
                appendToMap(l.schema)
              case _: Schema.Optional[_] if av.isInstanceOf[AttributeValue.Null.type] =>
                AttributeValue.Map(s._2.value)
              case _                                                                  =>
                AttributeValue.Map(s._2.value + (AttributeValue.String(k) -> av))
            }

          appendToMap(s._1.schema)
      }

    }

    private def primitiveEncoder[A](standardType: StandardType[A]): Encoder[A] =
      standardType match {
        case StandardType.UnitType           => _ => AttributeValue.Null
        case StandardType.CharType           => (a: A) => AttributeValue.String(Character.toString(a))
        case StandardType.StringType         => (a: A) => AttributeValue.String(a.toString)
        case StandardType.BoolType           => (a: A) => AttributeValue.Bool(a.asInstanceOf[Boolean])
        case StandardType.ByteType           => (a: A) => AttributeValue.Binary(Chunk(a))
        case StandardType.BinaryType         => (a: A) => AttributeValue.Binary(a)
        case StandardType.ShortType          => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.IntType            => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.LongType           => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.FloatType          => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.DoubleType         => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.BigDecimalType     => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.BigIntegerType     => (a: A) => AttributeValue.Number(BigDecimal(a.toString))
        case StandardType.UUIDType           => (a: A) => AttributeValue.String(a.toString)
        case StandardType.DayOfWeekType      => (a: A) => AttributeValue.String(a.toString)
        case StandardType.DurationType       => (a: A) => AttributeValue.String(a.toString)
        case StandardType.InstantType        => (a: A) => AttributeValue.String(a.toString)
        case StandardType.LocalDateType      => (a: A) => AttributeValue.String(a.toString)
        case StandardType.LocalDateTimeType  => (a: A) => AttributeValue.String(a.toString)
        case StandardType.LocalTimeType      => (a: A) => AttributeValue.String(a.toString)
        case StandardType.MonthType          => (a: A) => AttributeValue.String(a.toString)
        case StandardType.MonthDayType       => (a: A) => AttributeValue.String(a.toString)
        case StandardType.OffsetDateTimeType => (a: A) => AttributeValue.String(a.toString)
        case StandardType.OffsetTimeType     => (a: A) => AttributeValue.String(a.toString)
        case StandardType.PeriodType         => (a: A) => AttributeValue.String(a.toString)
        case StandardType.YearType           => yearEncoder
        case StandardType.YearMonthType      => (a: A) => AttributeValue.String(a.toString)
        case StandardType.ZonedDateTimeType  => (a: A) => AttributeValue.String(a.toString)
        case StandardType.ZoneIdType         => (a: A) => AttributeValue.String(a.toString)
        case StandardType.ZoneOffsetType     => (a: A) => AttributeValue.String(a.toString)
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

    private def enumEncoder[Z](annotations: Chunk[Any], cases: Schema.Case[Z, _]*): Encoder[Z] =
      if (hasAnnotationAtClassLevel(annotations))
        enumWithAnnotationAtClassLevelEncoder(
          hasSimpleEnum(annotations),
          hasNoDiscriminator(annotations),
          discriminatorWithDefault(annotations),
          cases: _*
        )
      else
        defaultEnumEncoder(cases: _*)

    private def defaultEnumEncoder[Z](cases: Schema.Case[Z, _]*): Encoder[Z] =
      (a: Z) => {
        val fieldIndex = cases.indexWhere(c => c.deconstructOption(a).isDefined)
        if (fieldIndex > -1) {
          val case_ = cases(fieldIndex)
          val enc   = encoder(case_.schema.asInstanceOf[Schema[Any]])
          val av    = enc(a)
          val id    = maybeCaseName(case_.annotations).getOrElse(case_.id)
          AttributeValue.Map(Map.empty + (AttributeValue.String(id) -> av))
        } else
          AttributeValue.Null
      }

    private def enumWithAnnotationAtClassLevelEncoder[Z](
      hasSimpleEnum: Boolean,
      hasNoDiscriminator: Boolean,
      discriminator: String,
      cases: Schema.Case[Z, _]*
    ): Encoder[Z] =
      (a: Z) => {
        val fieldIndex = cases.indexWhere(c => c.deconstructOption(a).isDefined)
        if (fieldIndex > -1) {
          val case_ = cases(fieldIndex)
          val enc   = encoder(case_.schema.asInstanceOf[Schema[Any]])
          val av    = enc(a)
          val id    = maybeCaseName(case_.annotations).getOrElse(case_.id)
          val av2   = AttributeValue.String(id)
          av match { // TODO: review all pattern matches inside of a lambda
            case AttributeValue.Map(map) if hasNoDiscriminator =>
              AttributeValue.Map(map)
            case AttributeValue.Map(map)                       =>
              AttributeValue.Map(
                map + (AttributeValue.String(discriminator) -> av2)
              )
            case AttributeValue.Null                           =>
              if (hasSimpleEnum || (isCaseObject(case_) && hasNoDiscriminator))
                av2
              else
                // these are case objects and are a special case - they need to wrapped in an AttributeValue.Map
                AttributeValue.Map(Map(AttributeValue.String(discriminator) -> av2))
            case av                                            => throw new IllegalStateException(s"unexpected state $av")
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
              a.asInstanceOf[Set[java.math.BigDecimal]].map(bd => BigDecimal(bd))
            )
        // DerivedGen will wrap a java BigDecimal with a Transform for a scala BigDecimal so we need to peek ahead here
        case Schema.Transform(Schema.Primitive(bigDecimal, _), _, _, _, _)
            if bigDecimal.isInstanceOf[StandardType.BigDecimalType.type] =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[BigDecimal]])
        case Schema.Primitive(StandardType.BigIntegerType, _) =>
          (a: Set[A]) =>
            AttributeValue.NumberSet(a.asInstanceOf[Set[java.math.BigInteger]].map(i => BigDecimal(i.longValue)))
        // DerivedGen will wrap a java BigInteger with a Transform for a scala BigInt so we need to peek ahead here
        case Schema.Transform(Schema.Primitive(bigDecimal, _), _, _, _, _)
            if bigDecimal.isInstanceOf[StandardType.BigIntegerType.type] =>
          (a: Set[A]) => AttributeValue.NumberSet(a.asInstanceOf[Set[BigInt]].map(bi => BigDecimal(bi.bigInteger)))

        // AttributeValue.BinarySet
        case Schema.Primitive(StandardType.BinaryType, _)     =>
          (a: Set[A]) => AttributeValue.BinarySet(a.asInstanceOf[Set[Chunk[Byte]]])

        case l @ Schema.Lazy(_)                               =>
          setEncoder(l.schema)

        // Non native set
        case schema                                           =>
          sequenceEncoder[Chunk[A], A](encoder(schema), (c: Iterable[A]) => Chunk.fromIterable(c))
            .asInstanceOf[Encoder[Set[A]]]
      }

    private def mapEncoder[K, V](ks: Schema[K], vs: Schema[V]): Encoder[Map[K, V]] =
      ks match {
        case Schema.Primitive(StandardType.StringType, _) =>
          nativeMapEncoder(encoder(vs))
        case l @ Schema.Lazy(_)                           =>
          mapEncoder(l.schema, vs)
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
          case _: Schema.Map[_, _]        =>
            Map
          case _: Schema.Set[_]           =>
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
        case s: Optional[a]                        => optionalDecoder[a](decoder(s.schema))
        case Schema.Fail(s, _)                     => _ => Left(DecodingError(s))
        case Schema.GenericRecord(_, structure, _) => genericRecordDecoder(structure).asInstanceOf[Decoder[A]]
        case Schema.Tuple2(l, r, _)                => tupleDecoder(decoder(l), decoder(r))
        case Schema.Transform(codec, f, _, _, _)   => transformDecoder(codec, f)
        case s: Schema.Sequence[col, a, _]         => sequenceDecoder[col, a](decoder(s.elementSchema), s.fromChunk)
        case Schema.Either(l, r, _)                => eitherDecoder(decoder(l), decoder(r))
        case Primitive(standardType, _)            => primitiveDecoder(standardType)
        case l @ Schema.Lazy(_)                    =>
          lazy val dec = decoder(l.schema)
          (av: AttributeValue) => dec(av)
        case Schema.Dynamic(_)                     =>
          dynamicDecoder
        case Schema.Set(s, _)                      =>
          setDecoder(s).asInstanceOf[Decoder[A]]
        case Schema.Map(ks, vs, _)                 =>
          mapDecoder(ks, vs).asInstanceOf[Decoder[A]]

        case s @ Schema.CaseClass0(_, _, _)        => caseClass0Decoder(s)

        case s @ Schema.CaseClass1(_, _, _, _)                                                                                                  => caseClass1Decoder(s)
        case s @ Schema.CaseClass2(_, _, _, _, _)                                                                                               => caseClass2Decoder(s)
        case s @ Schema.CaseClass3(_, _, _, _, _, _)                                                                                            => caseClass3Decoder(s)
        case s @ Schema.CaseClass4(_, _, _, _, _, _, _)                                                                                         => caseClass4Decoder(s)
        case s @ Schema.CaseClass5(_, _, _, _, _, _, _, _)                                                                                      => caseClass5Decoder(s)
        case s @ Schema.CaseClass6(_, _, _, _, _, _, _, _, _)                                                                                   => caseClass6Decoder(s)
        case s @ Schema.CaseClass7(_, _, _, _, _, _, _, _, _, _)                                                                                => caseClass7Decoder(s)
        case s @ Schema.CaseClass8(_, _, _, _, _, _, _, _, _, _, _)                                                                             => caseClass8Decoder(s)
        case s @ Schema.CaseClass9(_, _, _, _, _, _, _, _, _, _, _, _)                                                                          => caseClass9Decoder(s)
        case s @ Schema.CaseClass10(_, _, _, _, _, _, _, _, _, _, _, _, _)                                                                      => caseClass10Decoder(s)
        case s @ Schema.CaseClass11(_, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                   =>
          caseClass11Decoder(s)
        case s @ Schema.CaseClass12(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                                =>
          caseClass12Decoder(s)
        case s @ Schema.CaseClass13(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                             =>
          caseClass13Decoder(s)
        case s @ Schema
              .CaseClass14(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass14Decoder(s)
        case s @ Schema
              .CaseClass15(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass15Decoder(s)
        case s @ Schema.CaseClass16(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                    =>
          caseClass16Decoder(s)
        case s @ Schema.CaseClass17(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                                 =>
          caseClass17Decoder(s)
        case s @ Schema.CaseClass18(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                              =>
          caseClass18Decoder(s)
        case s @ Schema.CaseClass19(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                           =>
          caseClass19Decoder(s)
        case s @ Schema.CaseClass20(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                           =>
          caseClass20Decoder(s)
        case s @ Schema.CaseClass21(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                           =>
          caseClass21Decoder(s)
        case s @ Schema.CaseClass22(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)                                           =>
          caseClass22Decoder(s)
        case Schema.Enum1(_, c, annotations)                                                                                                    =>
          enumDecoder(annotations, c)
        case Schema.Enum2(_, c1, c2, annotations)                                                                                               =>
          enumDecoder(annotations, c1, c2)
        case Schema.Enum3(_, c1, c2, c3, annotations)                                                                                           =>
          enumDecoder(annotations, c1, c2, c3)
        case Schema.Enum4(_, c1, c2, c3, c4, annotations)                                                                                       =>
          enumDecoder(annotations, c1, c2, c3, c4)
        case Schema.Enum5(_, c1, c2, c3, c4, c5, annotations)                                                                                   =>
          enumDecoder(annotations, c1, c2, c3, c4, c5)
        case Schema.Enum6(_, c1, c2, c3, c4, c5, c6, annotations)                                                                               =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6)
        case Schema.Enum7(_, c1, c2, c3, c4, c5, c6, c7, annotations)                                                                           =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7)
        case Schema.Enum8(_, c1, c2, c3, c4, c5, c6, c7, c8, annotations)                                                                       =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8)
        case Schema.Enum9(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, annotations)                                                                   =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9)
        case Schema.Enum10(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, annotations)                                                             =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
        case Schema.Enum11(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, annotations)                                                        =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
        case Schema.Enum12(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, annotations)                                                   =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
        case Schema.Enum13(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, annotations)                                              =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
        case Schema.Enum14(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, annotations)                                         =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
        case Schema.Enum15(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, annotations)                                    =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
        case Schema.Enum16(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, annotations)                               =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
        case Schema.Enum17(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, annotations)                          =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
        case Schema.Enum18(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, annotations)                     =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)
        case Schema.Enum19(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, annotations)                =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)
        case Schema
              .Enum20(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, annotations) =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
        case Schema.Enum21(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, annotations)      =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)
        case Schema.Enum22(_, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, annotations) =>
          enumDecoder(annotations, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
        case Schema.EnumN(_, cs, annotations)                                                                                                   =>
          enumDecoder(annotations, cs.toSeq: _*)
        case _                                                                                                                                  => throw new Exception("Match was non-exhaustive")

      }
    //scalafmt: { maxColumn = 120, optIn.configStyleArguments = true }

    private[dynamodb] def caseClass0Decoder[Z](schema: Schema.CaseClass0[Z]): Decoder[Z] =
      _ => Right(schema.defaultConstruct())

    private def dynamicDecoder[A]: Decoder[A] =
      decoder(Schema.dynamicValue).asInstanceOf[Decoder[A]]

    private def genericRecordDecoder(structure: FieldSet): Decoder[Any] =
      (av: AttributeValue) =>
        av match {
          case AttributeValue.Map(map) =>
            structure.toChunk.forEach {
              case Schema.Field(key, schema: Schema[a], _, _, _, _) =>
                val av  = map(AttributeValue.String(key))
                val dec = decoder(schema)
                dec(av) match {
                  case Right(value) => Right(key -> value)
                  case Left(s)      => Left(s)
                }
            }
              .map(ls => ListMap.newBuilder.++=(ls).result())
          case av                      => Left(DecodingError(s"Expected AttributeValue.Map but found $av"))
        }

    private def primitiveDecoder[A](standardType: StandardType[A]): Decoder[A] =
      standardType match {
        case StandardType.UnitType           => _ => Right(())
        case StandardType.StringType         =>
          (av: AttributeValue) => FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av)
        case StandardType.BoolType           =>
          (av: AttributeValue) => FromAttributeValue.booleanFromAttributeValue.fromAttributeValue(av)
        case StandardType.ShortType          =>
          (av: AttributeValue) => FromAttributeValue.shortFromAttributeValue.fromAttributeValue(av)
        case StandardType.IntType            =>
          (av: AttributeValue) => FromAttributeValue.intFromAttributeValue.fromAttributeValue(av)
        case StandardType.LongType           =>
          (av: AttributeValue) => FromAttributeValue.longFromAttributeValue.fromAttributeValue(av)
        case StandardType.FloatType          =>
          (av: AttributeValue) => FromAttributeValue.floatFromAttributeValue.fromAttributeValue(av)
        case StandardType.DoubleType         =>
          (av: AttributeValue) => FromAttributeValue.doubleFromAttributeValue.fromAttributeValue(av)
        case StandardType.BigDecimalType     =>
          (av: AttributeValue) =>
            FromAttributeValue.bigDecimalFromAttributeValue
              .fromAttributeValue(av)
              .map(_.bigDecimal)
        case StandardType.BigIntegerType     =>
          (av: AttributeValue) =>
            FromAttributeValue.bigDecimalFromAttributeValue
              .fromAttributeValue(av)
              .map(_.toBigInt.bigInteger)
        case StandardType.BinaryType         =>
          (av: AttributeValue) =>
            FromAttributeValue.binaryFromAttributeValue
              .fromAttributeValue(av)
              .map(Chunk.fromIterable(_))
        case StandardType.ByteType           =>
          (av: AttributeValue) =>
            FromAttributeValue.byteFromAttributeValue
              .fromAttributeValue(av)
        case StandardType.CharType           =>
          (av: AttributeValue) =>
            FromAttributeValue.stringFromAttributeValue
              .fromAttributeValue(av)
              .map { s =>
                val array = s.toCharArray
                array(0)
              }
        case StandardType.UUIDType           =>
          (av: AttributeValue) =>
            FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
              Try(UUID.fromString(s)).toEither.left.map(iae => DecodingError(s"Invalid UUID: ${iae.getMessage}"))
            }
        case StandardType.DayOfWeekType      =>
          (av: AttributeValue) => javaTimeStringParser(av)(DayOfWeek.valueOf(_))
        case StandardType.DurationType       =>
          (av: AttributeValue) => javaTimeStringParser(av)(Duration.parse(_))
        case StandardType.InstantType        =>
          (av: AttributeValue) => javaTimeStringParser(av)(Instant.parse)
        case StandardType.LocalDateType      =>
          (av: AttributeValue) => javaTimeStringParser(av)(LocalDate.parse)
        case StandardType.LocalDateTimeType  =>
          (av: AttributeValue) => javaTimeStringParser(av)(LocalDateTime.parse)
        case StandardType.LocalTimeType      =>
          (av: AttributeValue) => javaTimeStringParser(av)(LocalTime.parse)
        case StandardType.MonthType          =>
          (av: AttributeValue) => javaTimeStringParser(av)(Month.valueOf(_))
        case StandardType.MonthDayType       =>
          (av: AttributeValue) => javaTimeStringParser(av)(MonthDay.parse(_))
        case StandardType.OffsetDateTimeType =>
          (av: AttributeValue) => javaTimeStringParser(av)(OffsetDateTime.parse)
        case StandardType.OffsetTimeType     =>
          (av: AttributeValue) => javaTimeStringParser(av)(OffsetTime.parse)
        case StandardType.PeriodType         =>
          (av: AttributeValue) => javaTimeStringParser(av)(Period.parse(_))
        case StandardType.YearType           =>
          (av: AttributeValue) => javaTimeStringParser(av)(Year.parse(_))
        case StandardType.YearMonthType      =>
          (av: AttributeValue) => javaTimeStringParser(av)(YearMonth.parse(_))
        case StandardType.ZonedDateTimeType  =>
          (av: AttributeValue) => javaTimeStringParser(av)(ZonedDateTime.parse)
        case StandardType.ZoneIdType         =>
          (av: AttributeValue) => javaTimeStringParser(av)(ZoneId.of(_))
        case StandardType.ZoneOffsetType     =>
          (av: AttributeValue) => javaTimeStringParser(av)(ZoneOffset.of(_))
      }

    private def javaTimeStringParser[A](av: AttributeValue)(unsafeParse: String => A): Either[ItemError, A] =
      FromAttributeValue.stringFromAttributeValue.fromAttributeValue(av).flatMap { s =>
        val stringOrA = Try(unsafeParse(s)).toEither.left
          .map(e => DecodingError(s"error parsing string '$s': ${e.getMessage}"))
        stringOrA
      }

    private def transformDecoder[A, B](codec: Schema[A], f: A => Either[String, B]): Decoder[B] = {
      val dec = decoder(codec)
      (a: AttributeValue) => dec(a).flatMap(f(_).left.map(DecodingError.apply))
    }

    private def optionalDecoder[A](decoder: Decoder[A]): Decoder[Option[A]] = {
      case AttributeValue.Null => Right(None)
      case av                  =>
        println(s"XXXXXXXXXXXXX av: $av")
        decoder(av).map(Some(_))
    }

    private def eitherDecoder[A, B](decL: Decoder[A], decR: Decoder[B]): Decoder[Either[A, B]] = {
      case AttributeValue.Map(map) =>
        map.toList match {
          case (AttributeValue.String("Left"), a) :: Nil  =>
            decL(a).map(Left(_))
          case (AttributeValue.String("Right"), b) :: Nil =>
            decR(b).map(Right(_))
          case av                                         =>
            Left(DecodingError(s"AttributeValue.Map map element $av not expected."))
        }
      case av                      => Left(DecodingError(s"Expected AttributeValue.Map but found $av"))
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
            Left(DecodingError(s"Expected an AttributeValue.List of two elements but found $av"))
        }

    private def sequenceDecoder[Col, A](decoder: Decoder[A], to: Chunk[A] => Col): Decoder[Col] = {
      case AttributeValue.List(list) =>
        list.forEach(decoder(_)).map(xs => to(Chunk.fromIterable(xs)))
      case av                        => Left(DecodingError(s"unable to decode $av as a list"))
    }

    private def setDecoder[A](s: Schema[A]): Decoder[Set[A]] = {
      def nativeStringSetDecoder[A]: Decoder[Set[A]] = {
        case AttributeValue.StringSet(stringSet) =>
          Right(stringSet.asInstanceOf[Set[A]])
        case av                                  =>
          Left(DecodingError(s"Error: expected a string set but found '$av'"))
      }

      def nativeNumberSetDecoder[A](f: BigDecimal => A): Decoder[Set[A]] = {
        case AttributeValue.NumberSet(numberSet) =>
          Right(numberSet.map(f))
        case av                                  =>
          Left(DecodingError(s"Error: expected a number set but found '$av'"))
      }

      def nativeBinarySetDecoder[A]: Decoder[Set[A]] = {
        case AttributeValue.BinarySet(setOfChunkOfByte) =>
          val set: Set[Chunk[Byte]] = setOfChunkOfByte.toSet.map((xs: Iterable[Byte]) => Chunk.fromIterable(xs))
          Right(set.asInstanceOf[Set[A]])
        case av                                         =>
          Left(DecodingError(s"Error: expected a Set of Chunk of Byte but found '$av'"))
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
        case Schema.Transform(Schema.Primitive(bigDecimal, _), _, _, _, _)
            if bigDecimal.isInstanceOf[StandardType.BigDecimalType.type] =>
          nativeNumberSetDecoder[BigDecimal](_.bigDecimal).asInstanceOf[Decoder[Set[A]]]
        case Schema.Primitive(StandardType.BigIntegerType, _) =>
          nativeNumberSetDecoder[BigInteger](bd => bd.toBigInt.bigInteger)
        case Schema.Transform(Schema.Primitive(bigInt, _), _, _, _, _)
            if bigInt.isInstanceOf[StandardType.BigIntegerType.type] =>
          nativeNumberSetDecoder[BigInt](_.toBigInt).asInstanceOf[Decoder[Set[A]]]

        // BinarySet
        case Schema.Primitive(StandardType.BinaryType, _)     =>
          nativeBinarySetDecoder

        case l @ Schema.Lazy(_)                               =>
          setDecoder(l.schema)

        // non native set
        case _                                                =>
          nonNativeSetDecoder(decoder(s))
      }
    }

    private def nonNativeSetDecoder[A](decA: Decoder[A]): Decoder[Set[A]] = { (av: AttributeValue) =>
      av match {
        case AttributeValue.List(listOfAv) =>
          val errorOrList = listOfAv.forEach { av =>
            decA(av)
          }
          errorOrList.map(_.toSet)
        case av                            => Left(DecodingError(s"Error: expected AttributeValue.List but found $av"))
      }
    }

    private def mapDecoder[K, V](ks: Schema[K], vs: Schema[V]): Decoder[Map[_, V]] =
      ks match {
        case Schema.Primitive(StandardType.StringType, _) =>
          nativeMapDecoder(decoder(vs))
        case l @ Schema.Lazy(_)                           =>
          mapDecoder(l.schema, vs)
        case _                                            =>
          nonNativeMapDecoder(decoder(ks), decoder(vs))
      }

    private def nativeMapDecoder[V](dec: Decoder[V]): Decoder[Map[String, V]] =
      (av: AttributeValue) => {
        av match {
          case AttributeValue.Map(map) =>
            val xs: Iterable[Either[ItemError, (String, V)]] = map.map {
              case (k, v) =>
                dec(v) match {
                  case Right(decV) => Right((k.value, decV))
                  case Left(s)     => Left(s)
                }
            }
            xs.flip.map(_.toMap)
          case av                      => Left(DecodingError(s"Error: expected AttributeValue.Map but found $av"))
        }
      }

    def nonNativeMapDecoder[A, B](decA: Decoder[A], decB: Decoder[B]): Decoder[Map[A, B]] =
      (av: AttributeValue) => {
        av match {
          case AttributeValue.List(listOfAv) =>
            val errorOrListOfTuple = listOfAv.forEach {
              case avList @ AttributeValue.List(_) =>
                tupleDecoder(decA, decB)(avList)
              case av                              =>
                Left(DecodingError(s"Error: expected AttributeValue.List but found $av"))
            }
            errorOrListOfTuple.map(_.toMap)
          case av                            => Left(DecodingError(s"Error: expected AttributeValue.List but found $av"))
        }
      }

    private def enumDecoder[Z](annotations: Chunk[Any], cases: Schema.Case[Z, _]*): Decoder[Z] =
      if (hasAnnotationAtClassLevel(annotations))
        enumWithAnnotationAtClassLevelDecoder(
          hasNoDiscriminator(annotations),
          discriminatorWithDefault(annotations),
          cases: _*
        )
      else
        defaultEnumDecoder(cases: _*)

    private def defaultEnumDecoder[Z](cases: Schema.Case[Z, _]*): Decoder[Z] =
      (av: AttributeValue) =>
        av match {
          case AttributeValue.Map(map) =>
            // default enum encoding uses a Map with a single entry that denotes the type
            // TODO: think about being stricter and rejecting Maps with > 1 entry ???
            map.toList.headOption.fold[Either[ItemError, Z]](Left(DecodingError(s"map $av is empty"))) {
              case (AttributeValue.String(subtype), av) =>
                cases.find { c =>
                  maybeCaseName(c.annotations).fold(c.id == subtype)(_ == subtype)
                } match {
                  case Some(c) =>
                    decoder(c.schema)(av).map(_.asInstanceOf[Z])
                  case None    =>
                    Left(DecodingError(s"subtype $subtype not found"))
                }
            }
          case _                       =>
            Left(DecodingError(s"invalid AttributeValue $av"))
        }

    private def enumWithAnnotationAtClassLevelDecoder[Z](
      hasNoDiscriminatorTag: Boolean,
      discriminator: String,
      cases: Schema.Case[Z, _]*
    ): Decoder[Z] = { (av: AttributeValue) =>
      def findCase(value: String): Either[ItemError, Schema.Case[Z, _]] =
        cases.find {
          case Schema.Case(_, _, _, _, _, Chunk(caseName(const))) => const == value
          case Schema.Case(id, _, _, _, _, _)                     => id == value
        }.toRight(DecodingError(s"type name '$value' not found in schema cases"))

      def decode(id: String): Either[ItemError, Z] =
        findCase(id).flatMap { c =>
          val dec = decoder(c.schema)
          dec(av).map(_.asInstanceOf[Z])
        }

      av match {
        case AttributeValue.String(id)                      =>
          decode(id)
        case AttributeValue.Map(m) if hasNoDiscriminatorTag =>
          def arity(s: Schema[_]): Int =
            s match {
              case c: Schema.Lazy[_]   => arity(c.schema)
              case c: Schema.Record[_] =>
                c.fields.size
              case _                   =>
                0
            }

          val numberOfItemAttributes = m.size
          val rights                 =
            cases
              .filter(c => !isCaseObject(c) && arity(c.schema) == numberOfItemAttributes)
              .sortWith((a: Schema.Case[_, _], b: Schema.Case[_, _]) => arity(a.schema) > arity(b.schema))
              .map(c => decoder(c.schema)(av))
              .filter(_.isRight)

          rights.toList match {
            case Nil      => Left(ItemError.DecodingError(s"All sub type decoders failed for $av"))
            case a :: Nil => a.map(_.asInstanceOf[Z])
            case _        =>
              Left(ItemError.DecodingError(s"More than one sub type decoder succeeded for $av"))
          }

        case AttributeValue.Map(map)                        =>
          map
            .get(AttributeValue.String(discriminator))
            .fold[Either[ItemError, Z]](
              Left(DecodingError(s"map $av does not contain discriminator field '$discriminator'"))
            ) {
              case AttributeValue.String(typeName) =>
                decode(typeName)
              case av                              =>
                Left(DecodingError(s"expected string type but found $av"))
            }
        case _                                              =>
          Left(DecodingError(s"unexpected AttributeValue type $av"))
      }
    }

    private[dynamodb] def decodeFields(
      av: AttributeValue,
      fields: Schema.Field[_, _]*
    ): Either[ItemError, List[Any]] =
      av match {
        case AttributeValue.Map(map) =>
          fields.toList.forEach {
            case Schema.Field(key, schema, _, _, _, _) =>
              val dec                            = decoder(schema)
              val k                              = key // @fieldName is respected by the zio-schema macro
              val maybeValue                     = map.get(AttributeValue.String(k))
              val maybeDecoder                   = maybeValue.map(dec).toRight(DecodingError(s"field '$k' not found in $av"))
              val either: Either[ItemError, Any] = for {
                decoder <- maybeDecoder
                decoded <- decoder
              } yield decoded

              if (maybeValue.isEmpty)
                ContainerField.containerField(schema) match {
                  case ContainerField.Optional => Right(None)
                  case ContainerField.List     => Right(List.empty)
                  case ContainerField.Map      => Right(Map.empty)
                  case ContainerField.Set      => Right(Set.empty)
                  case ContainerField.Scalar   => either
                }
              else
                either
          }
            .map(_.toList)
        case _                       =>
          Left(DecodingError(s"$av is not an AttributeValue.Map"))
      }

  } // end Decoder

  private def isCaseObject(s: Schema.Case[_, _]) =
    s match {
      case Schema.Case(_, Schema.CaseClass0(_, _, _), _, _, _, _) =>
        true
      case _                                                      =>
        false
    }

} // end Codec

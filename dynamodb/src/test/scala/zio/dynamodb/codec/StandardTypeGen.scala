package zio.dynamodb.codec

import java.math.{ BigDecimal => JBigDecimal, BigInteger => JBigInt }

import zio.test.{ Gen, Sized }
import zio.schema.StandardType

import scala.jdk.CollectionConverters._

object StandardTypeGen {

  def anyStandardType[A]: Gen[Any, StandardType[A]] =
    Gen
      .fromIterable(
        List(
          (StandardType.StringType),
          (StandardType.BoolType),
          (StandardType.ShortType),
          (StandardType.IntType),
          (StandardType.LongType),
          (StandardType.FloatType),
          (StandardType.DoubleType),
          (StandardType.BinaryType),
          (StandardType.BigDecimalType),
          (StandardType.BigIntegerType),
          (StandardType.CharType),
          (StandardType.CurrencyType),
          (StandardType.UUIDType),
          (StandardType.DayOfWeekType),
          (StandardType.DurationType),
          (StandardType.InstantType),
          (StandardType.LocalDateType),
          (StandardType.LocalDateTimeType),
          (StandardType.LocalTimeType),
          (StandardType.MonthType),
          (StandardType.MonthDayType),
          (StandardType.OffsetDateTimeType),
          (StandardType.OffsetTimeType),
          (StandardType.PeriodType),
          (StandardType.YearType),
          (StandardType.YearMonthType),
          (StandardType.ZonedDateTimeType),
          (StandardType.ZoneIdType)
        )
        //FIXME For some reason adding this causes other unrelated tests to break.
//    Gen.const(StandardType.ZoneOffset)
      )
      .asInstanceOf[Gen[Any, StandardType[A]]]

  val javaBigInt: Gen[Any, JBigInt] =
    Gen.bigInt(JBigInt.valueOf(Long.MinValue), JBigInt.valueOf(Long.MaxValue)).map { sBigInt =>
      new JBigInt(sBigInt.toByteArray)
    }

  val javaBigDecimal: Gen[Any, JBigDecimal] =
    Gen.bigDecimal(JBigDecimal.valueOf(Long.MinValue), JBigDecimal.valueOf(Long.MaxValue)).map(_.bigDecimal)

  type StandardTypeAndGen[A] = (StandardType[A], Gen[Sized, A])

  def anyStandardTypeAndGen[A]: Gen[Any, StandardTypeAndGen[A]] =
    anyStandardType[A].map {
      case typ: StandardType.CurrencyType.type       =>
        val allCurrencies: List[java.util.Currency] = java.util.Currency.getAvailableCurrencies.asScala.toList
        (typ -> Gen.fromIterable(allCurrencies)).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.StringType.type         => (typ -> Gen.string).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.BoolType.type           => (typ -> Gen.boolean).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.ShortType.type          => (typ -> Gen.short).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.IntType.type            => (typ -> Gen.int).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.LongType.type           => (typ -> Gen.long).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.FloatType.type          => (typ -> Gen.float).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.DoubleType.type         => (typ -> Gen.double).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.BinaryType.type         => (typ -> Gen.chunkOf(Gen.byte)).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.CharType.type           => (typ -> Gen.asciiChar).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.UUIDType.type           => (typ -> Gen.uuid).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.BigDecimalType.type     => (typ -> javaBigDecimal).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.BigIntegerType.type     => (typ -> javaBigInt).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.DayOfWeekType.type      => (typ -> JavaTimeGen.anyDayOfWeek).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.DurationType.type       => (typ -> JavaTimeGen.anyDuration).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.InstantType.type        => (typ -> JavaTimeGen.anyInstant).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.LocalDateType.type      => (typ -> JavaTimeGen.anyLocalDate).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.LocalDateTimeType.type  =>
        (typ -> JavaTimeGen.anyLocalDateTime).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.LocalTimeType.type      => (typ -> JavaTimeGen.anyLocalTime).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.MonthType.type          => (typ -> JavaTimeGen.anyMonth).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.MonthDayType.type       => (typ -> JavaTimeGen.anyMonthDay).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.OffsetDateTimeType.type =>
        (typ -> JavaTimeGen.anyOffsetDateTime).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.OffsetTimeType.type     =>
        (typ -> JavaTimeGen.anyOffsetTime).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.PeriodType.type         => (typ -> JavaTimeGen.anyPeriod).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.YearType.type           => (typ -> JavaTimeGen.anyYear).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.YearMonthType.type      => (typ -> JavaTimeGen.anyYearMonth).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.ZonedDateTimeType.type  =>
        (typ -> JavaTimeGen.anyZonedDateTime).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.ZoneIdType.type         => (typ -> JavaTimeGen.anyZoneId).asInstanceOf[StandardTypeAndGen[A]]
      case typ: StandardType.ZoneOffsetType.type     =>
        (typ -> JavaTimeGen.anyZoneOffset).asInstanceOf[StandardTypeAndGen[A]]
      case _                                         =>
        // TODO: Avi - plug this gap
        println("XXXXXXXXXXXXXXXXXXXXXXXX RAAAS CLAAAAAT! XXXXXXXXXXX")
        (StandardType.UnitType -> Gen.unit).asInstanceOf[StandardTypeAndGen[A]]
    }
}

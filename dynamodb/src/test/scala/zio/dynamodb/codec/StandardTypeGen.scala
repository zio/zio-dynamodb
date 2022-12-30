package zio.dynamodb.codec

import java.math.{ BigDecimal => JBigDecimal, BigInteger => JBigInt }

import zio.test.{ Gen, Sized }
import zio.schema.StandardType

object StandardTypeGen {

  val anyStandardType: Gen[Any, StandardType[_]] = Gen.fromIterable(
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

  val javaBigInt: Gen[Any, JBigInt] =
    Gen.bigInt(JBigInt.valueOf(Long.MinValue), JBigInt.valueOf(Long.MaxValue)).map { sBigInt =>
      new JBigInt(sBigInt.toByteArray)
    }

  val javaBigDecimal: Gen[Any, JBigDecimal] =
    Gen.bigDecimal(JBigDecimal.valueOf(Long.MinValue), JBigDecimal.valueOf(Long.MaxValue)).map(_.bigDecimal)

  type StandardTypeAndGen[A] = (StandardType[A], Gen[Sized, A])

  val anyStandardTypeAndGen: Gen[Any, StandardTypeAndGen[_]] =
    anyStandardType.map {
      case typ: StandardType.StringType.type         => typ -> Gen.string
      case typ: StandardType.BoolType.type           => typ -> Gen.boolean
      case typ: StandardType.ShortType.type          => typ -> Gen.short
      case typ: StandardType.IntType.type            => typ -> Gen.int
      case typ: StandardType.LongType.type           => typ -> Gen.long
      case typ: StandardType.FloatType.type          => typ -> Gen.float
      case typ: StandardType.DoubleType.type         => typ -> Gen.double
      case typ: StandardType.BinaryType.type         => typ -> Gen.chunkOf(Gen.byte)
      case typ: StandardType.CharType.type           => typ -> Gen.asciiChar
      case typ: StandardType.UUIDType.type           => typ -> Gen.uuid
      case typ: StandardType.BigDecimalType.type     => typ -> javaBigDecimal
      case typ: StandardType.BigIntegerType.type     => typ -> javaBigInt
      case typ: StandardType.DayOfWeekType.type      => typ -> JavaTimeGen.anyDayOfWeek
      case typ: StandardType.DurationType.type       => typ -> JavaTimeGen.anyDuration
      case typ: StandardType.InstantType.type        => typ -> JavaTimeGen.anyInstant
      case typ: StandardType.LocalDateType.type      => typ -> JavaTimeGen.anyLocalDate
      case typ: StandardType.LocalDateTimeType.type  => typ -> JavaTimeGen.anyLocalDateTime
      case typ: StandardType.LocalTimeType.type      => typ -> JavaTimeGen.anyLocalTime
      case typ: StandardType.MonthType.type          => typ -> JavaTimeGen.anyMonth
      case typ: StandardType.MonthDayType.type       => typ -> JavaTimeGen.anyMonthDay
      case typ: StandardType.OffsetDateTimeType.type => typ -> JavaTimeGen.anyOffsetDateTime
      case typ: StandardType.OffsetTimeType.type     => typ -> JavaTimeGen.anyOffsetTime
      case typ: StandardType.PeriodType.type         => typ -> JavaTimeGen.anyPeriod
      case typ: StandardType.YearType.type           => typ -> JavaTimeGen.anyYear
      case typ: StandardType.YearMonthType.type      => typ -> JavaTimeGen.anyYearMonth
      case typ: StandardType.ZonedDateTimeType.type  => typ -> JavaTimeGen.anyZonedDateTime
      case typ: StandardType.ZoneIdType.type         => typ -> JavaTimeGen.anyZoneId
      case typ: StandardType.ZoneOffsetType.type     => typ -> JavaTimeGen.anyZoneOffset
      case _                                         => StandardType.UnitType -> Gen.unit: StandardTypeAndGen[_]
    }
}

// package zio.dynamodb.codec

// import zio.schema._
// import zio.test.{ Gen, Sized }

// import java.time.format.DateTimeFormatter

// object StandardTypeGen {

//   val anyStandardType: Gen[Any, StandardType[_]] = Gen.fromIterable(
//     List(
//       (StandardType.StringType),
//       (StandardType.BoolType),
//       (StandardType.ShortType),
//       (StandardType.IntType),
//       (StandardType.LongType),
//       (StandardType.FloatType),
//       (StandardType.DoubleType),
//       (StandardType.BinaryType),
//       (StandardType.BigDecimalType),
//       (StandardType.BigIntegerType),
//       (StandardType.CharType),
//       (StandardType.DayOfWeekType),
//       (StandardType.DurationType),
//       (StandardType.InstantType(
//         DateTimeFormatter.ISO_INSTANT
//       )), // TODO: original uses (StandardType.Instant(DateTimeFormatter.ISO_DATE_TIME)),
//       // raise a PR to push this change back to main
//       (StandardType.LocalDateType(DateTimeFormatter.ISO_DATE)),
//       (StandardType.LocalDateTimeType(DateTimeFormatter.ISO_LOCAL_DATE_TIME)),
//       (StandardType.LocalTimeType(DateTimeFormatter.ISO_LOCAL_TIME)),
//       (StandardType.MonthType),
//       (StandardType.MonthDayType),
//       (StandardType.OffsetDateTimeType(DateTimeFormatter.ISO_OFFSET_DATE_TIME)),
//       (StandardType.OffsetTimeType(DateTimeFormatter.ISO_OFFSET_TIME)),
//       (StandardType.PeriodType),
//       (StandardType.YearType),
//       (StandardType.YearMonthType),
//       (StandardType.ZonedDateTimeType(DateTimeFormatter.ISO_ZONED_DATE_TIME)),
//       (StandardType.ZoneIdType)
//     )
//     //FIXME For some reason adding this causes other unrelated tests to break.
// //    Gen.const(StandardType.ZoneOffset)
//   )

//   type StandardTypeAndGen[A] = (StandardType[A], Gen[Sized, A])

//   val anyStandardTypeAndGen: Gen[Any, StandardTypeAndGen[_]] =
//     anyStandardType.map {
//       case typ: StandardType.StringType.type     => typ -> Gen.string
//       case typ: StandardType.BoolType.type       => typ -> Gen.boolean
//       case typ: StandardType.ShortType.type      => typ -> Gen.short
//       case typ: StandardType.IntType.type        => typ -> Gen.int
//       case typ: StandardType.LongType.type       => typ -> Gen.long
//       case typ: StandardType.FloatType.type      => typ -> Gen.float
//       case typ: StandardType.DoubleType.type     => typ -> Gen.double
//       case typ: StandardType.BinaryType.type     => typ -> Gen.chunkOf(Gen.byte)
//       case typ: StandardType.CharType.type       => typ -> Gen.asciiChar
//       case typ: StandardType.BigDecimalType.type => typ -> Gen.double.map(d => java.math.BigDecimal.valueOf(d))
//       case typ: StandardType.BigIntegerType.type => typ -> Gen.long.map(n => java.math.BigInteger.valueOf(n))
//       case typ: StandardType.DayOfWeekType.type  => typ -> JavaTimeGen.anyDayOfWeek
//       case typ: StandardType.DurationType.type   => typ -> JavaTimeGen.anyDuration
//       case typ: StandardType.InstantType         => typ -> JavaTimeGen.anyInstant
//       case typ: StandardType.LocalDateType       => typ -> JavaTimeGen.anyLocalDate
//       case typ: StandardType.LocalDateTimeType   => typ -> JavaTimeGen.anyLocalDateTime
//       case typ: StandardType.LocalTimeType       => typ -> JavaTimeGen.anyLocalTime
//       case typ: StandardType.MonthType.type      => typ -> JavaTimeGen.anyMonth
//       case typ: StandardType.MonthDayType.type   => typ -> JavaTimeGen.anyMonthDay
//       case typ: StandardType.OffsetDateTimeType  => typ -> JavaTimeGen.anyOffsetDateTime
//       case typ: StandardType.OffsetTimeType      => typ -> JavaTimeGen.anyOffsetTime
//       case typ: StandardType.PeriodType.type     => typ -> JavaTimeGen.anyPeriod
//       case typ: StandardType.YearType.type       => typ -> JavaTimeGen.anyYear
//       case typ: StandardType.YearMonthType.type  => typ -> JavaTimeGen.anyYearMonth
//       case typ: StandardType.ZonedDateTimeType   => typ -> JavaTimeGen.anyZonedDateTime
//       case typ: StandardType.ZoneIdType.type     => typ -> JavaTimeGen.anyZoneId
//       case typ: StandardType.ZoneOffsetType.type => typ -> JavaTimeGen.anyZoneOffset
//       case _                                     => StandardType.UnitType -> Gen.unit: StandardTypeAndGen[_]
//     }
// }

/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb.blocks.schema

import zio.blocks.chunk.Chunk
import zio.blocks.maybe.Maybe
import zio.blocks.schema.{ DynamicValue, Modifier, NameMapper, PrimitiveValue, Schema }
import zio.blocks.schema.json.{ DiscriminatorKind, Json }
import zio.dynamodb.AttributeValue
import zio.test._
import zio.test.Assertion.{
  anything,
  contains,
  equalTo,
  hasAt,
  hasField,
  hasKey,
  hasSize,
  isFalse,
  isRight,
  isSome,
  isSubtype,
  isTrue
}

import java.time._
import java.util.{ Currency, UUID }

object DynamoDBCodecDeriverSpec extends ZIOSpecDefault {

  // ---------- test models --------------------------------------------------

  case class Person(name: String, age: Int)
  object Person { implicit val schema: Schema[Person] = Schema.derived }

  case class Event(id: String, count: Long)
  object Event { implicit val schema: Schema[Event] = Schema.derived }

  case class WithOption(label: String, note: Option[String])
  object WithOption { implicit val schema: Schema[WithOption] = Schema.derived }

  case class Wrapper(value: String)
  object Wrapper { implicit val schema: Schema[Wrapper] = Schema.derived }

  sealed trait Color
  object Color {
    case object Red   extends Color
    case object Green extends Color
    case object Blue  extends Color
    implicit val schema: Schema[Color] = Schema.derived
  }

  sealed trait Shape
  object Shape {
    case class Circle(radius: Int)           extends Shape
    case class Rect(width: Int, height: Int) extends Shape
    implicit val schema: Schema[Shape] = Schema.derived
  }

  // @Modifier.discriminator — overrides deriver-level discriminatorKind to Field("_type")
  @Modifier.discriminator("_type")
  sealed trait TaggedShape
  object TaggedShape {
    case class Circle(radius: Int)           extends TaggedShape
    case class Rect(width: Int, height: Int) extends TaggedShape
    implicit val schema: Schema[TaggedShape] = Schema.derived
  }

  // @Modifier.caseNaming("snake_case") — case names mapped to snake_case for encode + decode
  @Modifier.caseNaming("snake_case")
  sealed trait SnakeShape
  object SnakeShape {
    case class CircleShape(radius: Int)  extends SnakeShape
    case class RectShape(w: Int, h: Int) extends SnakeShape
    implicit val schema: Schema[SnakeShape] = Schema.derived
  }

  // Both annotations together: Field discriminator + snake_case case names
  @Modifier.discriminator("kind")
  @Modifier.caseNaming("snake_case")
  sealed trait TaggedSnakeShape
  object TaggedSnakeShape {
    case class CircleShape(radius: Int)  extends TaggedSnakeShape
    case class RectShape(w: Int, h: Int) extends TaggedSnakeShape
    implicit val schema: Schema[TaggedSnakeShape] = Schema.derived
  }

  // @Modifier.caseNaming on an enum (no-field cases)
  @Modifier.caseNaming("snake_case")
  sealed trait SnakeColor
  object SnakeColor {
    case object DarkRed   extends SnakeColor
    case object LightBlue extends SnakeColor
    implicit val schema: Schema[SnakeColor] = Schema.derived
  }

  // ---------- Group 2 test models ------------------------------------------

  // @Modifier.encodeTransient: field is not encoded but IS decoded (gets its default)
  case class WithEncodeTransient(id: String, @Modifier.encodeTransient() score: Int = 0)
  object WithEncodeTransient { implicit val schema: Schema[WithEncodeTransient] = Schema.derived }

  // transientDefaultValue: field with default is omitted from DDB map when value == default
  case class WithDefault(id: String, count: Int = 0, tag: String = "untagged")
  object WithDefault { implicit val schema: Schema[WithDefault] = Schema.derived }

  // emptyCollectionConstructor: absent collection field gets empty collection, not an error
  case class WithCollections(id: String, tags: List[String], scores: Map[String, Int])
  object WithCollections { implicit val schema: Schema[WithCollections] = Schema.derived }

  case class WithDefaultField(name: String, rank: Int = 99)
  object WithDefaultField { implicit val schema: Schema[WithDefaultField] = Schema.derived }

  // covers every primitive register type's default-value handling
  // (FieldInfo.defaultEqualsValue / setMissingValueOrDefault), not just Int/String
  case class WithPrimitiveDefaults(
    id: String,
    longVal: Long = 100L,
    boolVal: Boolean = true,
    byteVal: Byte = 1,
    charVal: Char = 'x',
    shortVal: Short = 10,
    floatVal: Float = 1.5f,
    doubleVal: Double = 2.5
  )
  object WithPrimitiveDefaults { implicit val schema: Schema[WithPrimitiveDefaults] = Schema.derived }

  case class WithMaybe(id: String, value: Maybe[String])
  object WithMaybe { implicit val schema: Schema[WithMaybe] = Schema.derived }

  case class WithMaybeInt(id: String, count: Maybe[Int])
  object WithMaybeInt { implicit val schema: Schema[WithMaybeInt] = Schema.derived }

  // @Modifier.noExtraFields: unknown DDB map keys are rejected on decode
  @Modifier.noExtraFields()
  case class Strict(id: String, value: Int)
  object Strict { implicit val schema: Schema[Strict] = Schema.derived }

  // Used with the global rejectExtraFields deriver flag
  case class Lenient(id: String, value: Int)
  object Lenient { implicit val schema: Schema[Lenient] = Schema.derived }

  // @Modifier.fieldNaming: per-type snake_case field name mapping
  @Modifier.fieldNaming("snake_case")
  case class SnakeRecord(firstName: String, lastName: String, homeCity: String)
  object SnakeRecord { implicit val schema: Schema[SnakeRecord] = Schema.derived }

  // Same fields, no annotation — should use deriver-level mapper (Identity by default)
  case class CamelRecord(firstName: String, lastName: String)
  object CamelRecord { implicit val schema: Schema[CamelRecord] = Schema.derived }

  // Variant with Field discriminator + noExtraFields on the case record
  @Modifier.discriminator("_type")
  sealed trait StrictTagged
  object StrictTagged {
    @Modifier.noExtraFields()
    case class Node(x: Int) extends StrictTagged
    implicit val schema: Schema[StrictTagged] = Schema.derived
  }

  // ---------- helper -------------------------------------------------------

  private def codecFor[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(DynamoDBCodecDeriver).derive

  private def roundTrip[A: Schema](value: A): Either[?, A] = {
    val c = codecFor[A]
    c.encoder(value) match {
      case av => c.decoder(av)
    }
  }

  // ---------- test models: extended primitives --------------------------------

  case class AllPrimitives(
    b: Boolean,
    by: Byte,
    s: Short,
    i: Int,
    l: Long,
    f: Float,
    d: Double,
    c: Char,
    bi: BigInt,
    bd: BigDecimal
  )
  object AllPrimitives { implicit val schema: Schema[AllPrimitives] = Schema.derived }

  case class WithTemporals(
    instant: Instant,
    localDate: LocalDate,
    localDateTime: LocalDateTime,
    localTime: LocalTime,
    offsetDateTime: OffsetDateTime,
    offsetTime: OffsetTime,
    zonedDateTime: ZonedDateTime,
    zoneId: ZoneId,
    zoneOffset: ZoneOffset,
    duration: Duration,
    period: Period,
    year: Year,
    yearMonth: YearMonth,
    dayOfWeek: DayOfWeek,
    month: Month,
    monthDay: MonthDay
  )
  object WithTemporals { implicit val schema: Schema[WithTemporals] = Schema.derived }

  case class WithUuidCurrency(id: UUID, currency: Currency)
  object WithUuidCurrency { implicit val schema: Schema[WithUuidCurrency] = Schema.derived }

  case class BinaryPayload(id: String, payload: Array[Byte])
  object BinaryPayload { implicit val schema: Schema[BinaryPayload] = Schema.derived }

  // @transient fields must have a default value (enforced by Schema.derived at compile time).
  // cached is not stored in DynamoDB; it gets the register-zero value (0) on decode.
  case class WithTransient(id: String, score: Int, @Modifier.transient() cached: Int = 0)
  object WithTransient { implicit val schema: Schema[WithTransient] = Schema.derived }

  // ---------- spec ---------------------------------------------------------

  def spec = suite("DynamoDBCodecDeriverSpec")(
    primitiveSuite,
    temporalSuite,
    miscPrimitiveSuite,
    recordSuite,
    transientSuite,
    optionSuite,
    enumSuite,
    adtSuite,
    adtNoneDiscriminatorSuite,
    adtDiscriminatorFieldSuite,
    adtCaseNamingSuite,
    adtDiscriminatorAndCaseNamingSuite,
    enumCaseNamingSuite,
    encodeTransientSuite,
    transientDefaultValueSuite,
    defaultValueDecodeSuite,
    primitiveDefaultsSuite,
    emptyCollectionSuite,
    maybeSuite,
    sequenceSuite,
    nativeSetSuite,
    mapSuite,
    nonNativeMapSuite,
    dynamicValueSuite,
    jsonAVSuite,
    byteSequenceCompatSuite,
    tupleCompatSuite,
    yearCompatSuite,
    rejectExtraFieldsSuite,
    fieldNamingSuite
  )

  // ---- primitives --------------------------------------------------------

  private val primitiveSuite = suite("primitive codecs")(
    suite("String")(
      test("encodes to AttributeValue.String") {
        val codec = codecFor[String]
        assertTrue(codec.encoder("hello") == AttributeValue.String("hello"))
      },
      test("decodes from AttributeValue.String") {
        val codec = codecFor[String]
        assertTrue(codec.decoder(AttributeValue.String("hello")) == Right("hello"))
      },
      test("decode error on non-String") {
        val codec = codecFor[String]
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(1))).isLeft)
      }
    ),
    suite("Int")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[Int]
        assertTrue(codec.encoder(42) == AttributeValue.Number(BigDecimal.valueOf(42L)))
      },
      test("decodes from AttributeValue.Number") {
        val codec = codecFor[Int]
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(42))) == Right(42))
      },
      test("decode error on non-Number") {
        val codec = codecFor[Int]
        assertTrue(codec.decoder(AttributeValue.String("x")).isLeft)
      }
    ),
    suite("Long")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[Long]
        assertTrue(codec.encoder(Long.MaxValue) == AttributeValue.Number(BigDecimal.valueOf(Long.MaxValue)))
      },
      test("decodes from AttributeValue.Number") {
        val codec = codecFor[Long]
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal.valueOf(Long.MaxValue))) == Right(Long.MaxValue))
      },
      test("decode error on non-Number") {
        val codec = codecFor[Long]
        assertTrue(codec.decoder(AttributeValue.String("nope")).isLeft)
      }
    ),
    suite("Boolean")(
      test("encodes to AttributeValue.Bool") {
        val codec = codecFor[Boolean]
        assertTrue(
          codec.encoder(true) == AttributeValue.Bool(true) && codec.encoder(false) == AttributeValue.Bool(false)
        )
      },
      test("decodes from AttributeValue.Bool") {
        val codec = codecFor[Boolean]
        assertTrue(codec.decoder(AttributeValue.Bool(true)) == Right(true))
      },
      test("decode error on non-Bool") {
        val codec = codecFor[Boolean]
        assertTrue(codec.decoder(AttributeValue.String("true")).isLeft)
      }
    ),
    suite("Byte")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[Byte]
        assertTrue(codec.encoder(42.toByte) == AttributeValue.Number(BigDecimal.valueOf(42L)))
      },
      test("round-trips")(assertTrue(roundTrip[Byte](127.toByte) == Right(127.toByte))),
      test("decode error on non-Number") {
        val codec = codecFor[Byte]
        assertTrue(codec.decoder(AttributeValue.String("x")).isLeft)
      }
    ),
    suite("Short")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[Short]
        assertTrue(codec.encoder(1000.toShort) == AttributeValue.Number(BigDecimal.valueOf(1000L)))
      },
      test("round-trips")(assertTrue(roundTrip[Short](32767.toShort) == Right(32767.toShort))),
      test("decode error on non-Number") {
        val codec = codecFor[Short]
        assertTrue(codec.decoder(AttributeValue.Bool(false)).isLeft)
      }
    ),
    suite("Float")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[Float]
        assertTrue(codec.encoder(3.14f) == AttributeValue.Number(BigDecimal.valueOf(3.14f.toDouble)))
      },
      test("round-trips")(assertTrue(roundTrip[Float](1.5f).map(_.toDouble) == Right(1.5f.toDouble))),
      test("decode error on non-Number") {
        val codec = codecFor[Float]
        assertTrue(codec.decoder(AttributeValue.String("1.5")).isLeft)
      }
    ),
    suite("Double")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[Double]
        assertTrue(codec.encoder(2.718) == AttributeValue.Number(BigDecimal.valueOf(2.718)))
      },
      test("round-trips")(assertTrue(roundTrip[Double](2.718) == Right(2.718))),
      test("decode error on non-Number") {
        val codec = codecFor[Double]
        assertTrue(codec.decoder(AttributeValue.Null).isLeft)
      }
    ),
    suite("Char")(
      test("encodes to single-character AttributeValue.String") {
        val codec = codecFor[Char]
        assertTrue(codec.encoder('Z') == AttributeValue.String("Z"))
      },
      test("round-trips")(assertTrue(roundTrip[Char]('A') == Right('A'))),
      test("decode error on multi-character String") {
        val codec = codecFor[Char]
        assertTrue(codec.decoder(AttributeValue.String("ab")).isLeft)
      },
      test("decode error on non-String") {
        val codec = codecFor[Char]
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(65))).isLeft)
      }
    ),
    suite("BigInt")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[BigInt]
        assertTrue(
          codec.encoder(BigInt("123456789012345678901234567890")) == AttributeValue.Number(
            BigDecimal(BigInt("123456789012345678901234567890"))
          )
        )
      },
      test("round-trips") {
        assertTrue(roundTrip[BigInt](BigInt("999999999999999999999")) == Right(BigInt("999999999999999999999")))
      },
      test("decode error on non-Number") {
        val codec = codecFor[BigInt]
        assertTrue(codec.decoder(AttributeValue.String("123")).isLeft)
      }
    ),
    suite("BigDecimal")(
      test("encodes to AttributeValue.Number") {
        val codec = codecFor[BigDecimal]
        assertTrue(
          codec.encoder(BigDecimal("3.14159265358979")) == AttributeValue.Number(BigDecimal("3.14159265358979"))
        )
      },
      test("round-trips") {
        assertTrue(roundTrip[BigDecimal](BigDecimal("1.23456789")) == Right(BigDecimal("1.23456789")))
      },
      test("decode error on non-Number") {
        val codec = codecFor[BigDecimal]
        assertTrue(codec.decoder(AttributeValue.Null).isLeft)
      }
    ),
    test("case class with all numeric primitives round-trips") {
      val v = AllPrimitives(
        b = true,
        by = 1.toByte,
        s = 2.toShort,
        i = 3,
        l = 4L,
        f = 1.5f,
        d = 2.5,
        c = 'X',
        bi = BigInt(99),
        bd = BigDecimal("1.23")
      )
      assertTrue(roundTrip(v) == Right(v))
    }
  )

  // ---- temporal types ----------------------------------------------------

  private val temporalSuite = suite("temporal primitive codecs")(
    test("Instant round-trips") {
      val v = Instant.parse("2024-01-15T10:30:00Z")
      assertTrue(roundTrip[Instant](v) == Right(v))
    },
    test("LocalDate round-trips") {
      val v = LocalDate.of(2024, 1, 15)
      assertTrue(roundTrip[LocalDate](v) == Right(v))
    },
    test("LocalDateTime round-trips") {
      val v = LocalDateTime.of(2024, 1, 15, 10, 30, 0)
      assertTrue(roundTrip[LocalDateTime](v) == Right(v))
    },
    test("LocalTime round-trips") {
      val v = LocalTime.of(10, 30, 0)
      assertTrue(roundTrip[LocalTime](v) == Right(v))
    },
    test("OffsetDateTime round-trips") {
      val v = OffsetDateTime.parse("2024-01-15T10:30:00+01:00")
      assertTrue(roundTrip[OffsetDateTime](v) == Right(v))
    },
    test("OffsetTime round-trips") {
      val v = OffsetTime.parse("10:30:00+01:00")
      assertTrue(roundTrip[OffsetTime](v) == Right(v))
    },
    test("ZonedDateTime round-trips") {
      val v = ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of("Europe/London"))
      assertTrue(roundTrip[ZonedDateTime](v) == Right(v))
    },
    test("ZoneId round-trips") {
      val v = ZoneId.of("America/New_York")
      assertTrue(roundTrip[ZoneId](v) == Right(v))
    },
    test("ZoneOffset round-trips") {
      val v = ZoneOffset.of("+05:30")
      assertTrue(roundTrip[ZoneOffset](v) == Right(v))
    },
    test("Duration round-trips") {
      val v = Duration.ofHours(2).plusMinutes(30)
      assertTrue(roundTrip[Duration](v) == Right(v))
    },
    test("Period round-trips") {
      val v = Period.of(1, 2, 3)
      assertTrue(roundTrip[Period](v) == Right(v))
    },
    test("Year encodes as AttributeValue.Number") {
      val codec = codecFor[Year]
      assertTrue(codec.encoder(Year.of(2024)) == AttributeValue.Number(BigDecimal.valueOf(2024L)))
    },
    test("Year round-trips") {
      val v = Year.of(2024)
      assertTrue(roundTrip[Year](v) == Right(v))
    },
    test("YearMonth round-trips") {
      val v = YearMonth.of(2024, 6)
      assertTrue(roundTrip[YearMonth](v) == Right(v))
    },
    test("DayOfWeek encodes as name string") {
      val codec = codecFor[DayOfWeek]
      assertTrue(codec.encoder(DayOfWeek.MONDAY) == AttributeValue.String("MONDAY"))
    },
    test("DayOfWeek round-trips") {
      val v = DayOfWeek.WEDNESDAY
      assertTrue(roundTrip[DayOfWeek](v) == Right(v))
    },
    test("Month encodes as name string") {
      val codec = codecFor[Month]
      assertTrue(codec.encoder(Month.JANUARY) == AttributeValue.String("JANUARY"))
    },
    test("Month round-trips") {
      val v = Month.JULY
      assertTrue(roundTrip[Month](v) == Right(v))
    },
    test("MonthDay round-trips") {
      val v = MonthDay.of(12, 25)
      assertTrue(roundTrip[MonthDay](v) == Right(v))
    },
    test("temporal decode error on wrong AttributeValue type") {
      val codec = codecFor[Instant]
      assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(1))).isLeft)
    },
    test("temporal decode error on unparseable string") {
      val codec = codecFor[Instant]
      assertTrue(codec.decoder(AttributeValue.String("not-an-instant")).isLeft)
    },
    test("case class with all temporal fields round-trips") {
      val v = WithTemporals(
        instant = Instant.parse("2024-06-01T12:00:00Z"),
        localDate = LocalDate.of(2024, 6, 1),
        localDateTime = LocalDateTime.of(2024, 6, 1, 12, 0),
        localTime = LocalTime.of(12, 0),
        offsetDateTime = OffsetDateTime.parse("2024-06-01T12:00:00+01:00"),
        offsetTime = OffsetTime.parse("12:00:00+01:00"),
        zonedDateTime = ZonedDateTime.of(2024, 6, 1, 12, 0, 0, 0, ZoneId.of("UTC")),
        zoneId = ZoneId.of("UTC"),
        zoneOffset = ZoneOffset.UTC,
        duration = Duration.ofMinutes(90),
        period = Period.ofDays(7),
        year = Year.of(2024),
        yearMonth = YearMonth.of(2024, 6),
        dayOfWeek = DayOfWeek.SATURDAY,
        month = Month.JUNE,
        monthDay = MonthDay.of(6, 1)
      )
      assertTrue(roundTrip(v) == Right(v))
    }
  )

  // ---- UUID and Currency -------------------------------------------------

  private val miscPrimitiveSuite = suite("UUID and Currency codecs")(
    test("UUID encodes to AttributeValue.String") {
      val codec = codecFor[UUID]
      val id    = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
      assertTrue(codec.encoder(id) == AttributeValue.String("550e8400-e29b-41d4-a716-446655440000"))
    },
    test("UUID round-trips") {
      val id = UUID.randomUUID()
      assertTrue(roundTrip[UUID](id) == Right(id))
    },
    test("UUID decode error on invalid string") {
      val codec = codecFor[UUID]
      assertTrue(codec.decoder(AttributeValue.String("not-a-uuid")).isLeft)
    },
    test("UUID decode error on non-String") {
      val codec = codecFor[UUID]
      assertTrue(codec.decoder(AttributeValue.Null).isLeft)
    },
    test("Currency encodes to currency code string") {
      val codec = codecFor[Currency]
      assertTrue(codec.encoder(Currency.getInstance("USD")) == AttributeValue.String("USD"))
    },
    test("Currency round-trips") {
      val v = Currency.getInstance("GBP")
      assertTrue(roundTrip[Currency](v) == Right(v))
    },
    test("Currency decode error on invalid code") {
      val codec = codecFor[Currency]
      assertTrue(codec.decoder(AttributeValue.String("ZZZ")).isLeft)
    },
    test("case class with UUID and Currency round-trips") {
      val id  = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
      val ccy = Currency.getInstance("EUR")
      assertTrue(roundTrip(WithUuidCurrency(id, ccy)) == Right(WithUuidCurrency(id, ccy)))
    }
  )

  // ---- records (case classes) --------------------------------------------

  private val recordSuite = suite("record (case class) codecs")(
    test("encodes to AttributeValue.Map with field names as keys") {
      val codec = codecFor[Person]
      assert(codec.encoder(Person("Alice", 30)))(
        isSubtype[AttributeValue.Map](
          hasField[AttributeValue.Map, Option[AttributeValue]](
            "value",
            _.value.get(AttributeValue.String("name")),
            isSome(equalTo(AttributeValue.String("Alice")))
          ) &&
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("age")),
              isSome(equalTo(AttributeValue.Number(BigDecimal.valueOf(30L))))
            )
        )
      )
    },
    test("round-trips a case class with String and Int fields") {
      assertTrue(roundTrip(Person("Bob", 25)) == Right(Person("Bob", 25)))
    },
    test("round-trips a case class with String and Long fields") {
      assertTrue(roundTrip(Event("evt-1", 9999999999L)) == Right(Event("evt-1", 9999999999L)))
    },
    test("decode error when Map attribute value is missing a field") {
      val codec         = codecFor[Person]
      // only 'name' present, 'age' missing
      val incompleteMap = AttributeValue.Map(
        Map(AttributeValue.String("name") -> AttributeValue.String("Alice"))
      )
      assertTrue(codec.decoder(incompleteMap).isLeft)
    },
    test("decode error when given a non-Map attribute value") {
      val codec = codecFor[Person]
      assertTrue(codec.decoder(AttributeValue.String("wrong")).isLeft)
    },
    test("Array[Byte] field encodes as AttributeValue.Binary inside the Map") {
      val codec = codecFor[BinaryPayload]
      val bytes = Array[Byte](1, 2, 3)
      assert(codec.encoder(BinaryPayload("e1", bytes)))(
        isSubtype[AttributeValue.Map](
          hasField(
            "value",
            _.value.get(AttributeValue.String("payload")),
            isSome(isSubtype[AttributeValue.Binary](hasField("value", _.value.toList, equalTo(List[Byte](1, 2, 3)))))
          )
        )
      )
    },
    test("case class with Array[Byte] field round-trips") {
      val codec  = codecFor[BinaryPayload]
      val record = BinaryPayload("e1", Array[Byte](10, 20, 30))
      assert(codec.decoder(codec.encoder(record)))(
        isRight(
          hasField("id", (_: BinaryPayload).id, equalTo(record.id)) &&
            hasField("payload", (_: BinaryPayload).payload.toList, equalTo(record.payload.toList))
        )
      )
    }
  )

  // ---- @transient fields --------------------------------------------------

  private val transientSuite = suite("@transient field codec")(
    test("transient field is omitted from the encoded AttributeValue.Map") {
      val codec = codecFor[WithTransient]
      assert(codec.encoder(WithTransient("a", 99, cached = 42)))(
        isSubtype[AttributeValue.Map](
          hasField(
            "value",
            _.value,
            hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("id")) &&
              hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("score")) &&
              !hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("cached"))
          )
        )
      )
    },
    test("encoded map contains exactly the non-transient fields") {
      val codec = codecFor[WithTransient]
      assert(codec.encoder(WithTransient("a", 99, cached = 42)))(
        isSubtype[AttributeValue.Map](hasField("value", _.value, hasSize(equalTo(2))))
      )
    },
    test("decoding succeeds when transient field is absent from the map") {
      val codec = codecFor[WithTransient]
      val av    = AttributeValue.Map(
        Map(
          AttributeValue.String("id")    -> AttributeValue.String("a"),
          AttributeValue.String("score") -> AttributeValue.Number(BigDecimal.valueOf(99L))
        )
      )
      assertTrue(codec.decoder(av).isRight)
    },
    test("transient field gets zero/default register value after decoding") {
      val codec = codecFor[WithTransient]
      val av    = AttributeValue.Map(
        Map(
          AttributeValue.String("id")    -> AttributeValue.String("a"),
          AttributeValue.String("score") -> AttributeValue.Number(BigDecimal.valueOf(99L))
        )
      )
      // cached is not in the map; register stays at 0 (Int register default)
      assertTrue(codec.decoder(av) == Right(WithTransient("a", 99, cached = 0)))
    },
    test("decode still fails when a non-transient field is absent") {
      val codec = codecFor[WithTransient]
      val av    = AttributeValue.Map(
        Map(
          AttributeValue.String("id") -> AttributeValue.String("a")
          // score missing — should still be a MissingField error
        )
      )
      assertTrue(codec.decoder(av).isLeft)
    },
    test("encode → decode drops the transient field value") {
      val codec    = codecFor[WithTransient]
      val original = WithTransient("a", 99, cached = 42)
      // cached = 42 is encoded but dropped; decoded value gets cached = 0
      assertTrue(codec.decoder(codec.encoder(original)) == Right(WithTransient("a", 99, cached = 0)))
    }
  )

  // ---- Option ------------------------------------------------------------

  private val optionSuite = suite("Option codec")(
    test("Some encodes as the inner value") {
      val codec = codecFor[Option[String]]
      assertTrue(codec.encoder(Some("hi")) == AttributeValue.String("hi"))
    },
    test("None encodes as AttributeValue.Null") {
      val codec = codecFor[Option[String]]
      assertTrue(codec.encoder(None) == AttributeValue.Null)
    },
    test("decodes AttributeValue.Null as None") {
      val codec = codecFor[Option[String]]
      assertTrue(codec.decoder(AttributeValue.Null) == Right(None))
    },
    test("decodes inner value as Some") {
      val codec = codecFor[Option[String]]
      assertTrue(codec.decoder(AttributeValue.String("hi")) == Right(Some("hi")))
    },
    test("transientNone: None field omitted from Map, decoded back as None") {
      val codec = codecFor[WithOption]
      val av    = codec.encoder(WithOption("x", None))
      assert(av)(
        isSubtype[AttributeValue.Map](hasField("value", _.value, !hasKey(AttributeValue.String("note"))))
      ) &&
      assertTrue(codec.decoder(av) == Right(WithOption("x", None)))
    },
    test("Some field present and round-trips") {
      assertTrue(roundTrip(WithOption("x", Some("memo"))) == Right(WithOption("x", Some("memo"))))
    }
  )

  // ---- enum (no-field sealed cases) --------------------------------------

  private val enumSuite = suite("enum (no-field cases) codec")(
    test("encodes each case as AttributeValue.String of its name") {
      val codec = codecFor[Color]
      assertTrue(
        codec.encoder(Color.Red) == AttributeValue.String("Red") &&
          codec.encoder(Color.Green) == AttributeValue.String("Green") &&
          codec.encoder(Color.Blue) == AttributeValue.String("Blue")
      )
    },
    test("decodes AttributeValue.String back to the matching case") {
      val codec = codecFor[Color]
      assertTrue(
        codec.decoder(AttributeValue.String("Red")) == Right(Color.Red) &&
          codec.decoder(AttributeValue.String("Green")) == Right(Color.Green) &&
          codec.decoder(AttributeValue.String("Blue")) == Right(Color.Blue)
      )
    },
    test("decode error for unknown case name carries message with the offending value") {
      val codec  = codecFor[Color]
      val result = codec.decoder(AttributeValue.String("Purple"))
      assertTrue(
        result.isLeft &&
          result.left.toOption.exists(_.message.contains("unknown enum value 'Purple'"))
      )
    },
    test("decode error when attribute is not a String") {
      val codec  = codecFor[Color]
      val result = codec.decoder(AttributeValue.Number(BigDecimal(1)))
      assertTrue(
        result.isLeft &&
          result.left.toOption.exists(_.message.contains("expected String for enum"))
      )
    },
    test("round-trips all enum cases") {
      val codec = codecFor[Color]
      val cases = List(Color.Red, Color.Green, Color.Blue)
      assertTrue(cases.forall(c => codec.decoder(codec.encoder(c)) == Right(c)))
    }
  )

  // ---- ADT with fields (DiscriminatorKind.Key default) -------------------

  private val adtSuite = suite("ADT (DiscriminatorKind.Key) codec")(
    test("encodes Circle as singleton Map with case name as key") {
      val codec = codecFor[Shape]
      assert(codec.encoder(Shape.Circle(5)))(
        isSubtype[AttributeValue.Map](
          hasField("value", _.value.keys.map(_.value).toList, equalTo(List("Circle")))
        )
      )
    },
    test("round-trips Circle") {
      assertTrue(roundTrip[Shape](Shape.Circle(5)) == Right(Shape.Circle(5)))
    },
    test("round-trips Rect") {
      assertTrue(roundTrip[Shape](Shape.Rect(10, 20)) == Right(Shape.Rect(10, 20)))
    },
    test("decode error for unknown case key") {
      val codec = codecFor[Shape]
      val badAv = AttributeValue.Map(
        Map(AttributeValue.String("Triangle") -> AttributeValue.Map(Map.empty))
      )
      assertTrue(codec.decoder(badAv).isLeft)
    },
    test("decode error for empty Map") {
      val codec = codecFor[Shape]
      assertTrue(codec.decoder(AttributeValue.Map(Map.empty)).isLeft)
    }
  )

  // ---- ADT with DiscriminatorKind.None (decode by trying each case) -------
  //
  // No discriminator key/field is written; decoding tries each case's codec
  // in turn and takes the first one that succeeds. Scala 3 union types are a
  // more common use of this mode (see DynamoDBCodecDeriverVersionSpecificSpec),
  // but the underlying derivation is generic over any variant, so a plain
  // sealed trait exercises the same code path cross-version.

  private val noneDiscriminatorDeriver = DynamoDBCodecDeriver.withDiscriminatorKind(DiscriminatorKind.None)

  private def noneDiscriminatorCodecFor[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(noneDiscriminatorDeriver).derive

  private val adtNoneDiscriminatorSuite = suite("ADT (DiscriminatorKind.None) codec")(
    test("round-trips Circle without writing a discriminator") {
      val codec        = noneDiscriminatorCodecFor[Shape]
      val value: Shape = Shape.Circle(5)
      assertTrue(codec.decoder(codec.encoder(value)) == Right(value))
    },
    test("round-trips Rect without writing a discriminator") {
      val codec        = noneDiscriminatorCodecFor[Shape]
      val value: Shape = Shape.Rect(10, 20)
      assertTrue(codec.decoder(codec.encoder(value)) == Right(value))
    },
    test("encoded Circle carries no case-name key, unlike DiscriminatorKind.Key") {
      val codec = noneDiscriminatorCodecFor[Shape]
      assert(codec.encoder(Shape.Circle(5)))(
        isSubtype[AttributeValue.Map](hasField("value", _.value, !hasKey(AttributeValue.String("Circle"))))
      )
    },
    test("decode error when no case codec matches") {
      val codec = noneDiscriminatorCodecFor[Shape]
      assertTrue(codec.decoder(AttributeValue.String("nope")).isLeft)
    }
  )

  // ---- @Modifier.discriminator -------------------------------------------

  private val adtDiscriminatorFieldSuite =
    suite("ADT with @Modifier.discriminator (Field encoding)")(
      test("encodes Circle as flat Map with _type field") {
        val codec = codecFor[TaggedShape]
        assert(codec.encoder(TaggedShape.Circle(5)))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value,
              hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("_type")) &&
                hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("radius"))
            )
          )
        )
      },
      test("encodes _type value as case name string") {
        val codec = codecFor[TaggedShape]
        assert(codec.encoder(TaggedShape.Circle(5)))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("_type")),
              isSome(equalTo(AttributeValue.String("Circle")))
            )
          )
        )
      },
      test("does NOT wrap fields under a case-name key") {
        val codec = codecFor[TaggedShape]
        assert(codec.encoder(TaggedShape.Circle(5)))(
          isSubtype[AttributeValue.Map](hasField("value", _.value, !hasKey(AttributeValue.String("Circle"))))
        )
      },
      test("round-trips Circle") {
        assertTrue(roundTrip[TaggedShape](TaggedShape.Circle(5)) == Right(TaggedShape.Circle(5)))
      },
      test("round-trips Rect") {
        assertTrue(
          roundTrip[TaggedShape](TaggedShape.Rect(10, 20)) == Right(TaggedShape.Rect(10, 20))
        )
      },
      test("decode error when _type field is absent") {
        val codec = codecFor[TaggedShape]
        val av    = AttributeValue.Map(Map(AttributeValue.String("radius") -> AttributeValue.Number(5)))
        assertTrue(codec.decoder(av).isLeft)
      },
      test("decode error when _type value is unknown") {
        val codec = codecFor[TaggedShape]
        val av    = AttributeValue.Map(
          Map(
            AttributeValue.String("_type") -> AttributeValue.String("Triangle"),
            AttributeValue.String("sides") -> AttributeValue.Number(3)
          )
        )
        assertTrue(codec.decoder(av).isLeft)
      },
      test("deriver-level Key encoding unaffected (Shape still uses Key encoding)") {
        val codec = codecFor[Shape]
        assert(codec.encoder(Shape.Circle(5)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("Circle")))
          )
        )
      }
    )

  // ---- @Modifier.caseNaming on ADT ---------------------------------------

  private val adtCaseNamingSuite =
    suite("ADT with @Modifier.caseNaming (snake_case)")(
      test("encodes CircleShape under snake_case key 'circle_shape'") {
        val codec = codecFor[SnakeShape]
        assert(codec.encoder(SnakeShape.CircleShape(7)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("circle_shape")))
          )
        )
      },
      test("encodes RectShape under snake_case key 'rect_shape'") {
        val codec = codecFor[SnakeShape]
        assert(codec.encoder(SnakeShape.RectShape(3, 4)))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("rect_shape")))
          )
        )
      },
      test("round-trips CircleShape") {
        assertTrue(
          roundTrip[SnakeShape](SnakeShape.CircleShape(7)) == Right(SnakeShape.CircleShape(7))
        )
      },
      test("round-trips RectShape") {
        assertTrue(
          roundTrip[SnakeShape](SnakeShape.RectShape(3, 4)) == Right(SnakeShape.RectShape(3, 4))
        )
      },
      test("decode error when original PascalCase key is used") {
        val codec = codecFor[SnakeShape]
        val av    = AttributeValue.Map(
          Map(
            AttributeValue.String("CircleShape") -> AttributeValue.Map(
              Map(AttributeValue.String("radius") -> AttributeValue.Number(7))
            )
          )
        )
        assertTrue(codec.decoder(av).isLeft)
      }
    )

  // ---- @Modifier.discriminator + @Modifier.caseNaming together -----------

  private val adtDiscriminatorAndCaseNamingSuite =
    suite("ADT with @Modifier.discriminator and @Modifier.caseNaming combined")(
      test("encodes CircleShape as flat map with 'kind' = 'circle_shape'") {
        val codec = codecFor[TaggedSnakeShape]
        assert(codec.encoder(TaggedSnakeShape.CircleShape(3)))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("kind")),
              isSome(equalTo(AttributeValue.String("circle_shape")))
            )
          )
        )
      },
      test("flat map has 'kind' key but not a PascalCase or snake_case case-name key") {
        val codec = codecFor[TaggedSnakeShape]
        assert(codec.encoder(TaggedSnakeShape.CircleShape(3)))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.keys.map(_.value).toSet,
              contains("kind") && !contains("CircleShape") && !contains("circle_shape")
            )
          )
        )
      },
      test("round-trips CircleShape") {
        assertTrue(
          roundTrip[TaggedSnakeShape](TaggedSnakeShape.CircleShape(3)) == Right(
            TaggedSnakeShape.CircleShape(3)
          )
        )
      },
      test("round-trips RectShape") {
        assertTrue(
          roundTrip[TaggedSnakeShape](TaggedSnakeShape.RectShape(5, 8)) == Right(
            TaggedSnakeShape.RectShape(5, 8)
          )
        )
      }
    )

  // ---- @Modifier.caseNaming on enum --------------------------------------

  private val enumCaseNamingSuite =
    suite("enum with @Modifier.caseNaming (snake_case)")(
      test("encodes DarkRed as AttributeValue.String('dark_red')") {
        val codec = codecFor[SnakeColor]
        assertTrue(codec.encoder(SnakeColor.DarkRed) == AttributeValue.String("dark_red"))
      },
      test("encodes LightBlue as AttributeValue.String('light_blue')") {
        val codec = codecFor[SnakeColor]
        assertTrue(codec.encoder(SnakeColor.LightBlue) == AttributeValue.String("light_blue"))
      },
      test("round-trips DarkRed") {
        assertTrue(roundTrip[SnakeColor](SnakeColor.DarkRed) == Right(SnakeColor.DarkRed))
      },
      test("round-trips LightBlue") {
        assertTrue(roundTrip[SnakeColor](SnakeColor.LightBlue) == Right(SnakeColor.LightBlue))
      },
      test("decode error when original PascalCase name is used") {
        val codec = codecFor[SnakeColor]
        assertTrue(codec.decoder(AttributeValue.String("DarkRed")).isLeft)
      }
    )

  // ---- @Modifier.encodeTransient -----------------------------------------

  private val encodeTransientSuite =
    suite("@Modifier.encodeTransient field codec")(
      test("encode-transient field is absent from encoded Map") {
        val codec = codecFor[WithEncodeTransient]
        assert(codec.encoder(WithEncodeTransient("x", score = 42)))(
          isSubtype[AttributeValue.Map](hasField("value", _.value, !hasKey(AttributeValue.String("score"))))
        )
      },
      test("id field still encoded") {
        val codec = codecFor[WithEncodeTransient]
        assert(codec.encoder(WithEncodeTransient("x", score = 42)))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("id")),
              isSome(equalTo(AttributeValue.String("x")))
            )
          )
        )
      },
      test("decode with score present round-trips") {
        val codec  = codecFor[WithEncodeTransient]
        val stored = AttributeValue.Map(
          Map(
            AttributeValue.String("id")    -> AttributeValue.String("x"),
            AttributeValue.String("score") -> AttributeValue.Number(42)
          )
        )
        assertTrue(codec.decoder(stored) == Right(WithEncodeTransient("x", score = 42)))
      },
      test("decode with score absent falls back to schema default") {
        val codec  = codecFor[WithEncodeTransient]
        val stored = AttributeValue.Map(Map(AttributeValue.String("id") -> AttributeValue.String("x")))
        assertTrue(codec.decoder(stored) == Right(WithEncodeTransient("x", score = 0)))
      }
    )

  // ---- transientDefaultValue ---------------------------------------------

  private val transientDefaultValueSuite =
    suite("transientDefaultValue: fields at their default value are omitted")(
      test("count at default (0) is omitted from encoded Map") {
        val deriver = DynamoDBCodecDeriver.withTransientDefaultValue(transientDefaultValue = true)
        val codec   = WithDefault.schema.deriving(deriver).derive
        assert(codec.encoder(WithDefault("a", count = 0, tag = "custom")))(
          isSubtype[AttributeValue.Map](hasField("value", _.value, !hasKey(AttributeValue.String("count"))))
        )
      },
      test("tag at default ('untagged') is omitted from encoded Map") {
        val deriver = DynamoDBCodecDeriver.withTransientDefaultValue(transientDefaultValue = true)
        val codec   = WithDefault.schema.deriving(deriver).derive
        assert(codec.encoder(WithDefault("a", count = 5, tag = "untagged")))(
          isSubtype[AttributeValue.Map](hasField("value", _.value, !hasKey(AttributeValue.String("tag"))))
        )
      },
      test("count NOT at default is still written") {
        val deriver = DynamoDBCodecDeriver.withTransientDefaultValue(transientDefaultValue = true)
        val codec   = WithDefault.schema.deriving(deriver).derive
        assert(codec.encoder(WithDefault("a", count = 7, tag = "untagged")))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("count")),
              isSome(equalTo(AttributeValue.Number(7)))
            )
          )
        )
      },
      test("default false: all fields written even at default") {
        val codec = codecFor[WithDefault] // transientDefaultValue = false by default
        assert(codec.encoder(WithDefault("a")))(
          isSubtype[AttributeValue.Map](
            hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("id", "count", "tag")))
          )
        )
      }
    )

  // ---- requireDefaultValueFields / decoder uses default for missing field -

  private val defaultValueDecodeSuite =
    suite("missing field with schema default uses default value on decode")(
      test("missing rank uses schema default (99)") {
        val codec  = codecFor[WithDefaultField]
        val stored = AttributeValue.Map(Map(AttributeValue.String("name") -> AttributeValue.String("Alice")))
        assertTrue(codec.decoder(stored) == Right(WithDefaultField("Alice", rank = 99)))
      },
      test("requireDefaultValueFields true: missing rank is an error") {
        val deriver = DynamoDBCodecDeriver.withRequireDefaultValueFields(requireDefaultValueFields = true)
        val codec   = WithDefaultField.schema.deriving(deriver).derive
        val stored  = AttributeValue.Map(Map(AttributeValue.String("name") -> AttributeValue.String("Alice")))
        assertTrue(codec.decoder(stored).isLeft)
      },
      test("rank present overrides default") {
        val codec  = codecFor[WithDefaultField]
        val stored = AttributeValue.Map(
          Map(
            AttributeValue.String("name") -> AttributeValue.String("Alice"),
            AttributeValue.String("rank") -> AttributeValue.Number(1)
          )
        )
        assertTrue(codec.decoder(stored) == Right(WithDefaultField("Alice", rank = 1)))
      }
    )

  // ---- default-value handling across every primitive register type --------
  //
  // The suites above only exercise Int/String defaults. FieldInfo dispatches
  // on register type for both defaultEqualsValue (encode-side omission check)
  // and setMissingValueOrDefault (decode-side fallback) with a case per
  // primitive type, so cover Long/Boolean/Byte/Char/Short/Float/Double too.

  private val primitiveDefaultsSuite = suite("default-value handling for every primitive type")(
    test("all primitive-default fields are omitted when equal to their defaults") {
      val deriver = DynamoDBCodecDeriver.withTransientDefaultValue(transientDefaultValue = true)
      val codec   = WithPrimitiveDefaults.schema.deriving(deriver).derive
      assert(codec.encoder(WithPrimitiveDefaults("a")))(
        isSubtype[AttributeValue.Map](hasField("value", _.value.keys.map(_.value).toSet, equalTo(Set("id"))))
      )
    },
    test("all primitive-default fields are written when not at their defaults") {
      val deriver = DynamoDBCodecDeriver.withTransientDefaultValue(transientDefaultValue = true)
      val codec   = WithPrimitiveDefaults.schema.deriving(deriver).derive
      val value   = WithPrimitiveDefaults("a", 200L, false, 2, 'y', 20, 9.5f, 8.5)
      assert(codec.encoder(value))(
        isSubtype[AttributeValue.Map](
          hasField(
            "value",
            _.value.keys.map(_.value).toSet,
            equalTo(Set("id", "longVal", "boolVal", "byteVal", "charVal", "shortVal", "floatVal", "doubleVal"))
          )
        )
      )
    },
    test("decoding an item missing every primitive-default field uses each schema default") {
      val codec  = codecFor[WithPrimitiveDefaults]
      val stored = AttributeValue.Map(Map(AttributeValue.String("id") -> AttributeValue.String("a")))
      assertTrue(codec.decoder(stored) == Right(WithPrimitiveDefaults("a")))
    }
  )

  // ---- emptyCollectionConstructor ----------------------------------------

  private val collectionsCodec = codecFor[WithCollections]

  private val emptyCollectionSuite =
    suite("absent collection field decoded as empty collection (requireCollectionFields = false)")(
      test("missing tags decoded as empty List") {
        val stored = AttributeValue.Map(
          Map(
            AttributeValue.String("id")     -> AttributeValue.String("x"),
            AttributeValue.String("scores") -> AttributeValue.Map(Map.empty)
          )
        )
        assertTrue(collectionsCodec.decoder(stored) == Right(WithCollections("x", tags = Nil, scores = Map.empty)))
      },
      test("missing scores decoded as empty Map") {
        val stored = AttributeValue.Map(
          Map(
            AttributeValue.String("id")   -> AttributeValue.String("x"),
            AttributeValue.String("tags") -> AttributeValue.List(Nil)
          )
        )
        assertTrue(collectionsCodec.decoder(stored) == Right(WithCollections("x", tags = Nil, scores = Map.empty)))
      },
      test("requireCollectionFields = true: missing collection is an error") {
        val deriver = DynamoDBCodecDeriver.withRequiredCollectionFields(requireCollectionFields = true)
        val codec   = WithCollections.schema.deriving(deriver).derive
        val stored  = AttributeValue.Map(
          Map(AttributeValue.String("id") -> AttributeValue.String("x"))
        )
        assertTrue(codec.decoder(stored).isLeft)
      }
    )

  // ---- sequences ---------------------------------------------------------

  private val sequenceSuite = suite("sequence codec")(
    test("List[String] encodes to AttributeValue.List") {
      val codec = codecFor[List[String]]
      assert(codec.encoder(List("a", "b", "c")))(
        isSubtype[AttributeValue.List](
          hasField[AttributeValue.List, List[AttributeValue]](
            "value",
            _.value.toList,
            equalTo(List(AttributeValue.String("a"), AttributeValue.String("b"), AttributeValue.String("c")))
          )
        )
      )
    },
    test("List[Int] round-trips") {
      assertTrue(roundTrip(List(1, 2, 3)) == Right(List(1, 2, 3)))
    },
    test("empty List round-trips") {
      assertTrue(roundTrip(List.empty[String]) == Right(List.empty[String]))
    },
    test("List[String] round-trips") {
      assertTrue(roundTrip(List("x", "y")) == Right(List("x", "y")))
    },
    test("decode error for non-List attribute value") {
      val codec = codecFor[List[String]]
      assertTrue(codec.decoder(AttributeValue.String("not a list")).isLeft)
    },
    test("Chunk[Byte] encodes to AttributeValue.Binary") {
      val codec = codecFor[Chunk[Byte]]
      val bytes = Chunk[Byte](1, 2, 3, 4)
      assert(codec.encoder(bytes))(
        isSubtype[AttributeValue.Binary](hasField("value", _.value.toList, equalTo(List[Byte](1, 2, 3, 4))))
      )
    },
    test("Chunk[Byte] decodes from AttributeValue.Binary") {
      val codec    = codecFor[Chunk[Byte]]
      val expected = Chunk[Byte](1, 2, 3, 4)
      assertTrue(codec.decoder(AttributeValue.Binary(List[Byte](1, 2, 3, 4))) == Right(expected))
    },
    test("Chunk[Byte] decode error for non-Binary attribute value") {
      val codec = codecFor[Chunk[Byte]]
      assertTrue(codec.decoder(AttributeValue.List(Seq(AttributeValue.Number(BigDecimal(1))))).isLeft)
    },
    test("List[Byte] encodes to AttributeValue.Binary") {
      val codec = codecFor[List[Byte]]
      val bytes = List[Byte](10, 20, 30)
      assert(codec.encoder(bytes))(isSubtype[AttributeValue.Binary](hasField("value", _.value.toList, equalTo(bytes))))
    },
    test("List[Byte] round-trips through AttributeValue.Binary") {
      val codec = codecFor[List[Byte]]
      val bytes = List[Byte](10, 20, 30)
      assertTrue(codec.decoder(codec.encoder(bytes)) == Right(bytes))
    },
    test("Vector[Byte] round-trips through AttributeValue.Binary") {
      val codec = codecFor[Vector[Byte]]
      val bytes = Vector[Byte](5, 6, 7, 8)
      assertTrue(codec.decoder(codec.encoder(bytes)) == Right(bytes))
    },
    test("empty Chunk[Byte] encodes to empty AttributeValue.Binary") {
      val codec = codecFor[Chunk[Byte]]
      assert(codec.encoder(Chunk.empty[Byte]))(
        isSubtype[AttributeValue.Binary](hasField("value", _.value.isEmpty, isTrue))
      )
    },
    test("Array[Byte] encodes to AttributeValue.Binary") {
      val codec = Schema.derived[Array[Byte]].deriving(DynamoDBCodecDeriver).derive
      val bytes = Array[Byte](1, 2, 3, 4)
      assert(codec.encoder(bytes))(
        isSubtype[AttributeValue.Binary](hasField("value", _.value.toList, equalTo(List[Byte](1, 2, 3, 4))))
      )
    },
    test("Array[Byte] decodes from AttributeValue.Binary") {
      val codec  = Schema.derived[Array[Byte]].deriving(DynamoDBCodecDeriver).derive
      val result = codec.decoder(AttributeValue.Binary(List[Byte](1, 2, 3, 4)))
      assertTrue(result.map(_.toList) == Right(List[Byte](1, 2, 3, 4)))
    },
    test("Array[Byte] round-trips through AttributeValue.Binary") {
      val codec = Schema.derived[Array[Byte]].deriving(DynamoDBCodecDeriver).derive
      val bytes = Array[Byte](10, 20, 30)
      assertTrue(codec.decoder(codec.encoder(bytes)).map(_.toList) == Right(bytes.toList))
    }
  )

  // ---- native DynamoDB set types -----------------------------------------

  private val nativeSetSuite = suite("native Set codec")(
    suite("StringSet")(
      test("Set[String] encodes to AttributeValue.StringSet") {
        val codec = codecFor[Set[String]]
        assert(codec.encoder(Set("a", "b", "c")))(
          isSubtype[AttributeValue.StringSet](hasField("value", _.value, equalTo(Set("a", "b", "c"))))
        )
      },
      test("Set[String] round-trips") {
        assertTrue(roundTrip(Set("x", "y", "z")) == Right(Set("x", "y", "z")))
      },
      test("empty Set[String] round-trips") {
        assertTrue(roundTrip(Set.empty[String]) == Right(Set.empty[String]))
      },
      test("decode error for non-StringSet value") {
        val codec = codecFor[Set[String]]
        assertTrue(codec.decoder(AttributeValue.String("not a set")).isLeft)
      }
    ),
    suite("NumberSet")(
      test("Set[Int] encodes to AttributeValue.NumberSet") {
        val codec = codecFor[Set[Int]]
        assert(codec.encoder(Set(1, 2, 3)))(
          isSubtype[AttributeValue.NumberSet](
            hasField("value", _.value, equalTo(Set(BigDecimal(1), BigDecimal(2), BigDecimal(3))))
          )
        )
      },
      test("Set[Int] round-trips") {
        assertTrue(roundTrip(Set(10, 20, 30)) == Right(Set(10, 20, 30)))
      },
      test("Set[Long] round-trips") {
        assertTrue(roundTrip(Set(1L, 2L, 3L)) == Right(Set(1L, 2L, 3L)))
      },
      test("Set[Double] round-trips") {
        assertTrue(roundTrip(Set(1.5, 2.5)) == Right(Set(1.5, 2.5)))
      },
      test("Set[BigDecimal] round-trips") {
        val input = Set(BigDecimal("1.23"), BigDecimal("4.56"))
        assertTrue(roundTrip(input) == Right(input))
      },
      test("Set[BigInt] round-trips") {
        val input = Set(BigInt(100), BigInt(200))
        assertTrue(roundTrip(input) == Right(input))
      },
      test("empty Set[Int] round-trips") {
        assertTrue(roundTrip(Set.empty[Int]) == Right(Set.empty[Int]))
      },
      test("decode error for non-NumberSet value") {
        val codec = codecFor[Set[Int]]
        assertTrue(codec.decoder(AttributeValue.String("not a set")).isLeft)
      }
    ),
    suite("BinarySet")(
      test("Set[Chunk[Byte]] encodes to AttributeValue.BinarySet") {
        val codec = codecFor[Set[Chunk[Byte]]]
        val input = Set(Chunk[Byte](1, 2, 3), Chunk[Byte](4, 5, 6))
        assert(codec.encoder(input))(
          isSubtype[AttributeValue.BinarySet](
            hasField(
              "value",
              _.value.map(_.toList).toSet,
              equalTo(Set(List[Byte](1, 2, 3), List[Byte](4, 5, 6)))
            )
          )
        )
      },
      test("Set[Chunk[Byte]] round-trips") {
        val codec = codecFor[Set[Chunk[Byte]]]
        val input = Set(Chunk[Byte](10, 20), Chunk[Byte](30, 40))
        assertTrue(codec.decoder(codec.encoder(input)) == Right(input))
      },
      test("empty Set[Chunk[Byte]] round-trips") {
        val codec = codecFor[Set[Chunk[Byte]]]
        val input = Set.empty[Chunk[Byte]]
        assertTrue(codec.decoder(codec.encoder(input)) == Right(input))
      },
      test("decode error for non-BinarySet value") {
        val codec = codecFor[Set[Chunk[Byte]]]
        assertTrue(codec.decoder(AttributeValue.String("not a set")).isLeft)
      },
      test("encoding fails loudly rather than silently dropping elements under ReadBothWriteOld") {
        // withSchema1ByteSequenceCompatibility(ReadBothWriteOld) makes the element codec encode
        // byte-sequences as AttributeValue.List, not Binary — DynamoDB's BinarySet can't represent
        // that, so this must fail rather than silently produce an incomplete/empty BinarySet.
        val codec = compatCodecForOld[Set[Chunk[Byte]]]
        val input = Set(Chunk[Byte](1, 2, 3), Chunk[Byte](4, 5, 6))
        assertTrue(scala.util.Try(codec.encoder(input)).isFailure)
      }
    )
  )

  // ---- maps --------------------------------------------------------------

  private val mapSuite = suite("map codec")(
    test("Map[String, Int] (native) encodes to AttributeValue.Map") {
      val codec = codecFor[Map[String, Int]]
      assert(codec.encoder(Map("a" -> 1, "b" -> 2)))(
        isSubtype[AttributeValue.Map](
          hasField[AttributeValue.Map, Option[AttributeValue]](
            "value",
            _.value.get(AttributeValue.String("a")),
            isSome(equalTo(AttributeValue.Number(BigDecimal.valueOf(1L))))
          ) &&
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("b")),
              isSome(equalTo(AttributeValue.Number(BigDecimal.valueOf(2L))))
            )
        )
      )
    },
    test("Map[String, Int] round-trips") {
      assertTrue(roundTrip(Map("x" -> 10, "y" -> 20)) == Right(Map("x" -> 10, "y" -> 20)))
    },
    test("empty Map[String, String] round-trips") {
      assertTrue(roundTrip(Map.empty[String, String]) == Right(Map.empty[String, String]))
    },
    test("Map[String, String] round-trips") {
      assertTrue(roundTrip(Map("k" -> "v")) == Right(Map("k" -> "v")))
    },
    test("decode error for non-Map attribute value for Map[String, Int]") {
      val codec = codecFor[Map[String, Int]]
      assertTrue(codec.decoder(AttributeValue.String("nope")).isLeft)
    }
  )

  // ---- non-native map codec (non-String keys, encoded as List of [k, v]) --
  //
  // isNativeMap requires the key type to be a primitive String; any other key
  // type (Int here) falls back to a Sequence-of-tuple2 encoding instead of a
  // native AttributeValue.Map.

  private val nonNativeMapSuite = suite("map codec (non-String key, Sequence-of-tuple2 encoding)")(
    test("Map[Int, String] encodes as AttributeValue.List of 2-element [key, value] lists") {
      val codec = codecFor[Map[Int, String]]
      assert(codec.encoder(Map(1 -> "a")))(
        isSubtype[AttributeValue.List](
          hasField[AttributeValue.List, List[AttributeValue]](
            "value",
            _.value.toList,
            hasSize[AttributeValue](equalTo(1)) &&
              hasAt(0)(
                isSubtype[AttributeValue.List](
                  hasField[AttributeValue.List, List[AttributeValue]](
                    "value",
                    _.value.toList,
                    equalTo(List(AttributeValue.Number(BigDecimal(1)), AttributeValue.String("a")))
                  )
                )
              )
          )
        )
      )
    },
    test("Map[Int, String] round-trips") {
      assertTrue(roundTrip(Map(1 -> "a", 2 -> "b")) == Right(Map(1 -> "a", 2 -> "b")))
    },
    test("empty Map[Int, String] round-trips") {
      assertTrue(roundTrip(Map.empty[Int, String]) == Right(Map.empty[Int, String]))
    },
    test("decode error for non-List attribute value") {
      val codec = codecFor[Map[Int, String]]
      assertTrue(codec.decoder(AttributeValue.String("nope")).isLeft)
    },
    test("decode error for a malformed entry (not a 2-element list)") {
      val codec     = codecFor[Map[Int, String]]
      val malformed = AttributeValue.List(
        List(AttributeValue.List(List(AttributeValue.Number(BigDecimal(1)))))
      )
      assertTrue(codec.decoder(malformed).isLeft)
    },
    test("decode error when the key can't be decoded") {
      val codec     = codecFor[Map[Int, String]]
      val malformed = AttributeValue.List(
        List(AttributeValue.List(List(AttributeValue.String("not-a-number"), AttributeValue.String("a"))))
      )
      assertTrue(codec.decoder(malformed).isLeft)
    }
  )

  // ---- DynamicValue codec ------------------------------------------------

  private val dvCodec = DynamoDBCodecDeriver.dynamicValueCodec

  private val dynamicValueSuite = suite("dynamicValueCodec")(
    suite("encoder")(
      test("String primitive → AttributeValue.String") {
        assertTrue(dvCodec.encoder(DynamicValue.string("hi")) == AttributeValue.String("hi"))
      },
      test("Int primitive → AttributeValue.Number") {
        assertTrue(dvCodec.encoder(DynamicValue.int(42)) == AttributeValue.Number(BigDecimal.valueOf(42L)))
      },
      test("Long primitive → AttributeValue.Number") {
        assertTrue(
          dvCodec.encoder(DynamicValue.long(Long.MaxValue)) == AttributeValue.Number(BigDecimal.valueOf(Long.MaxValue))
        )
      },
      test("BigDecimal primitive → AttributeValue.Number") {
        val bd = BigDecimal("3.14")
        assertTrue(dvCodec.encoder(DynamicValue.bigDecimal(bd)) == AttributeValue.Number(bd))
      },
      test("Null → AttributeValue.Null") {
        assertTrue(dvCodec.encoder(DynamicValue.Null) == AttributeValue.Null)
      },
      test("Sequence → AttributeValue.List") {
        val seq = new DynamicValue.Sequence(Chunk(DynamicValue.string("a"), DynamicValue.int(1)))
        assert(dvCodec.encoder(seq))(isSubtype[AttributeValue.List](hasField("value", _.value, hasSize(equalTo(2)))))
      },
      test("Record → AttributeValue.Map") {
        val rec = new DynamicValue.Record(Chunk("x" -> DynamicValue.int(1)))
        assert(dvCodec.encoder(rec))(isSubtype[AttributeValue.Map](hasField("value", _.value.nonEmpty, isTrue)))
      },
      test("Unit primitive → empty AttributeValue.Map") {
        assertTrue(dvCodec.encoder(DynamicValue.unit) == AttributeValue.Map.empty)
      },
      test("Boolean primitive → AttributeValue.Bool") {
        assertTrue(
          dvCodec.encoder(DynamicValue.boolean(true)) == AttributeValue.Bool(true) &&
            dvCodec.encoder(DynamicValue.boolean(false)) == AttributeValue.Bool(false)
        )
      },
      test("Byte primitive → AttributeValue.Number") {
        assertTrue(dvCodec.encoder(DynamicValue.byte(42.toByte)) == AttributeValue.Number(BigDecimal.valueOf(42L)))
      },
      test("Short primitive → AttributeValue.Number") {
        assertTrue(dvCodec.encoder(DynamicValue.short(100.toShort)) == AttributeValue.Number(BigDecimal.valueOf(100L)))
      },
      test("Float primitive → AttributeValue.Number") {
        assertTrue(
          dvCodec.encoder(DynamicValue.float(1.5f)) == AttributeValue.Number(BigDecimal.valueOf(1.5f.toDouble))
        )
      },
      test("Double primitive → AttributeValue.Number") {
        assertTrue(dvCodec.encoder(DynamicValue.double(3.14)) == AttributeValue.Number(BigDecimal.valueOf(3.14)))
      },
      test("Char primitive → AttributeValue.String") {
        assertTrue(dvCodec.encoder(DynamicValue.char('Z')) == AttributeValue.String("Z"))
      },
      test("BigInt primitive → AttributeValue.Number") {
        val bi = BigInt("123456789012345678901234567890")
        assertTrue(dvCodec.encoder(DynamicValue.bigInt(bi)) == AttributeValue.Number(BigDecimal(bi)))
      },
      test("UUID primitive → AttributeValue.String") {
        val id = java.util.UUID.fromString("00000000-0000-0000-0000-000000000000")
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.UUID(id))) ==
            AttributeValue.String("00000000-0000-0000-0000-000000000000")
        )
      },
      test("Currency primitive → AttributeValue.String of currency code") {
        val usd = java.util.Currency.getInstance("USD")
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.Currency(usd))) == AttributeValue.String("USD")
        )
      },
      test("DayOfWeek primitive → AttributeValue.String of name") {
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.DayOfWeek(java.time.DayOfWeek.MONDAY))) ==
            AttributeValue.String("MONDAY")
        )
      },
      test("Month primitive → AttributeValue.String of name") {
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.Month(java.time.Month.JANUARY))) ==
            AttributeValue.String("JANUARY")
        )
      },
      test("Year primitive → AttributeValue.Number") {
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.Year(java.time.Year.of(2024)))) ==
            AttributeValue.Number(BigDecimal.valueOf(2024L))
        )
      },
      test("Instant primitive → AttributeValue.String") {
        val t = java.time.Instant.parse("2024-01-15T10:30:00Z")
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.Instant(t))) ==
            AttributeValue.String("2024-01-15T10:30:00Z")
        )
      },
      test("ZoneId primitive → AttributeValue.String of zone id") {
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.ZoneId(java.time.ZoneId.of("Europe/London")))) ==
            AttributeValue.String("Europe/London")
        )
      },
      test("ZoneOffset primitive → AttributeValue.String of offset id") {
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.ZoneOffset(java.time.ZoneOffset.UTC))) ==
            AttributeValue.String("Z")
        )
      },
      test("YearMonth primitive → AttributeValue.String") {
        val t = java.time.YearMonth.of(2024, 3)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.YearMonth(t))) == AttributeValue.String(t.toString)
        )
      },
      test("MonthDay primitive → AttributeValue.String") {
        val t = java.time.MonthDay.of(3, 15)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.MonthDay(t))) == AttributeValue.String(t.toString)
        )
      },
      test("Duration primitive → AttributeValue.String") {
        val t = java.time.Duration.ofMinutes(90)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.Duration(t))) == AttributeValue.String(t.toString)
        )
      },
      test("Period primitive → AttributeValue.String") {
        val t = java.time.Period.of(1, 2, 3)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.Period(t))) == AttributeValue.String(t.toString)
        )
      },
      test("LocalDate primitive → AttributeValue.String") {
        val t = java.time.LocalDate.of(2024, 1, 15)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.LocalDate(t))) == AttributeValue.String(t.toString)
        )
      },
      test("LocalTime primitive → AttributeValue.String") {
        val t = java.time.LocalTime.of(10, 30)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.LocalTime(t))) == AttributeValue.String(t.toString)
        )
      },
      test("LocalDateTime primitive → AttributeValue.String") {
        val t = java.time.LocalDateTime.of(2024, 1, 15, 10, 30)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.LocalDateTime(t))) == AttributeValue.String(
            t.toString
          )
        )
      },
      test("OffsetTime primitive → AttributeValue.String") {
        val t = java.time.OffsetTime.of(10, 30, 0, 0, java.time.ZoneOffset.UTC)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.OffsetTime(t))) == AttributeValue.String(t.toString)
        )
      },
      test("OffsetDateTime primitive → AttributeValue.String") {
        val t = java.time.OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, java.time.ZoneOffset.UTC)
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.OffsetDateTime(t))) == AttributeValue.String(
            t.toString
          )
        )
      },
      test("ZonedDateTime primitive → AttributeValue.String") {
        val t = java.time.ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, java.time.ZoneId.of("Europe/London"))
        assertTrue(
          dvCodec.encoder(DynamicValue.Primitive(PrimitiveValue.ZonedDateTime(t))) == AttributeValue.String(
            t.toString
          )
        )
      },
      test("Variant no-field case → Key-discriminator Map with empty inner") {
        assert(dvCodec.encoder(DynamicValue.Variant("Square", new DynamicValue.Record(Chunk.empty))))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("Square")),
              isSome(equalTo(AttributeValue.Map.empty))
            )
          )
        )
      },
      test("Variant record case → Key-discriminator Map with encoded fields") {
        val record = new DynamicValue.Record(Chunk("radius" -> DynamicValue.int(5)))
        assert(dvCodec.encoder(DynamicValue.Variant("Circle", record)))(
          isSubtype[AttributeValue.Map](
            hasField(
              "value",
              _.value.get(AttributeValue.String("Circle")),
              isSome(isSubtype[AttributeValue.Map](hasField("value", _.value, hasKey(AttributeValue.String("radius")))))
            )
          )
        )
      },
      test("Variant with a non-Record inner value encodes case name directly, without a Map wrapper") {
        assert(dvCodec.encoder(DynamicValue.Variant("X", DynamicValue.Primitive(PrimitiveValue.Int(5)))))(
          isSubtype[AttributeValue.String](hasField("value", _.value, equalTo("X")))
        )
      },
      test("DynamicValue.Map is not supported and throws") {
        val badDv = new DynamicValue.Map(Chunk.empty)
        assertTrue(scala.util.Try(dvCodec.encoder(badDv)).isFailure)
      }
    ),
    suite("decoder")(
      test("AttributeValue.String → DynamicValue.string") {
        assertTrue(dvCodec.decoder(AttributeValue.String("hi")) == Right(DynamicValue.string("hi")))
      },
      test("AttributeValue.Number that fits Int → DynamicValue.int") {
        assertTrue(dvCodec.decoder(AttributeValue.Number(BigDecimal(42))) == Right(DynamicValue.int(42)))
      },
      test("AttributeValue.Number that fits Long but not Int → DynamicValue.long") {
        val bigL = Long.MaxValue
        assertTrue(dvCodec.decoder(AttributeValue.Number(BigDecimal.valueOf(bigL))) == Right(DynamicValue.long(bigL)))
      },
      test("AttributeValue.Number with fractional value → DynamicValue.bigDecimal") {
        val bd = BigDecimal("1.5")
        assertTrue(dvCodec.decoder(AttributeValue.Number(bd)) == Right(DynamicValue.bigDecimal(bd)))
      },
      test("AttributeValue.Null → DynamicValue.Null") {
        assertTrue(dvCodec.decoder(AttributeValue.Null) == Right(DynamicValue.Null))
      },
      test("empty AttributeValue.Map → DynamicValue.unit") {
        assertTrue(dvCodec.decoder(AttributeValue.Map(Map.empty)) == Right(DynamicValue.unit))
      },
      test("AttributeValue.List → DynamicValue.Sequence") {
        val av = AttributeValue.List(List(AttributeValue.String("a")))
        assert(dvCodec.decoder(av))(
          isRight(isSubtype[DynamicValue.Sequence](hasField("elements", _.elements, hasSize(equalTo(1)))))
        )
      },
      test("AttributeValue.Map with entries → DynamicValue.Record") {
        val av = AttributeValue.Map(Map(AttributeValue.String("k") -> AttributeValue.String("v")))
        assert(dvCodec.decoder(av))(
          isRight(isSubtype[DynamicValue.Record](hasField("fields", _.fields, hasSize(equalTo(1)))))
        )
      },
      test("AttributeValue.Bool(true) → DynamicValue.boolean(true)") {
        assertTrue(dvCodec.decoder(AttributeValue.Bool(true)) == Right(DynamicValue.boolean(true)))
      },
      test("AttributeValue.Bool(false) → DynamicValue.boolean(false)") {
        assertTrue(dvCodec.decoder(AttributeValue.Bool(false)) == Right(DynamicValue.boolean(false)))
      },
      test("Boolean round-trip") {
        assertTrue(
          dvCodec.decoder(dvCodec.encoder(DynamicValue.boolean(true))) == Right(DynamicValue.boolean(true)) &&
            dvCodec.decoder(dvCodec.encoder(DynamicValue.boolean(false))) == Right(DynamicValue.boolean(false))
        )
      },
      test("unsupported AttributeValue type returns Left") {
        assertTrue(dvCodec.decoder(AttributeValue.BinarySet(Iterable.empty)).isLeft)
      },
      test("decode error in a nested List element propagates") {
        val av = AttributeValue.List(List(AttributeValue.String("ok"), AttributeValue.BinarySet(Iterable.empty)))
        assertTrue(dvCodec.decoder(av).isLeft)
      },
      test("decode error in a nested Map value propagates") {
        val av = AttributeValue.Map(
          Map(AttributeValue.String("k") -> AttributeValue.BinarySet(Iterable.empty))
        )
        assertTrue(dvCodec.decoder(av).isLeft)
      }
    ),
    // Null vs Unit conventions — see dynamicValueCodec scaladoc for the full rationale.
    suite("Null vs Unit conventions")(
      // DV.Null and DV.unit use distinct wire types and are unambiguous on decode.
      test("DynamicValue.Null encodes to AttributeValue.Null (not Map.empty)") {
        assertTrue(dvCodec.encoder(DynamicValue.Null) == AttributeValue.Null)
      },
      test("DynamicValue.unit encodes to empty AttributeValue.Map (not Null)") {
        assertTrue(dvCodec.encoder(DynamicValue.unit) == AttributeValue.Map.empty)
      },
      test("AttributeValue.Null decodes to DynamicValue.Null") {
        assertTrue(dvCodec.decoder(AttributeValue.Null) == Right(DynamicValue.Null))
      },
      test("empty AttributeValue.Map decodes to DynamicValue.unit") {
        assertTrue(dvCodec.decoder(AttributeValue.Map.empty) == Right(DynamicValue.unit))
      },
      test("DynamicValue.Null round-trips") {
        assertTrue(dvCodec.decoder(dvCodec.encoder(DynamicValue.Null)) == Right(DynamicValue.Null))
      },
      test("DynamicValue.unit round-trips") {
        assertTrue(dvCodec.decoder(dvCodec.encoder(DynamicValue.unit)) == Right(DynamicValue.unit))
      },
      // Correct pattern: use DynamicValue directly for a nullable field.
      // A DynamicValue field can be DV.Null — no Option wrapper needed or desired.
      test("DynamicValue.Null stored directly in a Record field round-trips") {
        case class Payload(id: String, value: DynamicValue)
        object Payload { implicit val schema: Schema[Payload] = Schema.derived }
        val codec   = codecFor[Payload]
        val payload = Payload("x", DynamicValue.Null)
        assertTrue(codec.decoder(codec.encoder(payload)) == Right(payload))
      },
      // Anti-pattern: Option[DynamicValue] loses Some(DV.Null) because the Option
      // codec encodes both None and Some(DV.Null) as AttributeValue.Null.
      // Use DynamicValue directly (above) to represent a nullable dynamic field.
      test("Option[DynamicValue] = Some(DynamicValue.Null) does not round-trip (known limitation)") {
        case class Wrapper(id: String, value: Option[DynamicValue])
        object Wrapper { implicit val schema: Schema[Wrapper] = Schema.derived }
        val codec   = codecFor[Wrapper]
        val wrapper = Wrapper("x", Some(DynamicValue.Null))
        // Some(DV.Null) encodes as AV.Null; the Option decoder claims AV.Null for None
        assertTrue(codec.decoder(codec.encoder(wrapper)) == Right(Wrapper("x", None)))
      }
    )
  )

  // ---- Json AST codec -----------------------------------------------------

  private val jc = DynamoDBCodecDeriver.jsonAVCodec

  private val nestedJson: Json =
    Json.Object(
      Chunk(
        "name"    -> Json.String("alice"),
        "score"   -> Json.Number(BigDecimal(99)),
        "active"  -> Json.Boolean(true),
        "nothing" -> Json.Null,
        "tags"    -> Json.Array(Chunk(Json.String("a"), Json.String("b"))),
        "meta"    -> Json.Object(Chunk("k" -> Json.String("v")))
      )
    )

  private val jsonAVSuite = suite("jsonAVCodec")(
    suite("encoder")(
      test("Null → AttributeValue.Null") {
        assertTrue(jc.encoder(Json.Null) == AttributeValue.Null)
      },
      test("Boolean true → AttributeValue.Bool(true)") {
        assertTrue(jc.encoder(Json.Boolean(true)) == AttributeValue.Bool(true))
      },
      test("Boolean false → AttributeValue.Bool(false)") {
        assertTrue(jc.encoder(Json.Boolean(false)) == AttributeValue.Bool(false))
      },
      test("String → AttributeValue.String") {
        assertTrue(jc.encoder(Json.String("hello")) == AttributeValue.String("hello"))
      },
      test("Number (integer) → AttributeValue.Number") {
        assertTrue(jc.encoder(Json.Number(BigDecimal(42))) == AttributeValue.Number(BigDecimal(42)))
      },
      test("Number (fractional) → AttributeValue.Number") {
        assertTrue(jc.encoder(Json.Number(BigDecimal("3.14"))) == AttributeValue.Number(BigDecimal("3.14")))
      },
      test("empty Array → empty AttributeValue.List") {
        assertTrue(jc.encoder(Json.Array(Chunk.empty)) == AttributeValue.List(Iterable.empty))
      },
      test("Array of primitives → AttributeValue.List") {
        val av = jc.encoder(Json.Array(Chunk(Json.String("x"), Json.Boolean(true), Json.Null)))
        assert(av)(
          isSubtype[AttributeValue.List](
            hasField[AttributeValue.List, List[AttributeValue]](
              "value",
              _.value.toList,
              equalTo(List(AttributeValue.String("x"), AttributeValue.Bool(true), AttributeValue.Null))
            )
          )
        )
      },
      test("empty Object → empty AttributeValue.Map") {
        assert(jc.encoder(Json.Object(Chunk.empty)))(
          isSubtype[AttributeValue.Map](hasField("value", _.value.isEmpty, isTrue))
        )
      },
      test("Object with fields → AttributeValue.Map") {
        assert(jc.encoder(Json.Object(Chunk("a" -> Json.Number(BigDecimal(1)), "b" -> Json.String("two")))))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("a")),
              isSome(equalTo(AttributeValue.Number(BigDecimal(1))))
            ) &&
              hasField[AttributeValue.Map, Option[AttributeValue]](
                "value",
                _.value.get(AttributeValue.String("b")),
                isSome(equalTo(AttributeValue.String("two")))
              )
          )
        )
      },
      test("nested Object encodes recursively") {
        assert(jc.encoder(nestedJson))(
          isSubtype[AttributeValue.Map](
            hasField[AttributeValue.Map, Option[AttributeValue]](
              "value",
              _.value.get(AttributeValue.String("name")),
              isSome(equalTo(AttributeValue.String("alice")))
            ) &&
              hasField[AttributeValue.Map, Option[AttributeValue]](
                "value",
                _.value.get(AttributeValue.String("active")),
                isSome(equalTo(AttributeValue.Bool(true)))
              ) &&
              hasField[AttributeValue.Map, Option[AttributeValue]](
                "value",
                _.value.get(AttributeValue.String("nothing")),
                isSome(equalTo(AttributeValue.Null))
              )
          )
        )
      }
    ),
    suite("decoder")(
      test("AttributeValue.Null → Json.Null") {
        assertTrue(jc.decoder(AttributeValue.Null) == Right(Json.Null))
      },
      test("AttributeValue.Bool(true) → Json.Boolean(true)") {
        assertTrue(jc.decoder(AttributeValue.Bool(true)) == Right(Json.Boolean(true)))
      },
      test("AttributeValue.Bool(false) → Json.Boolean(false)") {
        assertTrue(jc.decoder(AttributeValue.Bool(false)) == Right(Json.Boolean(false)))
      },
      test("AttributeValue.String → Json.String") {
        assertTrue(jc.decoder(AttributeValue.String("hello")) == Right(Json.String("hello")))
      },
      test("AttributeValue.Number (integer) → Json.Number") {
        assertTrue(jc.decoder(AttributeValue.Number(BigDecimal(42))) == Right(Json.Number(BigDecimal(42))))
      },
      test("AttributeValue.Number (fractional) → Json.Number") {
        val bd = BigDecimal("3.14")
        assertTrue(jc.decoder(AttributeValue.Number(bd)) == Right(Json.Number(bd)))
      },
      test("empty AttributeValue.List → empty Json.Array") {
        assertTrue(jc.decoder(AttributeValue.List(Iterable.empty)) == Right(Json.Array(Chunk.empty)))
      },
      test("AttributeValue.List of primitives → Json.Array") {
        val av = AttributeValue.List(List(AttributeValue.String("x"), AttributeValue.Bool(true)))
        assert(jc.decoder(av))(
          isRight(
            isSubtype[Json.Array](
              hasField[Json.Array, Chunk[Json]]("value", _.value, equalTo(Chunk(Json.String("x"), Json.Boolean(true))))
            )
          )
        )
      },
      test("empty AttributeValue.Map → empty Json.Object") {
        assert(jc.decoder(AttributeValue.Map(Map.empty)))(
          isRight(isSubtype[Json.Object](hasField("value", _.value.isEmpty, isTrue)))
        )
      },
      test("AttributeValue.Map with entries → Json.Object") {
        val av = AttributeValue.Map(
          Map(
            AttributeValue.String("x") -> AttributeValue.Number(BigDecimal(1))
          )
        )
        assert(jc.decoder(av))(
          isRight(
            isSubtype[Json.Object](
              hasField[Json.Object, List[(String, Json)]](
                "value",
                _.value.toList,
                equalTo(List("x" -> Json.Number(BigDecimal(1))))
              )
            )
          )
        )
      },
      test("unsupported AttributeValue type returns Left") {
        assertTrue(jc.decoder(AttributeValue.BinarySet(Iterable.empty)).isLeft)
      },
      test("decode error in nested List propagates") {
        val av = AttributeValue.List(List(AttributeValue.String("ok"), AttributeValue.BinarySet(Iterable.empty)))
        assertTrue(jc.decoder(av).isLeft)
      },
      test("decode error in nested Map propagates") {
        val av = AttributeValue.Map(
          Map(
            AttributeValue.String("bad") -> AttributeValue.BinarySet(Iterable.empty)
          )
        )
        assertTrue(jc.decoder(av).isLeft)
      }
    ),
    suite("round-trips")(
      test("Null round-trips") {
        assertTrue(jc.decoder(jc.encoder(Json.Null)) == Right(Json.Null))
      },
      test("Boolean round-trips") {
        assertTrue(
          jc.decoder(jc.encoder(Json.Boolean(true))) == Right(Json.Boolean(true)) &&
            jc.decoder(jc.encoder(Json.Boolean(false))) == Right(Json.Boolean(false))
        )
      },
      test("String round-trips") {
        assertTrue(jc.decoder(jc.encoder(Json.String("hello"))) == Right(Json.String("hello")))
      },
      test("Number round-trips") {
        val bd = BigDecimal("1234567890.0987654321")
        assertTrue(jc.decoder(jc.encoder(Json.Number(bd))) == Right(Json.Number(bd)))
      },
      test("nested structure round-trips") {
        assertTrue(jc.decoder(jc.encoder(nestedJson)) == Right(nestedJson))
      }
    ),
    suite("derived schema with Json field")(
      test("case class with Json field derives and round-trips") {
        case class Doc(id: String, body: Json)
        object Doc { implicit val schema: Schema[Doc] = Schema.derived }
        val codec = codecFor[Doc]
        val doc   = Doc("d1", nestedJson)
        val av    = codec.encoder(doc)
        assertTrue(codec.decoder(av) == Right(doc))
      }
    )
  )

  // ---- schema1 byte-sequence compatibility --------------------------------

  private val readBothWriteOldDeriver =
    DynamoDBCodecDeriver.withSchema1ByteSequenceCompatibility(Schema1Compat.ReadBothWriteOld)
  private val readBothWriteNewDeriver =
    DynamoDBCodecDeriver.withSchema1ByteSequenceCompatibility(Schema1Compat.ReadBothWriteNew)

  private def compatCodecForOld[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(readBothWriteOldDeriver).derive
  private def compatCodecForNew[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(readBothWriteNewDeriver).derive

  private val maybeSuite = suite("maybe codec")(
    test("present string encodes as the inner value") {
      val codec = codecFor[Maybe[String]]
      assertTrue(codec.encoder(Maybe.present("hello")) == AttributeValue.String("hello"))
    },
    test("absent encodes as Null") {
      val codec = codecFor[Maybe[String]]
      assertTrue(codec.encoder(Maybe.absent[String]) == AttributeValue.Null)
    },
    test("Null decodes as absent") {
      val codec = codecFor[Maybe[String]]
      assertTrue(codec.decoder(AttributeValue.Null) == Right(Maybe.absent[String]))
    },
    test("inner value decodes as present") {
      val codec = codecFor[Maybe[String]]
      assertTrue(codec.decoder(AttributeValue.String("world")) == Right(Maybe.present("world")))
    },
    test("present int encodes as Number") {
      val codec = codecFor[Maybe[Int]]
      assertTrue(codec.encoder(Maybe.present(42)) == AttributeValue.Number(BigDecimal(42)))
    },
    test("absent int encodes as Null") {
      val codec = codecFor[Maybe[Int]]
      assertTrue(codec.encoder(Maybe.absent[Int]) == AttributeValue.Null)
    },
    test("record with present Maybe field round-trips") {
      val codec = codecFor[WithMaybe]
      val value = WithMaybe("id1", Maybe.present("hello"))
      val av    = codec.encoder(value)
      assertTrue(codec.decoder(av) == Right(value))
    },
    test("record with absent Maybe field omits key (transientNone=true default)") {
      val codec = codecFor[WithMaybe]
      val av    = codec.encoder(WithMaybe("id1", Maybe.absent[String]))
      assert(av)(isSubtype[AttributeValue.Map](hasField("value", _.value, !hasKey(AttributeValue.String("value")))))
    },
    test("record with absent Maybe field round-trips (absent → absent)") {
      val codec = codecFor[WithMaybe]
      val value = WithMaybe("id1", Maybe.absent[String])
      val av    = codec.encoder(value)
      assertTrue(codec.decoder(av) == Right(value))
    },
    test("record with absent Maybe field decodes when field is Null") {
      val codec = codecFor[WithMaybe]
      val av    = AttributeValue.Map(
        Map(
          AttributeValue.String("id")    -> AttributeValue.String("id1"),
          AttributeValue.String("value") -> AttributeValue.Null
        )
      )
      assertTrue(codec.decoder(av) == Right(WithMaybe("id1", Maybe.absent[String])))
    },
    test("record missing Maybe field decodes as absent") {
      val codec = codecFor[WithMaybe]
      val av    = AttributeValue.Map(
        Map(
          AttributeValue.String("id") -> AttributeValue.String("id1")
        )
      )
      assertTrue(codec.decoder(av) == Right(WithMaybe("id1", Maybe.absent[String])))
    },
    test("Maybe[Int] present round-trips") {
      val codec = codecFor[WithMaybeInt]
      val value = WithMaybeInt("x", Maybe.present(99))
      val av    = codec.encoder(value)
      assertTrue(codec.decoder(av) == Right(value))
    },
    test("Maybe[Int] absent round-trips") {
      val codec = codecFor[WithMaybeInt]
      val value = WithMaybeInt("x", Maybe.absent[Int])
      val av    = codec.encoder(value)
      assertTrue(codec.decoder(av) == Right(value))
    }
  )

  private val byteSequenceCompatSuite = suite("Schema1Compat byte-sequence")(
    suite("ReadBothWriteOld — encodes List[Number], decodes both")(
      test("Chunk[Byte] encodes as AttributeValue.List of Number") {
        val codec = compatCodecForOld[Chunk[Byte]]
        val bytes = Chunk[Byte](1, 2, 3)
        assert(codec.encoder(bytes))(
          isSubtype[AttributeValue.List](
            hasField[AttributeValue.List, List[AttributeValue]](
              "value",
              _.value.toList,
              equalTo(
                List(
                  AttributeValue.Number(BigDecimal(1)),
                  AttributeValue.Number(BigDecimal(2)),
                  AttributeValue.Number(BigDecimal(3))
                )
              )
            )
          )
        )
      },
      test("Chunk[Byte] decodes from AttributeValue.List of Number") {
        val codec = compatCodecForOld[Chunk[Byte]]
        val av    = AttributeValue.List(
          List(
            AttributeValue.Number(BigDecimal(10)),
            AttributeValue.Number(BigDecimal(20))
          )
        )
        assertTrue(codec.decoder(av) == Right(Chunk[Byte](10, 20)))
      },
      test("Chunk[Byte] also decodes from AttributeValue.Binary (fallback)") {
        val codec = compatCodecForOld[Chunk[Byte]]
        assertTrue(codec.decoder(AttributeValue.Binary(Array[Byte](1, 2, 3))) == Right(Chunk[Byte](1, 2, 3)))
      },
      test("Array[Byte] encodes as AttributeValue.List of Number") {
        val codec = Schema.derived[Array[Byte]].deriving(readBothWriteOldDeriver).derive
        assert(codec.encoder(Array[Byte](4, 5)))(
          isSubtype[AttributeValue.List](hasField("value", _.value, hasSize(equalTo(2))))
        )
      }
    ),
    suite("ReadBothWriteNew — encodes Binary, decodes both")(
      test("Chunk[Byte] encodes as AttributeValue.Binary") {
        val codec = compatCodecForNew[Chunk[Byte]]
        assert(codec.encoder(Chunk[Byte](1, 2, 3)))(isSubtype[AttributeValue.Binary](anything))
      },
      test("Chunk[Byte] decodes from AttributeValue.Binary") {
        val codec = compatCodecForNew[Chunk[Byte]]
        assertTrue(codec.decoder(AttributeValue.Binary(Array[Byte](1, 2, 3))) == Right(Chunk[Byte](1, 2, 3)))
      },
      test("Chunk[Byte] also decodes from legacy AttributeValue.List of Number") {
        val codec = compatCodecForNew[Chunk[Byte]]
        val av    = AttributeValue.List(
          List(
            AttributeValue.Number(BigDecimal(10)),
            AttributeValue.Number(BigDecimal(20))
          )
        )
        assertTrue(codec.decoder(av) == Right(Chunk[Byte](10, 20)))
      },
      test("Chunk[Byte] round-trips through Binary") {
        val codec = compatCodecForNew[Chunk[Byte]]
        val bytes = Chunk[Byte](5, 6, 7)
        assertTrue(codec.decoder(codec.encoder(bytes)) == Right(bytes))
      }
    ),
    suite("ReadNewWriteNew — encodes Binary, decodes Binary only (default)")(
      test("Chunk[Byte] encodes as AttributeValue.Binary") {
        val codec = codecFor[Chunk[Byte]]
        assert(codec.encoder(Chunk[Byte](1, 2)))(isSubtype[AttributeValue.Binary](anything))
      },
      test("Chunk[Byte] rejects legacy AttributeValue.List") {
        val codec = codecFor[Chunk[Byte]]
        val av    = AttributeValue.List(List(AttributeValue.Number(BigDecimal(1))))
        assertTrue(codec.decoder(av).isLeft)
      }
    )
  )

  // ---- schema1 tuple compatibility -----------------------------------------
  //
  // typeId.isTuple derives a dedicated positional codec (List(a, b, c, ...))
  // instead of the vanilla record path. schema1TupleCompat controls whether
  // encoding uses that flat list (new) or the legacy schema1 nested
  // right-folded pair format List(List(a, b), c) (old), and whether decoding
  // falls back to the legacy shape when flat-list decoding fails.

  // Scala tuples' companion objects live in the standard library, so we can't
  // put a project-local given/implicit Schema there; unlike case classes in
  // this suite, we define explicit Schema.derived instances for each tuple
  // arity used below.
  private implicit val tuple2Schema: Schema[(String, Int)]                                                   = Schema.derived
  private implicit val tuple3Schema: Schema[(String, Int, Boolean)]                                          = Schema.derived
  private implicit val tuple4Schema: Schema[(String, Int, Boolean, String)]                                  = Schema.derived
  private implicit val tuplePrimitivesSchema: Schema[(Int, Long, Boolean, Byte, Char, Short, Float, Double)] =
    Schema.derived

  private val tupleCompatOldDeriver    =
    DynamoDBCodecDeriver.withSchema1TupleCompatibility(Schema1Compat.ReadBothWriteOld)
  private val tupleCompatNewDeriver    =
    DynamoDBCodecDeriver.withSchema1TupleCompatibility(Schema1Compat.ReadBothWriteNew)
  private val tupleCompatStrictDeriver =
    DynamoDBCodecDeriver.withSchema1TupleCompatibility(Schema1Compat.ReadNewWriteNew)

  private def tupleCodecForOld[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(tupleCompatOldDeriver).derive
  private def tupleCodecForNew[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(tupleCompatNewDeriver).derive
  private def tupleCodecStrict[A](implicit s: Schema[A]): DynamoDBCodec[A] =
    s.deriving(tupleCompatStrictDeriver).derive

  private val tupleCompatSuite = suite("Schema1Compat tuple")(
    test("default (ReadBothWriteNew) encodes a Tuple2 as a flat positional list") {
      val codec = codecFor[(String, Int)]
      assertTrue(
        codec.encoder(("a", 1)) == AttributeValue.List(
          List(AttributeValue.String("a"), AttributeValue.Number(BigDecimal(1)))
        )
      )
    },
    test("default (ReadBothWriteNew) round-trips a Tuple3 through the flat list") {
      val codec = codecFor[(String, Int, Boolean)]
      val value = ("x", 42, true)
      assertTrue(codec.decoder(codec.encoder(value)) == Right(value))
    },
    test("default (ReadBothWriteNew) also decodes the legacy nested-pair format") {
      val codec  = codecFor[(String, Int, Boolean)]
      val legacy = AttributeValue.List(
        List(
          AttributeValue.List(List(AttributeValue.String("x"), AttributeValue.Number(BigDecimal(42)))),
          AttributeValue.Bool(true)
        )
      )
      assertTrue(codec.decoder(legacy) == Right(("x", 42, true)))
    },
    test("ReadNewWriteNew (strict) rejects the legacy nested-pair format") {
      val codec  = tupleCodecStrict[(String, Int, Boolean)]
      val legacy = AttributeValue.List(
        List(
          AttributeValue.List(List(AttributeValue.String("x"), AttributeValue.Number(BigDecimal(42)))),
          AttributeValue.Bool(true)
        )
      )
      assertTrue(codec.decoder(legacy).isLeft)
    },
    test("ReadBothWriteOld encodes a Tuple3 as nested right-folded pairs") {
      val codec = tupleCodecForOld[(String, Int, Boolean)]
      assertTrue(
        codec.encoder(("x", 42, true)) == AttributeValue.List(
          List(
            AttributeValue.List(List(AttributeValue.String("x"), AttributeValue.Number(BigDecimal(42)))),
            AttributeValue.Bool(true)
          )
        )
      )
    },
    test("ReadBothWriteOld round-trips a Tuple4 through its own legacy encoding") {
      val codec = tupleCodecForOld[(String, Int, Boolean, String)]
      val value = ("a", 1, true, "d")
      assertTrue(codec.decoder(codec.encoder(value)) == Right(value))
    },
    test("ReadBothWriteNew encodes a Tuple3 as a flat positional list") {
      val codec = tupleCodecForNew[(String, Int, Boolean)]
      assertTrue(
        codec.encoder(("x", 42, true)) == AttributeValue.List(
          List(AttributeValue.String("x"), AttributeValue.Number(BigDecimal(42)), AttributeValue.Bool(true))
        )
      )
    },
    test("ReadBothWriteNew still decodes legacy nested-pair data written by an old encoder") {
      val codec  = tupleCodecForNew[(String, Int, Boolean)]
      val legacy = tupleCodecForOld[(String, Int, Boolean)].encoder(("x", 42, true))
      assertTrue(codec.decoder(legacy) == Right(("x", 42, true)))
    },
    test("malformed legacy nested pair (wrong inner list size) yields a decode error") {
      val codec     = tupleCodecForOld[(String, Int, Boolean)]
      val malformed = AttributeValue.List(
        List(
          AttributeValue.List(
            List(AttributeValue.String("x"), AttributeValue.Number(BigDecimal(1)), AttributeValue.Number(BigDecimal(2)))
          ),
          AttributeValue.Bool(true)
        )
      )
      assertTrue(codec.decoder(malformed).isLeft)
    },
    test("non-List attribute value yields a decode error") {
      val codec = codecFor[(String, Int)]
      assertTrue(codec.decoder(AttributeValue.String("not-a-list")).isLeft)
    },
    test("round-trips a tuple covering every primitive register type (Int/Long/Boolean/Byte/Char/Short/Float/Double)") {
      val codec = codecFor[(Int, Long, Boolean, Byte, Char, Short, Float, Double)]
      val value = (1, 2L, true, 3.toByte, 'c', 4.toShort, 5.5f, 6.6)
      assertTrue(codec.decoder(codec.encoder(value)) == Right(value))
    }
  )

  // ---- @Modifier.fieldNaming -----------------------------------------------

  private val fieldNamingSuite = suite("@Modifier.fieldNaming")(
    test("encodes fields as snake_case keys") {
      val av = codecFor[SnakeRecord].encoder(SnakeRecord("Alice", "Smith", "London"))
      assert(av)(
        isSubtype[AttributeValue.Map](
          hasField(
            "value",
            _.value,
            hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("first_name")) &&
              hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("last_name")) &&
              hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("home_city"))
          )
        )
      )
    },
    test("camelCase keys are absent when snake_case annotation is present") {
      val av = codecFor[SnakeRecord].encoder(SnakeRecord("Alice", "Smith", "London"))
      assert(av)(
        isSubtype[AttributeValue.Map](
          hasField(
            "value",
            _.value,
            !hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("firstName")) &&
              !hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("lastName"))
          )
        )
      )
    },
    test("decodes from snake_case keys") {
      val av    = avMap(
        "first_name" -> AttributeValue.String("Alice"),
        "last_name"  -> AttributeValue.String("Smith"),
        "home_city"  -> AttributeValue.String("London")
      )
      val codec = codecFor[SnakeRecord]
      assertTrue(codec.decoder(av) == Right(SnakeRecord("Alice", "Smith", "London")))
    },
    test("round-trips through snake_case") {
      val value = SnakeRecord("Alice", "Smith", "London")
      assertTrue(roundTrip(value) == Right(value))
    },
    test("unannotated record is unaffected — keeps camelCase keys") {
      val av = codecFor[CamelRecord].encoder(CamelRecord("Alice", "Smith"))
      assert(av)(
        isSubtype[AttributeValue.Map](
          hasField(
            "value",
            _.value,
            hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("firstName")) &&
              hasKey[AttributeValue.String, AttributeValue](AttributeValue.String("lastName"))
          )
        )
      )
    },
    test("per-type annotation overrides deriver-level mapper") {
      val deriver = DynamoDBCodecDeriver.withFieldNameMapper(NameMapper.fromString("PascalCase"))
      val av      = implicitly[Schema[SnakeRecord]]
        .deriving(deriver)
        .derive
        .encoder(SnakeRecord("Alice", "Smith", "London"))
      // @fieldNaming("snake_case") wins over deriver-level PascalCase
      assert(av)(
        isSubtype[AttributeValue.Map](hasField("value", _.value, hasKey(AttributeValue.String("first_name"))))
      )
    }
  )

  // ---- rejectExtraFields / @Modifier.noExtraFields --------------------------

  private def avMap(pairs: (String, AttributeValue)*): AttributeValue.Map =
    AttributeValue.Map(Map(pairs.map { case (k, v) => AttributeValue.String(k) -> v }: _*))

  private val rejectExtraFieldsSuite = suite("rejectExtraFields")(
    suite("@Modifier.noExtraFields on type")(
      test("known fields decode successfully") {
        val av    = avMap("id" -> AttributeValue.String("x"), "value" -> AttributeValue.Number(1))
        val codec = codecFor[Strict]
        assertTrue(codec.decoder(av) == Right(Strict("x", 1)))
      },
      test("unknown field yields Left") {
        val av    = avMap(
          "id"      -> AttributeValue.String("x"),
          "value"   -> AttributeValue.Number(1),
          "unknown" -> AttributeValue.String("surprise")
        )
        val codec = codecFor[Strict]
        assertTrue(codec.decoder(av).isLeft)
      },
      test("error message names the unknown field") {
        val av    = avMap(
          "id"    -> AttributeValue.String("x"),
          "value" -> AttributeValue.Number(1),
          "ghost" -> AttributeValue.String("boo")
        )
        val codec = codecFor[Strict]
        assertTrue(codec.decoder(av).left.exists(_.getMessage.contains("ghost")))
      },
      test("type without annotation accepts extra fields") {
        val av    = avMap(
          "id"      -> AttributeValue.String("x"),
          "value"   -> AttributeValue.Number(1),
          "unknown" -> AttributeValue.String("ignored")
        )
        val codec = codecFor[Lenient]
        assertTrue(codec.decoder(av) == Right(Lenient("x", 1)))
      }
    ),
    suite("withRejectExtraFields(true) on deriver")(
      test("known fields decode successfully") {
        val av    = avMap("id" -> AttributeValue.String("x"), "value" -> AttributeValue.Number(1))
        val codec = implicitly[Schema[Lenient]].deriving(DynamoDBCodecDeriver.withRejectExtraFields(true)).derive
        assertTrue(codec.decoder(av) == Right(Lenient("x", 1)))
      },
      test("unknown field yields Left") {
        val av    = avMap(
          "id"      -> AttributeValue.String("x"),
          "value"   -> AttributeValue.Number(1),
          "unknown" -> AttributeValue.String("surprise")
        )
        val codec = implicitly[Schema[Lenient]].deriving(DynamoDBCodecDeriver.withRejectExtraFields(true)).derive
        assertTrue(codec.decoder(av).isLeft)
      }
    ),
    suite("Field-discriminator variant with @noExtraFields case")(
      test("discriminator key is not rejected as extra") {
        val av    = avMap("_type" -> AttributeValue.String("Node"), "x" -> AttributeValue.Number(42))
        val codec = codecFor[StrictTagged]
        assertTrue(codec.decoder(av) == Right(StrictTagged.Node(42)))
      },
      test("genuine extra field in case record is still rejected") {
        val av    = avMap(
          "_type" -> AttributeValue.String("Node"),
          "x"     -> AttributeValue.Number(42),
          "extra" -> AttributeValue.String("bad")
        )
        val codec = codecFor[StrictTagged]
        assertTrue(codec.decoder(av).isLeft)
      }
    )
  )

  // ---- Schema1Compat Year compatibility ------------------------------------

  private def yearCodecFor(mode: Schema1Compat): DynamoDBCodec[Year] =
    implicitly[Schema[Year]]
      .deriving(DynamoDBCodecDeriver.withSchema1YearCompatibility(mode))
      .derive

  private val yearCompatSuite = suite("Schema1Compat Year")(
    suite("ReadNewWriteNew — encodes Number, decodes Number only (default)")(
      test("Year encodes as Number") {
        assertTrue(codecFor[Year].encoder(Year.of(2023)) == AttributeValue.Number(BigDecimal(2023)))
      },
      test("Year decodes from Number") {
        assertTrue(codecFor[Year].decoder(AttributeValue.Number(BigDecimal(2023))) == Right(Year.of(2023)))
      },
      test("Year rejects legacy String") {
        assertTrue(codecFor[Year].decoder(AttributeValue.String("2023")).isLeft)
      }
    ),
    suite("ReadBothWriteOld — encodes legacy String, decodes both")(
      test("Year encodes as zero-padded 4-digit String") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        assertTrue(codec.encoder(Year.of(2023)) == AttributeValue.String("2023"))
      },
      test("Year below 1000 is zero-padded") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        assertTrue(codec.encoder(Year.of(100)) == AttributeValue.String("0100"))
      },
      test("Year above 9999 gets + prefix") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        assertTrue(codec.encoder(Year.of(10000)) == AttributeValue.String("+10000"))
      },
      test("negative Year gets - prefix with zero-padding") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        assertTrue(codec.encoder(Year.of(-1)) == AttributeValue.String("-0001"))
      },
      test("Year decodes from legacy String") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        assertTrue(codec.decoder(AttributeValue.String("2023")) == Right(Year.of(2023)))
      },
      test("zero-padded String decodes correctly") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        assertTrue(codec.decoder(AttributeValue.String("0100")) == Right(Year.of(100)))
      },
      test("Year decodes from Number (new-format fallback)") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(2023))) == Right(Year.of(2023)))
      },
      test("Year round-trips through legacy String") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteOld)
        val year  = Year.of(2023)
        assertTrue(codec.decoder(codec.encoder(year)) == Right(year))
      }
    ),
    suite("ReadBothWriteNew — encodes Number, decodes both")(
      test("Year encodes as Number") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteNew)
        assertTrue(codec.encoder(Year.of(2023)) == AttributeValue.Number(BigDecimal(2023)))
      },
      test("Year decodes from Number") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteNew)
        assertTrue(codec.decoder(AttributeValue.Number(BigDecimal(2023))) == Right(Year.of(2023)))
      },
      test("Year decodes from legacy String") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteNew)
        assertTrue(codec.decoder(AttributeValue.String("2023")) == Right(Year.of(2023)))
      },
      test("zero-padded legacy String decodes correctly") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteNew)
        assertTrue(codec.decoder(AttributeValue.String("0100")) == Right(Year.of(100)))
      },
      test("Year round-trips through Number") {
        val codec = yearCodecFor(Schema1Compat.ReadBothWriteNew)
        val year  = Year.of(2023)
        assertTrue(codec.decoder(codec.encoder(year)) == Right(year))
      }
    )
  )
}
